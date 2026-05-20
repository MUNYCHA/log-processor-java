package org.munycha.logprocessor.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.munycha.logprocessor.alert.AlertPatternStore;
import org.munycha.logprocessor.config.TopicType;
import org.munycha.logprocessor.log.LogEvent;
import org.munycha.logprocessor.metric.ServerStorageSnapshot;
import org.munycha.logprocessor.normalizer.LogMessageNormalizer;
import org.munycha.logprocessor.notification.TelegramNotificationService;
import org.munycha.logprocessor.repository.AlertRepository;
import org.munycha.logprocessor.repository.ServerStorageSnapshotRepository;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class KafkaTopicConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicConsumer.class);

    private static final int MAX_TELEGRAM_ALERTS_PER_BATCH = 5;

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final String topic;
    private final TopicType type;
    private final Path outputFile;
    private final Set<String> alertKeywords;
    private final AlertRepository alertRepository;
    private final ServerStorageSnapshotRepository storageSnapshotRepository;
    private final KafkaConsumer<String, String> consumer;
    private final TelegramNotificationService notifier;
    private final ExecutorService telegramAlertExecutor;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ObjectWriter prettyWriter = mapper.writerWithDefaultPrettyPrinter();

    // null when patternStoreFile is not configured — dedup disabled for this topic
    private final AlertPatternStore patternStore;

    // Per-topic normalizer instance — carries this topic's custom rules
    private final LogMessageNormalizer normalizer;

    private volatile boolean running = true;

    public KafkaTopicConsumer(KafkaConsumerFactory consumerFactory,
                              String topic,
                              TopicType type,
                              Path outputFile,
                              TelegramNotificationService notifier,
                              List<String> alertKeywords,
                              AlertRepository alertRepository,
                              ServerStorageSnapshotRepository storageSnapshotRepository,
                              ExecutorService telegramAlertExecutor,
                              Path patternStoreFile,
                              LogMessageNormalizer normalizer) throws IOException {

        this.topic = topic;
        this.type = type;
        this.outputFile = outputFile;
        this.alertKeywords =
                alertKeywords == null
                        ? Collections.emptySet()
                        : alertKeywords.stream()
                        .filter(Objects::nonNull)
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .map(String::toLowerCase)
                        .collect(Collectors.toSet());
        this.alertRepository = alertRepository;
        this.storageSnapshotRepository = storageSnapshotRepository;
        this.telegramAlertExecutor = telegramAlertExecutor;
        this.patternStore = patternStoreFile != null ? new AlertPatternStore(patternStoreFile) : null;
        this.normalizer = normalizer != null ? normalizer : new LogMessageNormalizer();

        this.consumer = consumerFactory.createConsumer();
        this.consumer.subscribe(Collections.singletonList(this.topic));
        this.notifier = notifier;
    }

    @Override
    public void run() {

        try {

            while (running) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                boolean success = true;
                List<LogEvent> telegramQueue = new ArrayList<>();
                StringWriter batchBuffer = new StringWriter();

                for (ConsumerRecord<String, String> record : records) {

                    try {

                        switch (type) {
                            case LOG:
                                LogEvent alertEvent = handleLogRecord(record, batchBuffer);
                                if (alertEvent != null && telegramQueue.size() < MAX_TELEGRAM_ALERTS_PER_BATCH) {
                                    telegramQueue.add(alertEvent);
                                }
                                break;
                            case METRIC:
                                handleMetricRecord(record, batchBuffer);
                                break;
                        }

                    } catch (Exception e) {
                        success = false;
                        Throwable cause = e.getCause() != null ? e.getCause() : e;
                        log.warn("Retry triggered: topic={} partition={} offset={} reason={} cause={}",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                e.getMessage(),
                                cause.getMessage(),
                                cause);
                        break;
                    }
                }

                if (success) {
                    String batchContent = batchBuffer.toString();
                    if (!batchContent.isEmpty()) {
                        // Open, write entire batch, close — file is never held open between polls.
                        // Rotation scripts can safely truncate or replace the file at any time.
                        try (BufferedWriter fileWriter = Files.newBufferedWriter(
                                outputFile,
                                StandardCharsets.UTF_8,
                                StandardOpenOption.WRITE,
                                StandardOpenOption.APPEND
                        )) {
                            fileWriter.write(batchContent);
                        }
                    }
                    consumer.commitSync();

                    for (LogEvent ev : telegramQueue) {
                        sendTelegramAsync(ev);
                    }
                }
            }

        } catch (WakeupException ignored) {
        } catch (IOException e) {
            log.error("Writer failure: {}", e.getMessage(), e);
        } finally {
            consumer.close();
        }
    }

    // ===================== METRIC =====================

    private void handleMetricRecord(ConsumerRecord<String, String> record, Writer batchBuffer) {

        try {

            ServerStorageSnapshot snapshot =
                    mapper.readValue(record.value(), ServerStorageSnapshot.class);

            batchBuffer.write(prettyWriter.writeValueAsString(snapshot));
            batchBuffer.write(System.lineSeparator());

            storageSnapshotRepository.saveSnapshotWithDiskUsages(snapshot);

        } catch (Exception e) {
            throw new RuntimeException("Metric DB save failed", e);
        }
    }

    // ===================== LOG =====================

    private boolean isAlert(String lowerMsg) {
        return type == TopicType.LOG &&
                !alertKeywords.isEmpty() &&
                alertKeywords.stream().anyMatch(lowerMsg::contains);
    }

    private LogEvent handleLogRecord(ConsumerRecord<String, String> record, Writer batchBuffer) {

        try {

            LogEvent event = mapper.readValue(record.value(), LogEvent.class);

            String msg = event.getMessage();
            String lower = msg.toLowerCase();

            batchBuffer.write(msg);
            batchBuffer.write(System.lineSeparator());

            if (!isAlert(lower)) {
                return null;
            }

            if (patternStore != null) {
                String pattern = normalizer.normalizeMessage(msg);

                if (patternStore.isKnown(pattern)) {
                    log.debug("Suppressed: pattern already known: {}", pattern);
                    return null;
                }

                saveAlert(event);
                patternStore.add(pattern);
                return event;
            }

            saveAlert(event);
            return event;

        } catch (Exception e) {
            throw new RuntimeException("Log processing failed", e);
        }
    }

    // ===================== ALERT =====================

    private void saveAlert(LogEvent event) {

        alertRepository.saveAlert(
                event.getTopic(),
                event.getTimestamp(),
                event.getServerName(),
                event.getPath(),
                event.getMessage()
        );
    }

    private void sendTelegramAsync(LogEvent event) {

        telegramAlertExecutor.submit(() -> {

            String formatted =
                    Instant.parse(event.getTimestamp())
                            .atZone(ZoneId.systemDefault())
                            .format(TIMESTAMP_FORMATTER);

            String message =
                    "ALERT\nTime: " + formatted +
                            "\nHost: " + event.getServerName() +
                            "\nFile: " + event.getPath() +
                            "\nTopic: " + event.getTopic() +
                            "\nMessage: " + event.getMessage();

            notifier.sendMessage(message);
        });
    }

    // ===================== SHUTDOWN =====================

    public void shutdown() {
        running = false;
        consumer.wakeup();
    }
}
