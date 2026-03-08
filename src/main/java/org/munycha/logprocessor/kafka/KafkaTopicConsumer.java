package org.munycha.logprocessor.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.munycha.logprocessor.config.TopicType;
import org.munycha.logprocessor.model.LogEvent;
import org.munycha.logprocessor.model.ServerStorageSnapshot;
import org.munycha.logprocessor.notification.TelegramNotificationService;
import org.munycha.logprocessor.repository.AlertRepository;
import org.munycha.logprocessor.repository.ServerStorageSnapshotRepository;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class KafkaTopicConsumer implements Runnable {

    // Cap Telegram alerts queued per poll batch — suppresses log storm flooding
    private static final int MAX_TELEGRAM_ALERTS_PER_BATCH = 5;

    // Fix C: create once — both are thread-safe and immutable
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
    // Fix C: create once per consumer instance, not per record
    private final ObjectWriter prettyWriter = mapper.writerWithDefaultPrettyPrinter();
    private volatile boolean running = true;

    public KafkaTopicConsumer(KafkaConsumerFactory consumerFactory,
                              String topic,
                              TopicType type,
                              Path outputFile,
                              TelegramNotificationService notifier,
                              List<String> alertKeywords,
                              AlertRepository alertRepository,
                              ServerStorageSnapshotRepository storageSnapshotRepository,
                              ExecutorService telegramAlertExecutor) {

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

        this.consumer = consumerFactory.createConsumer();
        this.consumer.subscribe(Collections.singletonList(this.topic));
        this.notifier = notifier;
    }

    private void prepareOutputFile() {
        try {
            if (outputFile.getParent() != null) {
                Files.createDirectories(outputFile.getParent());
            }
            if (!Files.exists(outputFile)) {
                Files.createFile(outputFile);
            }
        } catch (IOException e) {
            System.err.println("[WARN] File preparation failed: " + e.getMessage());
        }
    }

    @Override
    public void run() {

        prepareOutputFile();

        try (BufferedWriter fileWriter =
                     new BufferedWriter(
                             new FileWriter(outputFile.toFile(), true),
                             64 * 1024
                     )) {

            while (running) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                boolean success = true;
                List<LogEvent> telegramQueue = new ArrayList<>();

                // Fix A: per-batch write buffer — created fresh each poll cycle.
                // Handlers write into this StringWriter, NOT directly into the file.
                // If any record fails, this buffer is simply discarded (goes out of scope)
                // and the actual BufferedWriter stays clean — no duplicate lines on retry.
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
                        System.err.printf(
                                "[RETRY] topic=%s partition=%d offset=%d reason=%s%n",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                e.getMessage()
                        );
                        break;
                    }
                }

                if (success) {
                    // Fix A: write entire batch to the real file only after all records succeed.
                    // Fix 1 (previous): flush file before committing Kafka offset.
                    fileWriter.write(batchBuffer.toString());
                    fileWriter.flush();
                    consumer.commitSync();

                    for (LogEvent ev : telegramQueue) {
                        sendTelegramAsync(ev);
                    }
                }
            }

        } catch (WakeupException ignored) {
        } catch (IOException e) {
            System.err.println("[ERROR] Writer failure: " + e.getMessage());
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

            // Fix B: snapshot + all disk_usage rows saved in one DB transaction —
            // no orphaned snapshot rows if any disk usage insert fails
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

            // Fix C: reuse static formatter — no allocation per record
            String formatted =
                    Instant.parse(event.getTimestamp())
                            .atZone(ZoneId.systemDefault())
                            .format(TIMESTAMP_FORMATTER);

            batchBuffer.write(formatted + " [" + event.getServerName() + "] " +
                    msg + System.lineSeparator());

            if (isAlert(lower)) {
                saveAlert(event);
                return event;
            }

            return null;

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

            // Fix C: reuse static formatter
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
        // telegramAlertExecutor is shared — LogProcessorApplication owns its lifecycle
    }
}
