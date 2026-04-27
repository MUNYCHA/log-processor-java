package org.munycha.logprocessor.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.munycha.logprocessor.config.TopicType;
import org.munycha.logprocessor.model.LogEvent;
import org.munycha.logprocessor.model.ServerStorageSnapshot;
import org.munycha.logprocessor.notification.AlertAggregator;
import org.munycha.logprocessor.repository.AlertRepository;
import org.munycha.logprocessor.repository.ServerStorageSnapshotRepository;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaTopicConsumer implements Runnable {

    private final String topic;
    private final TopicType type;
    private final Path outputFile;
    private final Set<String> alertKeywords;
    private final AlertRepository alertRepository;
    private final ServerStorageSnapshotRepository storageSnapshotRepository;
    private final KafkaConsumer<String, String> consumer;
    private final AlertAggregator aggregator;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ObjectWriter prettyWriter = mapper.writerWithDefaultPrettyPrinter();
    private volatile boolean running = true;

    public KafkaTopicConsumer(KafkaConsumerFactory consumerFactory,
                              String topic,
                              TopicType type,
                              Path outputFile,
                              List<String> alertKeywords,
                              AlertRepository alertRepository,
                              ServerStorageSnapshotRepository storageSnapshotRepository,
                              AlertAggregator aggregator) {

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
        this.aggregator = aggregator;

        this.consumer = consumerFactory.createConsumer();
        this.consumer.subscribe(Collections.singletonList(this.topic));
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
                StringWriter batchBuffer = new StringWriter();

                for (ConsumerRecord<String, String> record : records) {

                    try {

                        switch (type) {
                            case LOG:
                                handleLogRecord(record, batchBuffer);
                                break;
                            case METRIC:
                                handleMetricRecord(record, batchBuffer);
                                break;
                        }

                    } catch (Exception e) {
                        success = false;
                        Throwable cause = e.getCause() != null ? e.getCause() : e;
                        System.err.printf(
                                "[RETRY] topic=%s partition=%d offset=%d reason=%s cause=%s%n",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                e.getMessage(),
                                cause.getMessage()
                        );
                        cause.printStackTrace(System.err);
                        break;
                    }
                }

                if (success) {
                    fileWriter.write(batchBuffer.toString());
                    fileWriter.flush();
                    consumer.commitSync();
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

            storageSnapshotRepository.saveSnapshotWithDiskUsages(snapshot);

        } catch (Exception e) {
            throw new RuntimeException("Metric DB save failed", e);
        }
    }

    // ===================== LOG =====================

    // Returns the first keyword found in the message, or null if no match
    private String findMatchedKeyword(String lowerMsg) {
        if (type != TopicType.LOG || alertKeywords.isEmpty()) return null;
        return alertKeywords.stream()
                .filter(lowerMsg::contains)
                .findFirst()
                .orElse(null);
    }

    private void handleLogRecord(ConsumerRecord<String, String> record, Writer batchBuffer) {

        try {

            LogEvent event = mapper.readValue(record.value(), LogEvent.class);

            String msg = event.getMessage();
            batchBuffer.write(msg);
            batchBuffer.write(System.lineSeparator());

            String matchedKeyword = findMatchedKeyword(msg.toLowerCase());
            if (matchedKeyword != null) {
                saveAlert(event);
                aggregator.accept(event, matchedKeyword);
            }

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

    // ===================== SHUTDOWN =====================

    public void shutdown() {
        running = false;
        consumer.wakeup();
    }
}
