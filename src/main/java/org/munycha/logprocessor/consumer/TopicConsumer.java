package org.munycha.logprocessor.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.munycha.logprocessor.config.TopicType;
import org.munycha.logprocessor.db.*;
import org.munycha.logprocessor.model.*;
import org.munycha.logprocessor.telegram.TelegramNotifier;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class TopicConsumer implements Runnable {

    private final String topic;
    private final TopicType type;
    private final Path outputFile;
    private final Set<String> alertKeywords;
    private final AlertDB alertDB;
    private final ServerStorageSnapshotDB serverStorageSnapshotDB;
    private final MountPathStorageUsageDB mountPathStorageUsageDB;
    private final KafkaConsumer<String, String> consumer;
    private final TelegramNotifier notifier;
    private final ObjectMapper mapper = new ObjectMapper();
    private volatile boolean running = true;

    private final ExecutorService telegramAlertExecutor =
            Executors.newSingleThreadExecutor();

    public TopicConsumer(KafkaConsumerFactory consumerFactory,
                         String topic,
                         TopicType type,
                         Path outputFile,
                         TelegramNotifier notifier,
                         List<String> alertKeywords,
                         AlertDB alertDB,
                         ServerStorageSnapshotDB serverStorageSnapshotDB,
                         MountPathStorageUsageDB mountPathStorageUsageDB) {

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
        this.alertDB = alertDB;
        this.serverStorageSnapshotDB = serverStorageSnapshotDB;
        this.mountPathStorageUsageDB = mountPathStorageUsageDB;

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

        try (FileWriter writer = new FileWriter(outputFile.toFile(), true)) {

            while (running) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {

                    try {

                        switch (type) {
                            case LOG:
                                handleLogRecord(record, writer);
                                break;
                            case METRIC:
                                handleMetricRecord(record, writer);
                                break;
                        }

                    } catch (Exception e) {
                        System.err.printf(
                                "[RETRY] topic=%s partition=%d offset=%d reason=%s%n",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                e.getMessage()
                        );
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

    private void commit(ConsumerRecord<String, String> record) {

        TopicPartition tp = new TopicPartition(record.topic(), record.partition());

        consumer.commitSync(Collections.singletonMap(
                tp,
                new OffsetAndMetadata(record.offset() + 1)
        ));
    }

    // ===================== METRIC =====================

    private void handleMetricRecord(ConsumerRecord<String, String> record, FileWriter writer) {

        try {

            ServerStorageSnapshot snapshot =
                    mapper.readValue(record.value(), ServerStorageSnapshot.class);

            ObjectWriter pw = mapper.writerWithDefaultPrettyPrinter();
            writer.write(pw.writeValueAsString(snapshot));
            writer.write(System.lineSeparator());
            writer.flush();

            long id = serverStorageSnapshotDB.saveSnapshot(snapshot);

            for (MountPathStorageUsage m : snapshot.getMountPathStorageUsages()) {
                mountPathStorageUsageDB.savePath(id, m);
            }

            commit(record);

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

    private void handleLogRecord(ConsumerRecord<String, String> record, FileWriter writer) {

        try {

            LogEvent event = mapper.readValue(record.value(), LogEvent.class);

            String msg = event.getMessage();
            String lower = msg.toLowerCase();

            String formatted =
                    Instant.parse(event.getTimestamp())
                            .atZone(ZoneId.systemDefault())
                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            writer.write(formatted + " [" + event.getServerName() + "] " +
                    msg + System.lineSeparator());
            writer.flush();

            if (isAlert(lower)) {
                saveAlertDB(event);
                sendTelegramAsync(event);
            }

            commit(record);

        } catch (Exception e) {
            throw new RuntimeException("Log processing failed", e);
        }
    }

    // ===================== ALERT =====================

    private void saveAlertDB(LogEvent event) {

        alertDB.saveAlert(
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
                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            String message =
                    "ALERT\nTime: " + formatted +
                            "\nHost: " + event.getServerName() +
                            "\nFile: " + event.getPath() +
                            "\nTopic: " + event.getTopic() +
                            "\nMessage: " + event.getMessage();

            int retries = 3;

            while (retries-- > 0) {
                try {
                    notifier.sendMessage(message);
                    return;
                } catch (Exception e) {
                    try { Thread.sleep(3000); }
                    catch (InterruptedException ignored) {}
                }
            }
        });
    }

    // ===================== SHUTDOWN =====================

    public void shutdown() {

        running = false;
        consumer.wakeup();

        telegramAlertExecutor.shutdown();

        try {
            telegramAlertExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {}
    }
}
