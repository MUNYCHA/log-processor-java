package org.munycha.logprocessor.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.clients.consumer.*;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public class TopicConsumer implements Runnable {

    private final String topic;
    private final TopicType type;
    private final Path outputFile;
    private final List<String> alertKeywords;
    private final AlertDB alertDB;
    private final ServerStorageSnapshotDB serverStorageSnapshotDB;
    private final MountPathStorageUsageDB mountPathStorageUsageDB;
    private final KafkaConsumer<String, String> consumer;
    private final TelegramNotifier notifier;
    private final ObjectMapper mapper = new ObjectMapper();
    private volatile boolean running = true;

    private static final ExecutorService telegramAlertExecutor =
            new ThreadPoolExecutor(
                    1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(200),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );

    private static final ExecutorService dbExecutor =
            new ThreadPoolExecutor(
                    3, 3,
                    0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(1000),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );

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
        this.alertKeywords = alertKeywords;
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
                            default:
                                System.err.println("[WARN] Unknown TopicType: " + type);
                        }

                    } catch (Exception e) {
                        System.err.printf(
                                "[SKIPPED] topic=%s offset=%d error=%s%n",
                                record.topic(),
                                record.offset(),
                                e.getMessage()
                        );
                    }
                }

                consumer.commitSync();;
            }

        } catch (WakeupException ignored) {
        } catch (IOException e) {
            System.err.println("[ERROR] Writer failure: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }

    private void handleMetricRecord(ConsumerRecord<String, String> record, FileWriter writer) {

        try {

            ServerStorageSnapshot snapshot =
                    mapper.readValue(record.value(), ServerStorageSnapshot.class);

            try {
                ObjectWriter pw = mapper.writerWithDefaultPrettyPrinter();
                writer.write(pw.writeValueAsString(snapshot));
                writer.write(System.lineSeparator());
                writer.flush();
            } catch (IOException ioe) {
                System.err.println("[WARN] File write failed: " + ioe.getMessage());
            }

            dbExecutor.submit(() -> {
                try {
                    long id = serverStorageSnapshotDB.saveSnapshot(snapshot);
                    for (MountPathStorageUsage m : snapshot.getMountPathStorageUsages()) {
                        mountPathStorageUsageDB.savePath(id, m);
                    }
                } catch (Exception ex) {
                    System.err.println("[METRIC][DB] Save failed: " + ex.getMessage());
                }
            });

        } catch (Exception e) {
            throw new RuntimeException("Metric deserialization failed", e);
        }
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

            try {
                writer.write(formatted + " [" + event.getServerName() + "] " +
                        msg + System.lineSeparator());
                writer.flush();
            } catch (IOException ioe) {
                System.err.println("[WARN] File write failed: " + ioe.getMessage());
            }

            if (alertKeywords.stream().anyMatch(lower::contains)) {
                processAlert(event);
            }

        } catch (Exception e) {
            throw new RuntimeException("Log deserialization failed", e);
        }
    }

    private void processAlert(LogEvent event) {

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

        telegramAlertExecutor.submit(() -> {
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
            System.err.println("[ALERT] Telegram send failed after retries");
        });

        dbExecutor.submit(() -> {
            try {
                alertDB.saveAlert(
                        event.getTopic(),
                        event.getTimestamp(),
                        event.getServerName(),
                        event.getPath(),
                        event.getMessage()
                );
            } catch (Exception e) {
                System.err.println("[ALERT][DB] Save failed: " + e.getMessage());
            }
        });
    }

    public void shutdown() {

        System.out.println("[SHUTDOWN] Initiated...");

        // 1. Stop poll loop
        running = false;
        consumer.wakeup();

        // 2. Stop accepting new async tasks
        telegramAlertExecutor.shutdown();
        dbExecutor.shutdown();

        try {

            // 3. Wait for DB tasks to complete
            if (!dbExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                System.err.println("[SHUTDOWN] DB tasks did not finish in time. Forcing...");
                dbExecutor.shutdownNow();
            }

            // 4. Wait for Telegram tasks
            if (!telegramAlertExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                System.err.println("[SHUTDOWN] Telegram tasks did not finish in time. Forcing...");
                telegramAlertExecutor.shutdownNow();
            }

        } catch (InterruptedException e) {

            System.err.println("[SHUTDOWN] Interrupted while waiting. Forcing shutdown...");
            dbExecutor.shutdownNow();
            telegramAlertExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        System.out.println("[SHUTDOWN] Executors drained. Safe to exit.");
    }

}
