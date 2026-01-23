package org.munycha.kafkaconsumer.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.clients.consumer.*;
import org.munycha.kafkaconsumer.config.TopicType;
import org.munycha.kafkaconsumer.db.MountPathStorageUsageDB;
import org.munycha.kafkaconsumer.db.ServerStorageSnapshotDB;
import org.munycha.kafkaconsumer.model.LogEvent;
import org.munycha.kafkaconsumer.model.MountPathStorageUsage;
import org.munycha.kafkaconsumer.model.ServerStorageSnapshot;
import org.munycha.kafkaconsumer.telegram.TelegramNotifier;
import org.munycha.kafkaconsumer.db.AlertDB;


import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


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
    private final KafkaConsumerFactory consumerFactory;
    private final ObjectMapper mapper = new ObjectMapper();

    //Create a single thread executor for Telegram alerts
    private static final ExecutorService telegramAlertExecutor = Executors.newSingleThreadExecutor();

    // Create a thread pool for database saving
    private static final ExecutorService dbExecutor = Executors.newFixedThreadPool(3);


    // Shutdown thread pool alertExecutor and dbExecutor
    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down executors...");

            telegramAlertExecutor.shutdown();
            dbExecutor.shutdown();

            try {
                if (!telegramAlertExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    telegramAlertExecutor.shutdownNow();
                }
                if (!dbExecutor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)) {
                    dbExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                telegramAlertExecutor.shutdownNow();
                dbExecutor.shutdownNow();
            }

            System.out.println("All executors shut down cleanly.");
        }));
    }


    public TopicConsumer(String bootstrapServers,
                         String topic,
                         TopicType type,
                         Path outputFile,
                         String botToken,
                         String chatId,
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

        this.consumerFactory = new KafkaConsumerFactory(bootstrapServers, topic);
        this.consumer = this.consumerFactory.createConsumer();
        this.consumer.subscribe(Collections.singletonList(this.topic));
        this.notifier = new TelegramNotifier(botToken, chatId);
    }


    private void ensureOutputFileExists() {
        if (!Files.exists(outputFile)) {
            System.err.println(" Output file does NOT exist: " + outputFile);
            System.err.println("   Please create it manually. Consumer will NOT write.");
        }
    }



    @Override
    public void run() {

        ensureOutputFileExists();

        try (FileWriter writer = new FileWriter(outputFile.toFile(), true)) {
            System.out.printf("Listening to %s → writing to %s%n", topic, outputFile);

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {

                    switch (type) {

                        case LOG:
                            handleLogRecord(record, writer);
                            break;

                        case METRIC:
                            handleMetricRecord(record, writer);
                            break;

                        default:
                            throw new IllegalStateException(
                                    "Unsupported TopicType: " + type
                            );
                    }
                }
            }


        } catch (IOException e) {
            System.err.println("File error: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }

    private void handleMetricRecord(
            ConsumerRecord<String, String> record,
            FileWriter writer
    ) throws IOException {

        ServerStorageSnapshot serverStorageSnapshot =
                mapper.readValue(record.value(), ServerStorageSnapshot.class);

        System.out.println("===== SERVER STORAGE SNAPSHOT RECEIVED =====");
        System.out.println("Server   : " + serverStorageSnapshot.getServerName());
        System.out.println("IP       : " + serverStorageSnapshot.getServerIp());
        System.out.println("Timestamp: " + serverStorageSnapshot.getTimestamp());

        for (MountPathStorageUsage spsu : serverStorageSnapshot.getMountPathStorageUsages()) {
            System.out.printf(
                    "Path: %-12s | Used: %6.2f%% | Used: %d / %d bytes%n",
                    spsu.getPath(),
                    spsu.getUsedPercent(),
                    spsu.getUsedBytes(),
                    spsu.getTotalBytes()
            );
        }
        System.out.println("=========================================");


        if (Files.exists(outputFile)) {
            ObjectWriter prettyWriter =
                    mapper.writerWithDefaultPrettyPrinter();

            writer.write(prettyWriter.writeValueAsString(serverStorageSnapshot));
            writer.write(System.lineSeparator());
            writer.flush();
        }


        dbExecutor.submit(() -> {
            try {
                long serverStorageUsageId =
                        serverStorageSnapshotDB.saveSnapshot(serverStorageSnapshot);

                for (MountPathStorageUsage spsu : serverStorageSnapshot.getMountPathStorageUsages()) {
                    mountPathStorageUsageDB.savePath(serverStorageUsageId, spsu);
                }

            } catch (Exception e) {
                System.err.println("[METRIC][DB] Failed to save storage metrics");
                e.printStackTrace();
            }
        });
    }



    private void handleLogRecord(ConsumerRecord<String, String> record, FileWriter writer) throws IOException {

        LogEvent event = mapper.readValue(record.value(), LogEvent.class);

        String msg = event.getMessage();
        String lowerMsg = msg.toLowerCase();

        String formattedTime =
                Instant.parse(event.getTimestamp())
                        .atZone(ZoneId.systemDefault())
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));


        System.out.printf(
                "[%s] (%s) %s%n",
                formattedTime,
                event.getTopic(),
                msg
        );

        if (Files.exists(outputFile)) {
            writer.write(
                    formattedTime + " [" + event.getServerName() + "] " +
                            msg + System.lineSeparator()
            );
            writer.flush();
        }

        boolean alert = alertKeywords.stream()
                .anyMatch(k -> lowerMsg.contains(k));

        if (alert) {
            processAlert(event);
        }
    }

    private void processAlert(LogEvent event) {

        String formattedTime =
                Instant.parse(event.getTimestamp())
                        .atZone(ZoneId.systemDefault())
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));


        String alertMessage =
                "ALERT\n" +
                        " Time: " + formattedTime + "\n" +
                        " Host: " + event.getServerName() + "\n" +
                        " File: " + event.getPath() + "\n" +
                        " Topic: " + event.getTopic() + "\n" +
                        " Message: " + event.getMessage();

        telegramAlertExecutor.submit(() -> notifier.sendMessage(alertMessage));

        dbExecutor.submit(() ->
                alertDB.saveAlert(
                        event.getTopic(),
                        event.getTimestamp(),
                        event.getServerName(),
                        event.getPath(),
                        event.getMessage()
                )
        );
    }



}