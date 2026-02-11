package org.munycha.logprocessor.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.clients.consumer.*;
import org.munycha.logprocessor.config.TopicType;
import org.munycha.logprocessor.db.AlertDB;
import org.munycha.logprocessor.db.MountPathStorageUsageDB;
import org.munycha.logprocessor.db.ServerStorageSnapshotDB;
import org.munycha.logprocessor.model.LogEvent;
import org.munycha.logprocessor.model.MountPathStorageUsage;
import org.munycha.logprocessor.model.ServerStorageSnapshot;
import org.munycha.logprocessor.telegram.TelegramNotifier;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

public final class TopicConsumer implements Runnable {

    private static final int TELEGRAM_MAX_LEN = 4000;

    private final String topic;
    private final TopicType type;
    private final Path outputFile;
    private final List<String> alertKeywords;

    private final KafkaConsumer<String, String> consumer;
    private final TelegramNotifier notifier;
    private final ObjectMapper mapper = new ObjectMapper();

    private final AlertDB alertDB;
    private final ServerStorageSnapshotDB serverStorageSnapshotDB;
    private final MountPathStorageUsageDB mountPathStorageUsageDB;

    // ===== BOUNDED EXECUTORS =====

    private final ExecutorService dbExecutor =
            new ThreadPoolExecutor(
                    3, 3,
                    0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(500),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );

    private volatile boolean running = true;

    // ===== CONSTRUCTOR =====
    public TopicConsumer(
            KafkaConsumerFactory consumerFactory,
            String topic,
            TopicType type,
            Path outputFile,
            String botToken,
            String chatId,
            List<String> alertKeywords,
            AlertDB alertDB,
            ServerStorageSnapshotDB serverStorageSnapshotDB,
            MountPathStorageUsageDB mountPathStorageUsageDB
    ) {
        this.topic = topic;
        this.type = type;
        this.outputFile = outputFile;
        this.alertKeywords = alertKeywords;
        this.alertDB = alertDB;
        this.serverStorageSnapshotDB = serverStorageSnapshotDB;
        this.mountPathStorageUsageDB = mountPathStorageUsageDB;

        this.consumer = consumerFactory.createConsumer();
        this.consumer.subscribe(Collections.singletonList(topic));

        this.notifier = new TelegramNotifier(botToken, chatId);
    }

    // ===== MAIN LOOP =====
    @Override
    public void run() {
        try (FileWriter writer = new FileWriter(outputFile.toFile(), true)) {

            while (running) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                if (records.isEmpty()) {
                    continue;
                }

                try {
                    for (ConsumerRecord<String, String> record : records) {
                        processRecord(record, writer);
                    }

                    // Commit ONLY after all records processed
                    consumer.commitSync();

                } catch (Exception e) {
                    // Do NOT commit offsets
                    System.err.println("[Consumer] Processing failed — offsets NOT committed");
                    e.printStackTrace();
                }
            }

        } catch (IOException e) {
            System.err.println("[Consumer] File error");
            e.printStackTrace();
        } finally {
            shutdownExecutors();
            consumer.close();
        }
    }

    // ===== RECORD DISPATCH =====
    private void processRecord(
            ConsumerRecord<String, String> record,
            FileWriter writer
    ) throws Exception {

        try {
            switch (type) {
                case LOG:
                    handleLog(record, writer);
                    break;
                case METRIC:
                    handleMetric(record, writer);
                    break;
                default:
                    throw new IllegalStateException("Unsupported TopicType: " + type);
            }
        } catch (Exception e) {
            // Poison message protection
            System.err.println("[Consumer] Bad message skipped");
            e.printStackTrace();
        }
    }

    // ===== LOG HANDLING =====
    private void handleLog(
            ConsumerRecord<String, String> record,
            FileWriter writer
    ) throws Exception {

        LogEvent event = mapper.readValue(record.value(), LogEvent.class);

        String time =
                Instant.parse(event.getTimestamp())
                        .atZone(ZoneId.systemDefault())
                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        writer.write(
                time + " [" + event.getServerName() + "] " +
                        event.getMessage() + System.lineSeparator()
        );
        writer.flush();

        String lower = event.getMessage().toLowerCase();
        if (alertKeywords.stream().anyMatch(lower::contains)) {
            sendAlert(event, time);
        }
    }

    // ===== METRIC HANDLING =====
    private void handleMetric(
            ConsumerRecord<String, String> record,
            FileWriter writer
    ) throws Exception {

        ServerStorageSnapshot snapshot =
                mapper.readValue(record.value(), ServerStorageSnapshot.class);

        ObjectWriter pretty = mapper.writerWithDefaultPrettyPrinter();
        writer.write(pretty.writeValueAsString(snapshot));
        writer.write(System.lineSeparator());
        writer.flush();

        // Synchronous DB write → correctness > speed
        long id = serverStorageSnapshotDB.saveSnapshot(snapshot);
        for (MountPathStorageUsage usage : snapshot.getMountPathStorageUsages()) {
            mountPathStorageUsageDB.savePath(id, usage);
        }
    }

    // ===== ALERT =====
    private void sendAlert(LogEvent event, String time) {

        String msg =
                "ALERT\n" +
                        " Time: " + time + "\n" +
                        " Host: " + event.getServerName() + "\n" +
                        " File: " + event.getPath() + "\n" +
                        " Topic: " + event.getTopic() + "\n" +
                        " Message: " + event.getMessage();

        String finalMsg =
                msg.length() > TELEGRAM_MAX_LEN
                        ? msg.substring(0, TELEGRAM_MAX_LEN)
                        : msg;

        this.notifier.sendMessage(finalMsg);

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

    // ===== SHUTDOWN =====
    public void shutdown() {
        running = false;
        consumer.wakeup();
    }

    private void shutdownExecutors() {
        dbExecutor.shutdown();
    }
}
