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

import java.io.BufferedWriter;
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
            new ThreadPoolExecutor(
                    1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(1000),
                    new ThreadPoolExecutor.DiscardPolicy()
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

        try (BufferedWriter writer =
                     new BufferedWriter(
                             new FileWriter(outputFile.toFile(), true),
                             64 * 1024
                     )) {

            while (running) {

                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                boolean success = true;
                List<LogEvent> telegramQueue = new ArrayList<>();
                for (ConsumerRecord<String, String> record : records) {

                    try {

                        switch (type) {
                            case LOG:
                                LogEvent alertEvent = handleLogRecord(record, writer);
                                if (alertEvent != null) {
                                    telegramQueue.add(alertEvent);
                                }
                                break;
                            case METRIC:
                                handleMetricRecord(record, writer);
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
                if(success){
                    consumer.commitSync();
                    writer.flush();

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

    private void handleMetricRecord(ConsumerRecord<String, String> record, BufferedWriter writer) {

        try {

            ServerStorageSnapshot snapshot =
                    mapper.readValue(record.value(), ServerStorageSnapshot.class);

            ObjectWriter pw = mapper.writerWithDefaultPrettyPrinter();
            writer.write(pw.writeValueAsString(snapshot));
            writer.write(System.lineSeparator());

            long id = serverStorageSnapshotDB.saveSnapshot(snapshot);

            for (MountPathStorageUsage m : snapshot.getMountPathStorageUsages()) {
                mountPathStorageUsageDB.savePath(id, m);
            }

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

    private LogEvent handleLogRecord(ConsumerRecord<String, String> record, BufferedWriter writer) {

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

            boolean alert = isAlert(lower);

            if (alert) {
                saveAlertDB(event);
                return event;
            }

            return null;

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
