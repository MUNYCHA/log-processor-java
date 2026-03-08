package org.munycha.logprocessor;

import org.munycha.logprocessor.config.AppConfig;
import org.munycha.logprocessor.config.ConfigLoader;
import org.munycha.logprocessor.config.ConfigPathResolver;
import org.munycha.logprocessor.config.TopicConfig;
import org.munycha.logprocessor.kafka.KafkaConsumerFactory;
import org.munycha.logprocessor.kafka.KafkaTopicConsumer;
import org.munycha.logprocessor.notification.TelegramNotificationService;
import org.munycha.logprocessor.repository.AlertRepository;
import org.munycha.logprocessor.repository.DiskUsageRepository;
import org.munycha.logprocessor.repository.ServerStorageSnapshotRepository;

import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LogProcessorApplication {

    public static void main(String[] args) throws Exception {

        // Load config ONCE
        String configPath = ConfigPathResolver.resolve(
                args,
                "CONSUMER_CONFIG",          // ENV var
                "consumer.config",          // JVM system property
                "config/consumer_config.json" // classpath default
        );

        System.out.println("[Config] Using config path: " + configPath);

        ConfigLoader loader = new ConfigLoader(configPath);
        AppConfig config = loader.load();

        // Initialize repositories
        AlertRepository alertRepository = new AlertRepository(config.getDatabase());
        ServerStorageSnapshotRepository storageSnapshotRepository = new ServerStorageSnapshotRepository(config.getDatabase());
        DiskUsageRepository diskUsageRepository = new DiskUsageRepository(config.getDatabase());

        // Telegram notification service
        TelegramNotificationService notifier = new TelegramNotificationService(
                config.getTelegramBotToken(), config.getTelegramChatId()
        );

        // One thread per topic
        ExecutorService executor = Executors.newFixedThreadPool(config.getTopics().size());

        // Create KafkaConsumerFactory ONCE
        KafkaConsumerFactory consumerFactory = new KafkaConsumerFactory(
                config.getBootstrapServers(),
                "file-log-consumer"
        );

        // Start one KafkaTopicConsumer per topic
        for (TopicConfig t : config.getTopics()) {
            executor.submit(
                    new KafkaTopicConsumer(
                            consumerFactory,
                            t.getTopic(),
                            t.getType(),
                            Paths.get(t.getOutput()),
                            notifier,
                            t.getAlertKeywords(),
                            alertRepository,
                            storageSnapshotRepository,
                            diskUsageRepository
                    )
            );
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down consumers...");
            executor.shutdownNow();
        }));

        new CountDownLatch(1).await();
    }
}
