package org.munycha.logprocessor;

import org.munycha.logprocessor.config.AppConfig;
import org.munycha.logprocessor.config.ConfigLoader;
import org.munycha.logprocessor.config.ConfigPathResolver;
import org.munycha.logprocessor.config.TopicConfig;
import org.munycha.logprocessor.consumer.KafkaConsumerFactory;
import org.munycha.logprocessor.consumer.TopicConsumer;
import org.munycha.logprocessor.db.AlertDB;
import org.munycha.logprocessor.db.MountPathStorageUsageDB;
import org.munycha.logprocessor.db.ServerStorageSnapshotDB;
import org.munycha.logprocessor.telegram.TelegramNotifier;

import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AppMain {

    public static void main(String[] args) throws Exception {

        // Load config ONCE
        String configPath = ConfigPathResolver.resolve(
                args,
                "CONSUMER_CONFIG",          // ENV var
                "consumer.config",                 // JVM system property
                "config/consumer_config.json"      // classpath default
        );

        System.out.println("[Config] Using config path: " + configPath);

        ConfigLoader loader = new ConfigLoader(configPath);
        AppConfig config = loader.load();


        // Initialize alert database
        AlertDB alertDatabase = new AlertDB(config.getDatabase());

        //Initialize system storage snapshot database
        ServerStorageSnapshotDB serverStorageUsageDB = new ServerStorageSnapshotDB(config.getDatabase());

        //Initialize path storage database
        MountPathStorageUsageDB mountPathStorageUsageDB = new MountPathStorageUsageDB(config.getDatabase());

        //telegram notifier
        TelegramNotifier notifier = new TelegramNotifier(config.getTelegramBotToken(),config.getTelegramChatId());

        // One thread per topic
        ExecutorService executor =
                Executors.newFixedThreadPool(config.getTopics().size());

        // Create KafkaConsumerFactory ONCE
                KafkaConsumerFactory consumerFactory =
                        new KafkaConsumerFactory(
                                config.getBootstrapServers(),
                                "file-log-consumer"
                        );

        // Start one TopicConsumer per topic
                for (TopicConfig t : config.getTopics()) {

                    executor.submit(
                            new TopicConsumer(
                                    consumerFactory,
                                    t.getTopic(),
                                    t.getType(),
                                    Paths.get(t.getOutput()),
                                    notifier,
                                    t.getAlertKeywords(),
                                    alertDatabase,
                                    serverStorageUsageDB,
                                    mountPathStorageUsageDB
                            )
                    );
                }



        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down consumers...");
            executor.shutdownNow();
        }));

        new java.util.concurrent.CountDownLatch(1).await();
    }
}
