package org.munycha.kafkaconsumer;

import org.munycha.kafkaconsumer.config.AppConfig;
import org.munycha.kafkaconsumer.config.ConfigLoader;
import org.munycha.kafkaconsumer.config.ConfigPathResolver;
import org.munycha.kafkaconsumer.config.TopicConfig;
import org.munycha.kafkaconsumer.consumer.TopicConsumer;
import org.munycha.kafkaconsumer.db.AlertDB;
import org.munycha.kafkaconsumer.db.MountPathStorageUsageDB;
import org.munycha.kafkaconsumer.db.ServerStorageSnapshotDB;

import java.nio.file.Path;
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

        // Create a thread pool — one consumer thread per topic
        ExecutorService executor = Executors.newFixedThreadPool(config.getTopics().size());

        // Start one TopicConsumer per topic defined in config
        for (TopicConfig t : config.getTopics()) {
            executor.submit(new TopicConsumer(
                    config.getBootstrapServers(),
                    t.getTopic(),
                    t.getType(),
                    Paths.get(t.getOutput()),
                    config.getTelegramBotToken(),
                    config.getTelegramChatId(),
                    config.getAlertKeywords(),
                    alertDatabase,
                    serverStorageUsageDB,
                    mountPathStorageUsageDB
            ));
        }


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down consumers...");
            executor.shutdownNow();
        }));
    }
}
