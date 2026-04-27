package org.munycha.logprocessor;

import org.munycha.logprocessor.config.AppConfig;
import org.munycha.logprocessor.config.ConfigLoader;
import org.munycha.logprocessor.config.ConfigPathResolver;
import org.munycha.logprocessor.config.TopicConfig;
import org.munycha.logprocessor.kafka.KafkaConsumerFactory;
import org.munycha.logprocessor.kafka.KafkaTopicConsumer;
import org.munycha.logprocessor.notification.AlertAggregator;
import org.munycha.logprocessor.notification.TelegramNotificationService;
import org.munycha.logprocessor.repository.AlertRepository;
import org.munycha.logprocessor.repository.ServerStorageSnapshotRepository;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class LogProcessorApplication {

    public static void main(String[] args) throws Exception {

        String configPath = ConfigPathResolver.resolve(
                args,
                "CONSUMER_CONFIG",
                "consumer.config",
                "config/consumer_config.json"
        );

        System.out.println("[Config] Using config path: " + configPath);

        ConfigLoader loader = new ConfigLoader(configPath);
        AppConfig config = loader.load();

        AlertRepository alertRepository = new AlertRepository(config.getDatabase());
        ServerStorageSnapshotRepository storageSnapshotRepository =
                new ServerStorageSnapshotRepository(config.getDatabase());

        TelegramNotificationService notifier = new TelegramNotificationService(
                config.getTelegramBotToken(), config.getTelegramChatId()
        );

        // ONE shared single-threaded executor — all aggregators submit here so
        // Telegram sends across all topics remain serialized on one thread.
        ExecutorService telegramAlertExecutor = new ThreadPoolExecutor(
                1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1000),
                new ThreadPoolExecutor.DiscardPolicy()
        );

        ExecutorService consumerExecutor = Executors.newFixedThreadPool(config.getTopics().size());

        KafkaConsumerFactory consumerFactory = new KafkaConsumerFactory(
                config.getBootstrapServers(),
                "file-log-consumer"
        );

        List<KafkaTopicConsumer> consumers = new ArrayList<>();
        List<AlertAggregator> aggregators = new ArrayList<>();

        for (TopicConfig t : config.getTopics()) {

            long cooldownMs = TimeUnit.MINUTES.toMillis(t.getAlertCooldownMinutes());

            AlertAggregator aggregator = new AlertAggregator(
                    notifier,
                    telegramAlertExecutor,
                    cooldownMs,
                    t.getAlertThresholdCount()
            );
            aggregators.add(aggregator);

            KafkaTopicConsumer consumer = new KafkaTopicConsumer(
                    consumerFactory,
                    t.getTopic(),
                    t.getType(),
                    Paths.get(t.getOutput()),
                    t.getAlertKeywords(),
                    alertRepository,
                    storageSnapshotRepository,
                    aggregator
            );
            consumers.add(consumer);
            consumerExecutor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[Shutdown] Signalling consumers to stop...");
            for (KafkaTopicConsumer consumer : consumers) {
                consumer.shutdown();
            }

            consumerExecutor.shutdown();
            try {
                if (!consumerExecutor.awaitTermination(15, TimeUnit.SECONDS)) {
                    System.err.println("[Shutdown] Consumer threads did not stop in time, forcing.");
                    consumerExecutor.shutdownNow();
                }
            } catch (InterruptedException ignored) {
                consumerExecutor.shutdownNow();
            }

            System.out.println("[Shutdown] Stopping aggregator schedulers...");
            for (AlertAggregator aggregator : aggregators) {
                aggregator.shutdown();
            }

            System.out.println("[Shutdown] Draining pending Telegram alerts...");
            telegramAlertExecutor.shutdown();
            try {
                if (!telegramAlertExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    System.err.println("[Shutdown] Telegram executor did not drain in time, forcing.");
                    telegramAlertExecutor.shutdownNow();
                }
            } catch (InterruptedException ignored) {
                telegramAlertExecutor.shutdownNow();
            }

            System.out.println("[Shutdown] Complete.");
        }));

        new CountDownLatch(1).await();
    }
}
