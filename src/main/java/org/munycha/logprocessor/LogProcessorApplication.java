package org.munycha.logprocessor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.munycha.logprocessor.config.AppConfig;
import org.munycha.logprocessor.config.ConfigLoader;
import org.munycha.logprocessor.config.ConfigPathResolver;
import org.munycha.logprocessor.config.TopicConfig;
import org.munycha.logprocessor.config.TopicType;
import org.munycha.logprocessor.kafka.KafkaConsumerFactory;
import org.munycha.logprocessor.kafka.KafkaTopicConsumer;
import org.munycha.logprocessor.log.AlertDetector;
import org.munycha.logprocessor.log.AlertPatternStore;
import org.munycha.logprocessor.log.LogMessageNormalizer;
import org.munycha.logprocessor.notification.TelegramAlertFormatter;
import org.munycha.logprocessor.notification.TelegramNotificationService;
import org.munycha.logprocessor.pipeline.LogRecordHandler;
import org.munycha.logprocessor.pipeline.MetricRecordHandler;
import org.munycha.logprocessor.pipeline.RecordHandler;
import org.munycha.logprocessor.repository.AlertRepository;
import org.munycha.logprocessor.repository.ServerStorageSnapshotRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class LogProcessorApplication {

    private static final Logger log = LoggerFactory.getLogger(LogProcessorApplication.class);

    public static void main(String[] args) throws Exception {

        // Load config ONCE
        String configPath = ConfigPathResolver.resolve(
                args,
                "CONSUMER_CONFIG",            // ENV var
                "consumer.config",            // JVM system property
                "config/consumer_config.json" // classpath default
        );

        log.info("Using config path: {}", configPath);

        ConfigLoader loader = new ConfigLoader(configPath);
        AppConfig config = loader.load();

        // Initialize repositories
        AlertRepository alertRepository = new AlertRepository(config.getDatabase());
        // Snapshot repository now handles disk_usage inserts in the same transaction
        ServerStorageSnapshotRepository storageSnapshotRepository =
                new ServerStorageSnapshotRepository(config.getDatabase());

        // Telegram notification service + shared message formatter
        TelegramNotificationService notifier = new TelegramNotificationService(
                config.getTelegramBotToken(), config.getTelegramChatId()
        );
        TelegramAlertFormatter alertFormatter = new TelegramAlertFormatter();

        // Shared, thread-safe Jackson mapper for all handlers.
        ObjectMapper jsonMapper = new ObjectMapper();

        // Validate all configured paths exist and are writable before starting anything
        for (TopicConfig t : config.getTopics()) {
            validatePaths(t);
        }

        // ONE shared single-threaded executor for all Telegram sends across all topics.
        // One queue, one sender thread — prevents concurrent topics hammering the API.
        ExecutorService telegramAlertExecutor = new ThreadPoolExecutor(
                1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1000),
                new ThreadPoolExecutor.DiscardPolicy()
        );

        // One thread per topic
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(config.getTopics().size());

        // Create KafkaConsumerFactory ONCE
        KafkaConsumerFactory consumerFactory = new KafkaConsumerFactory(
                config.getBootstrapServers(),
                "file-log-consumer"
        );

        // Keep references so shutdown() can be called on each consumer
        List<KafkaTopicConsumer> consumers = new ArrayList<>();

        // Start one KafkaTopicConsumer per topic
        for (TopicConfig t : config.getTopics()) {
            RecordHandler handler = buildHandler(
                    t, jsonMapper, alertRepository, storageSnapshotRepository);

            KafkaTopicConsumer consumer = new KafkaTopicConsumer(
                    consumerFactory,
                    t.getTopic(),
                    Paths.get(t.getOutput()),
                    handler,
                    notifier,
                    alertFormatter,
                    telegramAlertExecutor
            );
            consumers.add(consumer);
            consumerExecutor.submit(consumer);
        }

        // Graceful shutdown sequence:
        //   1. signal each consumer (sets running=false, wakes up Kafka poll)
        //   2. wait for consumer threads to finish their current batch and exit
        //   3. drain the telegram queue so queued alerts are not silently discarded
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Signalling consumers to stop...");
            for (KafkaTopicConsumer consumer : consumers) {
                consumer.shutdown();
            }

            consumerExecutor.shutdown();
            try {
                if (!consumerExecutor.awaitTermination(15, TimeUnit.SECONDS)) {
                    log.warn("Consumer threads did not stop in time, forcing.");
                    consumerExecutor.shutdownNow();
                }
            } catch (InterruptedException ignored) {
                consumerExecutor.shutdownNow();
            }

            log.info("Draining pending Telegram alerts...");
            telegramAlertExecutor.shutdown();
            try {
                if (!telegramAlertExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                    log.warn("Telegram executor did not drain in time, forcing.");
                    telegramAlertExecutor.shutdownNow();
                }
            } catch (InterruptedException ignored) {
                telegramAlertExecutor.shutdownNow();
            }

            log.info("Shutdown complete.");
        }));

        new CountDownLatch(1).await();
    }

    private static RecordHandler buildHandler(TopicConfig t,
                                              ObjectMapper jsonMapper,
                                              AlertRepository alertRepository,
                                              ServerStorageSnapshotRepository snapshotRepository) throws IOException {
        if (t.getType() == TopicType.METRIC) {
            return new MetricRecordHandler(jsonMapper, snapshotRepository);
        }

        AlertPatternStore patternStore = t.hasPatternStore()
                ? new AlertPatternStore(Paths.get(t.getPatternStoreFile()))
                : null;
        LogMessageNormalizer normalizer = buildNormalizer(t);
        AlertDetector alertDetector = new AlertDetector(t.getAlertKeywords());

        return new LogRecordHandler(jsonMapper, alertDetector, normalizer, patternStore, alertRepository);
    }

    private static LogMessageNormalizer buildNormalizer(TopicConfig t) {
        if (!t.hasCustomNormalizationRules()) {
            return new LogMessageNormalizer();
        }
        List<LogMessageNormalizer.Rule> rules = new ArrayList<>();
        for (TopicConfig.NormalizationRule r : t.getCustomNormalizationRules()) {
            if (r.getPattern() == null || r.getReplacement() == null) continue;
            rules.add(new LogMessageNormalizer.Rule(r.getPattern(), r.getReplacement()));
        }
        return new LogMessageNormalizer(rules);
    }

    private static void validatePaths(TopicConfig t) throws IOException {
        Path outputPath = Paths.get(t.getOutput());
        if (!Files.isRegularFile(outputPath) || !Files.isWritable(outputPath)) {
            throw new IOException(
                    "[Startup] Output file does not exist or is not writable: " + t.getOutput()
            );
        }

        if (t.hasPatternStore()) {
            Path patternPath = Paths.get(t.getPatternStoreFile());
            if (!Files.isRegularFile(patternPath) || !Files.isWritable(patternPath)) {
                throw new IOException(
                        "[Startup] Pattern store file does not exist or is not writable: " +
                                t.getPatternStoreFile()
                );
            }
        }
    }
}
