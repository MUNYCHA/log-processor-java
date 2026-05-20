package org.munycha.logprocessor.bootstrap;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.munycha.logprocessor.config.AppConfig;
import org.munycha.logprocessor.config.TopicConfig;
import org.munycha.logprocessor.config.TopicType;
import org.munycha.logprocessor.kafka.KafkaConsumerFactory;
import org.munycha.logprocessor.kafka.TopicPollLoop;
import org.munycha.logprocessor.log.AlertDetector;
import org.munycha.logprocessor.log.AlertPatternStore;
import org.munycha.logprocessor.log.LogMessageNormalizer;
import org.munycha.logprocessor.notification.TelegramAlertFormatter;
import org.munycha.logprocessor.notification.TelegramNotificationService;
import org.munycha.logprocessor.pipeline.BatchFileWriter;
import org.munycha.logprocessor.pipeline.LogRecordHandler;
import org.munycha.logprocessor.pipeline.MetricRecordHandler;
import org.munycha.logprocessor.pipeline.RecordHandler;
import org.munycha.logprocessor.pipeline.TopicContext;
import org.munycha.logprocessor.repository.AlertRepository;
import org.munycha.logprocessor.repository.ServerStorageSnapshotRepository;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Wires the application up from configuration to a fully-running state:
 * validates paths, builds repositories and notifier, constructs one
 * {@link TopicPollLoop} per configured topic, and submits each to the
 * consumer executor. Returns a {@link RunningApplication} handle so the
 * caller can register a shutdown hook.
 */
public final class ApplicationBootstrap {

    private static final String KAFKA_CONSUMER_GROUP_ID = "file-log-consumer";

    private ApplicationBootstrap() {}

    public static RunningApplication start(AppConfig config) throws IOException {
        for (TopicConfig t : config.getTopics()) {
            PathValidator.validate(t);
        }

        AlertRepository alertRepository = new AlertRepository(config.getDatabase());
        ServerStorageSnapshotRepository snapshotRepository =
                new ServerStorageSnapshotRepository(config.getDatabase());

        TelegramNotificationService notifier = new TelegramNotificationService(
                config.getTelegramBotToken(), config.getTelegramChatId());
        TelegramAlertFormatter alertFormatter = new TelegramAlertFormatter();

        // Shared, thread-safe Jackson mapper for all handlers.
        ObjectMapper jsonMapper = new ObjectMapper();

        ExecutorService telegramExecutor = ExecutorFactory.telegramExecutor();
        ExecutorService consumerExecutor = ExecutorFactory.consumerExecutor(config.getTopics().size());

        KafkaConsumerFactory consumerFactory = new KafkaConsumerFactory(
                config.getBootstrapServers(), KAFKA_CONSUMER_GROUP_ID);

        List<TopicPollLoop> pollLoops = new ArrayList<>();
        for (TopicConfig t : config.getTopics()) {
            RecordHandler handler = buildHandler(t, jsonMapper, alertRepository, snapshotRepository);
            BatchFileWriter batchFileWriter = new BatchFileWriter(Paths.get(t.getOutput()));
            TopicContext ctx = new TopicContext(t.getTopic(), handler, batchFileWriter);

            TopicPollLoop pollLoop = new TopicPollLoop(
                    consumerFactory, ctx, notifier, alertFormatter, telegramExecutor);
            pollLoops.add(pollLoop);
            consumerExecutor.submit(pollLoop);
        }

        return new RunningApplication(pollLoops, consumerExecutor, telegramExecutor);
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
}
