package org.munycha.logprocessor.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.munycha.logprocessor.log.LogEvent;
import org.munycha.logprocessor.notification.Notifier;
import org.munycha.logprocessor.notification.TelegramAlertFormatter;
import org.munycha.logprocessor.pipeline.BatchFileWriter;
import org.munycha.logprocessor.pipeline.RecordHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * Long-running Kafka poll loop for one topic. Delegates per-record work
 * to a {@link RecordHandler}; collects up to {@value #MAX_TELEGRAM_ALERTS_PER_BATCH}
 * alert events per poll and dispatches them on a shared executor after
 * the batch is committed.
 *
 * Records are buffered in memory, flushed to disk in one append per
 * poll cycle, and only then committed to Kafka. If the handler throws,
 * the entire batch is discarded and the same offsets are re-polled.
 */
public class KafkaTopicConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicConsumer.class);

    private static final int MAX_TELEGRAM_ALERTS_PER_BATCH = 5;

    private final String topic;
    private final BatchFileWriter batchFileWriter;
    private final RecordHandler handler;
    private final KafkaConsumer<String, String> consumer;
    private final Notifier notifier;
    private final TelegramAlertFormatter alertFormatter;
    private final ExecutorService telegramAlertExecutor;

    private volatile boolean running = true;

    public KafkaTopicConsumer(KafkaConsumerFactory consumerFactory,
                              String topic,
                              Path outputFile,
                              RecordHandler handler,
                              Notifier notifier,
                              TelegramAlertFormatter alertFormatter,
                              ExecutorService telegramAlertExecutor) {
        this.topic = topic;
        this.batchFileWriter = new BatchFileWriter(outputFile);
        this.handler = handler;
        this.notifier = notifier;
        this.alertFormatter = alertFormatter;
        this.telegramAlertExecutor = telegramAlertExecutor;

        this.consumer = consumerFactory.createConsumer();
        this.consumer.subscribe(Collections.singletonList(this.topic));
    }

    @Override
    public void run() {
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                boolean success = true;
                List<LogEvent> telegramQueue = new ArrayList<>();
                StringWriter batchBuffer = new StringWriter();

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Optional<LogEvent> alertEvent = handler.handle(record, batchBuffer);
                        if (alertEvent.isPresent() && telegramQueue.size() < MAX_TELEGRAM_ALERTS_PER_BATCH) {
                            telegramQueue.add(alertEvent.get());
                        }
                    } catch (Exception e) {
                        success = false;
                        Throwable cause = e.getCause() != null ? e.getCause() : e;
                        log.warn("Retry triggered: topic={} partition={} offset={} reason={} cause={}",
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                e.getMessage(),
                                cause.getMessage(),
                                cause);
                        break;
                    }
                }

                if (success) {
                    batchFileWriter.flush(batchBuffer.toString());
                    consumer.commitSync();
                    for (LogEvent ev : telegramQueue) {
                        sendTelegramAsync(ev);
                    }
                }
            }
        } catch (WakeupException ignored) {
        } catch (IOException e) {
            log.error("Writer failure: {}", e.getMessage(), e);
        } finally {
            consumer.close();
        }
    }

    private void sendTelegramAsync(LogEvent event) {
        telegramAlertExecutor.submit(() -> notifier.send(alertFormatter.format(event)));
    }

    public void shutdown() {
        running = false;
        consumer.wakeup();
    }
}
