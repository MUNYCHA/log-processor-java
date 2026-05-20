package org.munycha.logprocessor.pipeline;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.munycha.logprocessor.log.LogEvent;

import java.io.Writer;
import java.util.Optional;

/**
 * Processes one Kafka record from a topic. Implementations decide what to
 * do with the payload (write to file, save to DB, emit alerts, etc.).
 *
 * The returned Optional is the poll-loop's "do you want this surfaced as
 * a Telegram alert?" signal. Present = enqueue the event for async
 * Telegram delivery. Empty = handler is done; the loop should move on.
 *
 * Failures must be raised as RuntimeException so the poll loop's
 * retry-on-failure logic can roll back the batch and re-poll.
 */
public interface RecordHandler {

    Optional<LogEvent> handle(ConsumerRecord<String, String> record, Writer batchBuffer);
}
