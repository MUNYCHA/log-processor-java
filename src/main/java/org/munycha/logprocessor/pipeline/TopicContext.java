package org.munycha.logprocessor.pipeline;

/**
 * Per-topic wiring assembled at bootstrap and handed to the poll loop:
 * what topic to subscribe to, how to process each record, and where to
 * flush the batch buffer.
 *
 * Shared infrastructure (Kafka factory, notifier, executors) is passed
 * separately so a single instance can be reused across topics.
 */
public final class TopicContext {

    public final String topic;
    public final RecordHandler handler;
    public final BatchFileWriter batchFileWriter;

    public TopicContext(String topic, RecordHandler handler, BatchFileWriter batchFileWriter) {
        this.topic = topic;
        this.handler = handler;
        this.batchFileWriter = batchFileWriter;
    }
}
