package org.munycha.logprocessor.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.munycha.logprocessor.log.LogEvent;
import org.munycha.logprocessor.metric.ServerStorageSnapshot;
import org.munycha.logprocessor.repository.ServerStorageSnapshotRepository;

import java.io.Writer;
import java.util.Optional;

/**
 * Per-record handler for METRIC-type topics. Pretty-prints the snapshot
 * JSON into the batch buffer and persists it (with its disk usages) in
 * one transaction. Never produces a Telegram alert.
 */
public class MetricRecordHandler implements RecordHandler {

    private final ObjectMapper mapper;
    private final ObjectWriter prettyWriter;
    private final ServerStorageSnapshotRepository snapshotRepository;

    public MetricRecordHandler(ObjectMapper mapper,
                               ServerStorageSnapshotRepository snapshotRepository) {
        this.mapper = mapper;
        this.prettyWriter = mapper.writerWithDefaultPrettyPrinter();
        this.snapshotRepository = snapshotRepository;
    }

    @Override
    public Optional<LogEvent> handle(ConsumerRecord<String, String> record, Writer batchBuffer) {
        try {
            ServerStorageSnapshot snapshot =
                    mapper.readValue(record.value(), ServerStorageSnapshot.class);

            batchBuffer.write(prettyWriter.writeValueAsString(snapshot));
            batchBuffer.write(System.lineSeparator());

            snapshotRepository.saveSnapshotWithDiskUsages(snapshot);
            return Optional.empty();

        } catch (Exception e) {
            throw new RuntimeException("Metric DB save failed", e);
        }
    }
}
