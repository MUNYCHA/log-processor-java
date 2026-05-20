package org.munycha.logprocessor.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.munycha.logprocessor.config.DatabaseConfig;
import org.munycha.logprocessor.config.TableConfig;
import org.munycha.logprocessor.log.LogEvent;
import org.munycha.logprocessor.metric.ServerStorageSnapshot;
import org.munycha.logprocessor.repository.ServerStorageSnapshotRepository;

import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MetricRecordHandlerTest {

    private static final String SNAPSHOT_JSON =
            "{\"systemId\":\"sys1\",\"systemName\":\"prod\",\"serverName\":\"host1\"," +
            "\"serverIp\":\"10.0.0.1\",\"timestamp\":\"2026-05-20T10:00:00Z\"," +
            "\"diskUsages\":[{\"path\":\"/\",\"totalBytes\":100,\"usedBytes\":50,\"usedPercent\":50.0}]}";

    @Test
    void handlePersistsSnapshotAndAlwaysReturnsEmpty() {
        StubSnapshotRepository repo = newRepo();
        MetricRecordHandler handler = new MetricRecordHandler(new ObjectMapper(), repo);
        StringWriter buffer = new StringWriter();

        Optional<LogEvent> result = handler.handle(record(SNAPSHOT_JSON), buffer);

        assertFalse(result.isPresent());
        assertNotNull(repo.lastSaved.get());
        assertEquals("sys1", repo.lastSaved.get().getSystemId());
        assertTrue(buffer.toString().contains("\"systemId\""),
                "expected pretty-printed JSON in batch buffer");
    }

    @Test
    void repositoryFailureBubblesAsRuntimeException() {
        ServerStorageSnapshotRepository failing = new ServerStorageSnapshotRepository(newDbConfig()) {
            @Override
            public void saveSnapshotWithDiskUsages(ServerStorageSnapshot snapshot) throws SQLException {
                throw new SQLException("simulated DB outage");
            }
        };
        MetricRecordHandler handler = new MetricRecordHandler(new ObjectMapper(), failing);

        RuntimeException ex = assertThrows(RuntimeException.class,
                () -> handler.handle(record(SNAPSHOT_JSON), new StringWriter()));
        assertEquals("Metric DB save failed", ex.getMessage());
    }

    private static ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>("metric", 0, 0L, null, value);
    }

    private static DatabaseConfig newDbConfig() {
        TableConfig tables = new TableConfig();
        tables.setServerStorageSnapshotTable("snap");
        tables.setMountPathStorageUsageTable("usage");
        return new DatabaseConfig("jdbc:none", "u", "p", tables);
    }

    private static StubSnapshotRepository newRepo() {
        return new StubSnapshotRepository(newDbConfig());
    }

    /** Captures the snapshot without touching a real database. */
    private static class StubSnapshotRepository extends ServerStorageSnapshotRepository {
        final AtomicReference<ServerStorageSnapshot> lastSaved = new AtomicReference<>();

        StubSnapshotRepository(DatabaseConfig db) { super(db); }

        @Override
        public void saveSnapshotWithDiskUsages(ServerStorageSnapshot snapshot) {
            lastSaved.set(snapshot);
        }
    }
}
