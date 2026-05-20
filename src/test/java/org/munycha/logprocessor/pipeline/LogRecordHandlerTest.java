package org.munycha.logprocessor.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.munycha.logprocessor.config.DatabaseConfig;
import org.munycha.logprocessor.config.TableConfig;
import org.munycha.logprocessor.log.AlertDetector;
import org.munycha.logprocessor.log.AlertPatternStore;
import org.munycha.logprocessor.log.LogEvent;
import org.munycha.logprocessor.log.LogMessageNormalizer;
import org.munycha.logprocessor.repository.AlertRepository;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogRecordHandlerTest {

    private static final long WATCHER_REGISTER_DELAY_MS = 500;

    private static final String ALERT_JSON =
            "{\"serverName\":\"host1\",\"path\":\"/var/log/x\",\"topic\":\"t\"," +
            "\"timestamp\":\"2026-05-20T10:00:00Z\",\"message\":\"FATAL boom\"}";

    private static final String NORMAL_JSON =
            "{\"serverName\":\"host1\",\"path\":\"/var/log/x\",\"topic\":\"t\"," +
            "\"timestamp\":\"2026-05-20T10:00:00Z\",\"message\":\"all good\"}";

    @Test
    void nonAlertWritesToBufferAndReturnsEmpty() {
        StubAlertRepository repo = newRepo();
        LogRecordHandler handler = new LogRecordHandler(
                new ObjectMapper(),
                new AlertDetector(Arrays.asList("FATAL")),
                new LogMessageNormalizer(),
                null,
                repo);
        StringWriter buffer = new StringWriter();

        Optional<LogEvent> result = handler.handle(record(NORMAL_JSON), buffer);

        assertFalse(result.isPresent());
        assertTrue(buffer.toString().contains("all good"));
        assertEquals(0, repo.saveCount.get());
    }

    @Test
    void alertWithoutPatternStoreReturnsEvent() {
        StubAlertRepository repo = newRepo();
        LogRecordHandler handler = new LogRecordHandler(
                new ObjectMapper(),
                new AlertDetector(Arrays.asList("FATAL")),
                new LogMessageNormalizer(),
                null,
                repo);
        StringWriter buffer = new StringWriter();

        Optional<LogEvent> result = handler.handle(record(ALERT_JSON), buffer);

        assertTrue(result.isPresent());
        assertEquals("FATAL boom", result.get().getMessage());
        assertEquals(1, repo.saveCount.get());
    }

    @Test
    void alertWithKnownPatternIsSuppressed(@TempDir Path tmp) throws IOException, InterruptedException {
        Path file = tmp.resolve("patterns.txt");
        Files.createFile(file);
        AlertPatternStore store = new AlertPatternStore(file);
        Thread.sleep(WATCHER_REGISTER_DELAY_MS);

        StubAlertRepository repo = newRepo();
        LogRecordHandler handler = new LogRecordHandler(
                new ObjectMapper(),
                new AlertDetector(Arrays.asList("FATAL")),
                new LogMessageNormalizer(),
                store,
                repo);
        StringWriter buffer = new StringWriter();

        // First time: fires alert, pattern remembered.
        Optional<LogEvent> first = handler.handle(record(ALERT_JSON), buffer);
        assertTrue(first.isPresent());
        assertEquals(1, repo.saveCount.get());

        // Second identical: suppressed.
        Optional<LogEvent> second = handler.handle(record(ALERT_JSON), buffer);
        assertFalse(second.isPresent());
        assertEquals(1, repo.saveCount.get(), "DB save should not be called again");
    }

    @Test
    void firstAlertWithPatternStoreSavesAndRemembers(@TempDir Path tmp) throws IOException, InterruptedException {
        Path file = tmp.resolve("patterns.txt");
        Files.createFile(file);
        AlertPatternStore store = new AlertPatternStore(file);
        Thread.sleep(WATCHER_REGISTER_DELAY_MS);

        StubAlertRepository repo = newRepo();
        LogMessageNormalizer normalizer = new LogMessageNormalizer();
        LogRecordHandler handler = new LogRecordHandler(
                new ObjectMapper(),
                new AlertDetector(Arrays.asList("FATAL")),
                normalizer,
                store,
                repo);

        Optional<LogEvent> result = handler.handle(record(ALERT_JSON), new StringWriter());

        assertTrue(result.isPresent());
        assertTrue(store.isKnown(normalizer.normalizeMessage("FATAL boom")));
    }

    private static ConsumerRecord<String, String> record(String value) {
        return new ConsumerRecord<>("t", 0, 0L, null, value);
    }

    private static StubAlertRepository newRepo() {
        TableConfig tables = new TableConfig();
        tables.setAlertLogTable("alert_logs");
        DatabaseConfig db = new DatabaseConfig("jdbc:none", "u", "p", tables);
        return new StubAlertRepository(db);
    }

    /** Counts saveAlert calls without touching a real database. */
    private static class StubAlertRepository extends AlertRepository {
        final AtomicInteger saveCount = new AtomicInteger();

        StubAlertRepository(DatabaseConfig db) { super(db); }

        @Override
        public void saveAlert(String topic, String timestamp, String serverName,
                              String filePath, String message) {
            saveCount.incrementAndGet();
        }
    }
}
