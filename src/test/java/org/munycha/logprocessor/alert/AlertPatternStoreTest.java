package org.munycha.logprocessor.alert;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AlertPatternStoreTest {

    private static final long WATCH_TIMEOUT_MS = 15_000;
    private static final long POLL_INTERVAL_MS = 100;

    @Test
    void loadsExistingPatternsFromFile(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("patterns.txt");
        Files.write(file, Arrays.asList("alpha", "beta", "gamma"), StandardCharsets.UTF_8);

        AlertPatternStore store = new AlertPatternStore(file);

        assertTrue(store.isKnown("alpha"));
        assertTrue(store.isKnown("beta"));
        assertTrue(store.isKnown("gamma"));
        assertFalse(store.isKnown("delta"));
    }

    @Test
    void addMakesPatternKnown(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("patterns.txt");
        Files.createFile(file);

        AlertPatternStore store = new AlertPatternStore(file);

        assertFalse(store.isKnown("X"));
        store.add("X");
        assertTrue(store.isKnown("X"));
    }

    @Test
    void addPersistsPatternToDisk(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("patterns.txt");
        Files.createFile(file);

        AlertPatternStore store = new AlertPatternStore(file);
        store.add("persistent-pattern");

        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        assertTrue(lines.contains("persistent-pattern"),
                "expected pattern persisted to disk, got: " + lines);
    }

    @Test
    void blankLinesInFileAreIgnored(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("patterns.txt");
        Files.write(file, Arrays.asList("foo", "", "  ", "bar"), StandardCharsets.UTF_8);

        AlertPatternStore store = new AlertPatternStore(file);

        assertTrue(store.isKnown("foo"));
        assertTrue(store.isKnown("bar"));
        assertFalse(store.isKnown(""));
    }

    @Test
    void externalFileEditTriggersReload(@TempDir Path tmp) throws IOException, InterruptedException {
        Path file = tmp.resolve("patterns.txt");
        Files.write(file, Arrays.asList("old-pattern"), StandardCharsets.UTF_8);

        AlertPatternStore store = new AlertPatternStore(file);
        assertTrue(store.isKnown("old-pattern"));

        // External edit: replace contents with a new pattern.
        Files.write(file, Arrays.asList("new-pattern"), StandardCharsets.UTF_8);

        assertTrue(waitUntil(() -> store.isKnown("new-pattern")),
                "WatchService never picked up external edit within " + WATCH_TIMEOUT_MS + "ms");
        assertFalse(store.isKnown("old-pattern"),
                "after reload, old pattern should be gone");
    }

    @Test
    void clearedFileResultsInEmptyStoreAfterReload(@TempDir Path tmp) throws IOException, InterruptedException {
        Path file = tmp.resolve("patterns.txt");
        Files.write(file, Arrays.asList("X", "Y", "Z"), StandardCharsets.UTF_8);

        AlertPatternStore store = new AlertPatternStore(file);
        assertTrue(store.isKnown("X"));

        // Truncate the file.
        Files.write(file, new byte[0]);

        assertTrue(waitUntil(() -> !store.isKnown("X") && !store.isKnown("Y") && !store.isKnown("Z")),
                "store should be empty after file truncation within " + WATCH_TIMEOUT_MS + "ms");
    }

    private boolean waitUntil(BooleanSupplier check) throws InterruptedException {
        long deadline = System.currentTimeMillis() + WATCH_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            if (check.getAsBoolean()) return true;
            Thread.sleep(POLL_INTERVAL_MS);
        }
        return check.getAsBoolean();
    }
}
