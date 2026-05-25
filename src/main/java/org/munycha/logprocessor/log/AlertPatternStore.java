package org.munycha.logprocessor.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class AlertPatternStore {

    private static final Logger log = LoggerFactory.getLogger(AlertPatternStore.class);

    private static final int WARN_THRESHOLD = 10_000;
    private static final int MAX_WATCHER_RESTARTS = 5;
    private static final int DIR_POLL_INTERVAL_MS = 5_000;
    private static final int DIR_POLL_MAX_ATTEMPTS = 24; // 24 × 5s = 2 minutes

    private volatile Set<String> knownPatterns = ConcurrentHashMap.newKeySet();
    private final Object lock = new Object();
    private final Path patternFile;

    public AlertPatternStore(Path patternFile) throws IOException {
        this.patternFile = patternFile;
        load();
        startWatcher();
    }

    private void load() throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(patternFile, StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty()) {
                    knownPatterns.add(trimmed);
                }
            }
        }
        log.info("Loaded {} patterns from {}", knownPatterns.size(), patternFile);
    }

    private void reload() {
        synchronized (lock) {
            if (!Files.exists(patternFile)) {
                knownPatterns = ConcurrentHashMap.newKeySet();
                return;
            }
            Set<String> next = ConcurrentHashMap.newKeySet();
            try (BufferedReader reader = Files.newBufferedReader(patternFile, StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String trimmed = line.trim();
                    if (!trimmed.isEmpty()) {
                        next.add(trimmed);
                    }
                }
                knownPatterns = next;
                log.info("Reloaded {} patterns from {}", knownPatterns.size(), patternFile);
            } catch (IOException e) {
                log.warn("Reload failed, keeping previous patterns: {}", e.getMessage());
            }
        }
    }

    public boolean isKnown(String pattern) {
        return knownPatterns.contains(pattern);
    }

    public void add(String pattern) {
        synchronized (lock) {
            knownPatterns.add(pattern);

            if (Files.exists(patternFile)) {
                try (BufferedWriter writer = Files.newBufferedWriter(
                        patternFile,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.APPEND
                )) {
                    writer.write(pattern);
                    writer.newLine();
                } catch (IOException e) {
                    log.warn("Failed to persist pattern: {}", e.getMessage());
                }
            }
        }

        if (knownPatterns.size() == WARN_THRESHOLD) {
            log.warn("{} patterns accumulated. Normalization may be missing a variable token type: {}",
                    WARN_THRESHOLD, patternFile);
        }
    }

    private void startWatcher() {
        Thread t = new Thread(() -> {
            int crashCount = 0;
            while (crashCount <= MAX_WATCHER_RESTARTS) {
                try {
                    runWatchLoop();
                    break;
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    Path dir = patternFile.getParent();

                    if (dir == null) {
                        log.error("FATAL: pattern file has no parent directory, cannot watch: {}", patternFile);
                        break;
                    }

                    if (!Files.exists(dir)) {
                        log.warn("Watched directory gone, waiting for it to return: {}", dir);
                        if (waitForDirectory(dir)) {
                            log.info("Directory returned, restarting watcher: {}", dir);
                            // not a crash — don't increment crashCount
                        } else {
                            log.error("FATAL: watched directory did not return after 2 minutes — pattern resets require app restart.");
                            break;
                        }
                    } else {
                        crashCount++;
                        if (crashCount > MAX_WATCHER_RESTARTS) {
                            log.error("FATAL: watcher stopped after {} restarts — pattern resets require app restart.",
                                    MAX_WATCHER_RESTARTS, e);
                        } else {
                            log.warn("Watcher crashed (attempt {}/{}), restarting in 2s: {}",
                                    crashCount, MAX_WATCHER_RESTARTS, e.getMessage());
                            try {
                                Thread.sleep(2000);
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                break;
                            }
                        }
                    }
                }
            }
        });
        t.setDaemon(true);
        t.setName("pattern-store-watcher");
        t.start();
    }

    private boolean waitForDirectory(Path dir) {
        for (int i = 0; i < DIR_POLL_MAX_ATTEMPTS; i++) {
            try {
                Thread.sleep(DIR_POLL_INTERVAL_MS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
            if (Files.exists(dir)) return true;
            log.warn("Still waiting for directory to return ({}/{}): {}",
                    i + 1, DIR_POLL_MAX_ATTEMPTS, dir);
        }
        return false;
    }

    private void runWatchLoop() throws IOException, InterruptedException {
        Path dir = patternFile.getParent();
        try (WatchService ws = FileSystems.getDefault().newWatchService()) {
            dir.register(ws,
                    StandardWatchEventKinds.ENTRY_MODIFY,
                    StandardWatchEventKinds.ENTRY_DELETE,
                    StandardWatchEventKinds.ENTRY_CREATE);

            while (!Thread.currentThread().isInterrupted()) {
                WatchKey key = ws.take();
                for (WatchEvent<?> event : key.pollEvents()) {
                    Path changed = (Path) event.context();
                    if (!patternFile.getFileName().equals(changed)) continue;

                    if (event.kind() == StandardWatchEventKinds.ENTRY_DELETE) {
                        synchronized (lock) {
                            knownPatterns = ConcurrentHashMap.newKeySet();
                        }
                        log.info("File deleted — cleared all patterns");
                    } else {
                        reload();
                    }
                }
                if (!key.reset()) {
                    throw new IOException("Watch key invalidated — parent directory removed");
                }
            }
        }
    }
}
