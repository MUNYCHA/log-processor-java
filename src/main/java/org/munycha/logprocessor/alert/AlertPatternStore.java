package org.munycha.logprocessor.alert;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class AlertPatternStore {

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
        System.out.println("[PatternStore] Loaded " + knownPatterns.size() +
                " patterns from " + patternFile);
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
                System.out.println("[PatternStore] Reloaded " + knownPatterns.size() +
                        " patterns from " + patternFile);
            } catch (IOException e) {
                System.err.println("[PatternStore] Reload failed, keeping previous patterns: " + e.getMessage());
            }
        }
    }

    public boolean isKnown(String pattern) {
        return knownPatterns.contains(pattern);
    }

    public void add(String pattern) {
        synchronized (lock) {
            knownPatterns.add(pattern);
        }

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
                System.err.println("[PatternStore] Failed to persist pattern: " + e.getMessage());
            }
        }

        if (knownPatterns.size() == WARN_THRESHOLD) {
            System.err.println("[PatternStore] WARN: " + WARN_THRESHOLD +
                    " patterns accumulated. Normalization may be missing a variable token type: " +
                    patternFile);
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
                        System.err.println("[PatternStore] FATAL: pattern file has no parent directory, cannot watch: " + patternFile);
                        break;
                    }

                    if (!Files.exists(dir)) {
                        System.err.println("[PatternStore] WARN: watched directory gone, waiting for it to return: " + dir);
                        if (waitForDirectory(dir)) {
                            System.out.println("[PatternStore] Directory returned, restarting watcher: " + dir);
                            // not a crash — don't increment crashCount
                        } else {
                            System.err.println("[PatternStore] FATAL: watched directory did not return after 2 minutes — pattern resets require app restart.");
                            break;
                        }
                    } else {
                        crashCount++;
                        if (crashCount > MAX_WATCHER_RESTARTS) {
                            System.err.println("[PatternStore] FATAL: watcher stopped after " +
                                    MAX_WATCHER_RESTARTS + " restarts — pattern resets require app restart. " +
                                    e.getMessage());
                        } else {
                            System.err.println("[PatternStore] WARN: watcher crashed (attempt " + crashCount +
                                    "/" + MAX_WATCHER_RESTARTS + "), restarting in 2s. " + e.getMessage());
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
            System.err.println("[PatternStore] WARN: still waiting for directory to return (" +
                    (i + 1) + "/" + DIR_POLL_MAX_ATTEMPTS + "): " + dir);
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
                        System.out.println("[PatternStore] File deleted — cleared all patterns");
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
