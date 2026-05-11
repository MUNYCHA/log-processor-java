package org.munycha.logprocessor.alert;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.HashSet;
import java.util.Set;

public class AlertPatternStore {

    private static final int WARN_THRESHOLD = 10_000;

    private final Set<String> knownPatterns = new HashSet<>();
    private final Path patternFile;

    public AlertPatternStore(Path patternFile) throws IOException {
        this.patternFile = patternFile;
        load();
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

    public boolean isKnown(String pattern) {
        return knownPatterns.contains(pattern);
    }

    public void add(String pattern) {
        knownPatterns.add(pattern);

        // Open, append one line, close — no file handle held between writes
        try (BufferedWriter writer = Files.newBufferedWriter(
                patternFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        )) {
            writer.write(pattern);
            writer.newLine();
        } catch (IOException e) {
            // Pattern is in memory — dedup works this run
            // On restart this pattern won't reload — may fire once more
            System.err.println("[PatternStore] Failed to persist pattern: " + e.getMessage());
        }

        if (knownPatterns.size() == WARN_THRESHOLD) {
            System.err.println("[PatternStore] WARN: " + WARN_THRESHOLD +
                    " patterns accumulated. Normalization may be missing a variable token type: " +
                    patternFile);
        }
    }
}
