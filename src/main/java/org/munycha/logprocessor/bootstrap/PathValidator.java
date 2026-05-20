package org.munycha.logprocessor.bootstrap;

import org.munycha.logprocessor.config.TopicConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Fails fast at startup if a topic's output file or pattern file is
 * missing or not writable, so misconfiguration surfaces before the
 * Kafka consumer starts.
 */
public final class PathValidator {

    private PathValidator() {}

    public static void validate(TopicConfig t) throws IOException {
        Path outputPath = Paths.get(t.getOutput());
        if (!Files.isRegularFile(outputPath) || !Files.isWritable(outputPath)) {
            throw new IOException(
                    "Output file does not exist or is not writable: " + t.getOutput());
        }

        if (t.hasPatternStore()) {
            Path patternPath = Paths.get(t.getPatternStoreFile());
            if (!Files.isRegularFile(patternPath) || !Files.isWritable(patternPath)) {
                throw new IOException(
                        "Pattern store file does not exist or is not writable: "
                                + t.getPatternStoreFile());
            }
        }
    }
}
