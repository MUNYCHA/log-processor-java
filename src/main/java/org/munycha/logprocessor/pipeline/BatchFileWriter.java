package org.munycha.logprocessor.pipeline;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Append-only batch flush to a UTF-8 text file.
 *
 * The file handle is opened and closed per flush call, so external log
 * rotation scripts can truncate or replace the file between flushes
 * without the JVM holding a stale descriptor.
 */
public class BatchFileWriter {

    private final Path outputFile;

    public BatchFileWriter(Path outputFile) {
        this.outputFile = outputFile;
    }

    public void flush(String content) throws IOException {
        if (content == null || content.isEmpty()) return;

        try (BufferedWriter writer = Files.newBufferedWriter(
                outputFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        )) {
            writer.write(content);
        }
    }
}
