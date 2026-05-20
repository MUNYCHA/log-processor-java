package org.munycha.logprocessor.pipeline;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BatchFileWriterTest {

    @Test
    void appendsToExistingFile(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("out.log");
        Files.write(file, "existing\n".getBytes(StandardCharsets.UTF_8));

        new BatchFileWriter(file).flush("new line\n");

        assertEquals("existing\nnew line\n",
                new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
    }

    @Test
    void emptyContentIsNoop(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("out.log");
        Files.write(file, "original\n".getBytes(StandardCharsets.UTF_8));

        new BatchFileWriter(file).flush("");

        assertEquals("original\n",
                new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
    }

    @Test
    void nullContentIsNoop(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("out.log");
        Files.write(file, "keep\n".getBytes(StandardCharsets.UTF_8));

        new BatchFileWriter(file).flush(null);

        assertEquals("keep\n",
                new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
    }

    @Test
    void writesUtf8(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("out.log");
        Files.createFile(file);

        new BatchFileWriter(file).flush("héllo ✓\n");

        assertArrayEquals(
                "héllo ✓\n".getBytes(StandardCharsets.UTF_8),
                Files.readAllBytes(file));
    }

    @Test
    void multipleFlushesAppendInOrder(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("out.log");
        Files.createFile(file);

        BatchFileWriter w = new BatchFileWriter(file);
        w.flush("a\n");
        w.flush("b\n");
        w.flush("c\n");

        assertEquals("a\nb\nc\n",
                new String(Files.readAllBytes(file), StandardCharsets.UTF_8));
    }
}
