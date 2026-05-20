package org.munycha.logprocessor.log;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Golden-corpus driven tests for {@link LogMessageNormalizer}.
 *
 * The corpus lives in src/test/resources/normalizer/ as text files
 * named NNN-name.txt. Inside each file, non-blank non-comment lines
 * appear in pairs:
 *
 *   <input line>
 *   <expected normalized output>
 *
 * Adding a new source = adding a new file (or new pairs in an
 * existing file). The test framework discovers them dynamically so
 * there's no class-per-source boilerplate.
 *
 * Each pair becomes its own dynamic test, so a failure points at the
 * exact line, not the whole file.
 */
class LogMessageNormalizerCorpusTest {

    private static final String[] CORPUS_FILES = new String[] {
            "/normalizer/010-java-stack-traces.txt",
            "/normalizer/020-apache-nginx.txt",
            "/normalizer/030-json-and-keyvalue.txt",
            "/normalizer/040-syslog-and-paths.txt",
            "/normalizer/050-negative-cases.txt"
    };

    @TestFactory
    Iterable<DynamicTest> corpus() throws IOException {
        List<DynamicTest> tests = new ArrayList<>();
        for (String resource : CORPUS_FILES) {
            tests.addAll(loadFile(resource));
        }
        if (tests.isEmpty()) {
            throw new IllegalStateException("No corpus pairs loaded — check resource paths.");
        }
        return tests;
    }

    private List<DynamicTest> loadFile(String resource) throws IOException {
        List<String> lines = readResource(resource);
        List<DynamicTest> out = new ArrayList<>();

        String pendingInput = null;
        int pendingInputLineNum = -1;
        int lineNum = 0;

        for (String raw : lines) {
            lineNum++;
            String line = raw;
            String trimmed = line.trim();
            if (trimmed.isEmpty() || trimmed.startsWith("#")) continue;

            if (pendingInput == null) {
                pendingInput = line;
                pendingInputLineNum = lineNum;
            } else {
                final String input = pendingInput;
                final String expected = line;
                final int inputLine = pendingInputLineNum;
                String testName = resource + ":" + inputLine + " -> " + truncate(input, 40);
                out.add(DynamicTest.dynamicTest(testName, () -> {
                    String actual = LogMessageNormalizer.normalize(input);
                    assertEquals(expected, actual,
                            "Normalization mismatch.\n" +
                            "  Source:   " + resource + " line " + inputLine + "\n" +
                            "  Input:    " + input + "\n" +
                            "  Expected: " + expected + "\n" +
                            "  Actual:   " + actual);
                }));
                pendingInput = null;
            }
        }

        if (pendingInput != null) {
            throw new IllegalStateException(
                    "Unpaired input at " + resource + ":" + pendingInputLineNum +
                            " — every input line needs a following expected line.");
        }

        return out;
    }

    private static List<String> readResource(String resource) throws IOException {
        try (InputStream in = LogMessageNormalizerCorpusTest.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("Corpus resource not found: " + resource);
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                List<String> out = new ArrayList<>();
                String line;
                while ((line = reader.readLine()) != null) {
                    out.add(line);
                }
                return out;
            }
        }
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max - 1) + "…";
    }
}
