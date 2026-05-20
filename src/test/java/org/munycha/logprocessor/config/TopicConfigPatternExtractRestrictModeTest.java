package org.munycha.logprocessor.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.munycha.logprocessor.log.PatternExtractRestrictMode;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TopicConfigPatternExtractRestrictModeTest {

    private static final String CONFIG_TEMPLATE =
            "{\n" +
            "  \"bootstrapServers\": \"localhost:9092\",\n" +
            "  \"telegramBotToken\": \"t\",\n" +
            "  \"telegramChatId\": \"c\",\n" +
            "  \"topics\": [\n" +
            "    {\n" +
            "      \"topic\": \"x\",\n" +
            "      \"type\": \"LOG\",\n" +
            "      \"output\": \"/tmp/x.log\"%s\n" +
            "    }\n" +
            "  ],\n" +
            "  \"database\": {\n" +
            "    \"url\": \"jdbc:mysql://localhost/db\",\n" +
            "    \"user\": \"u\",\n" +
            "    \"password\": \"p\",\n" +
            "    \"tables\": {\n" +
            "      \"alertLogTable\": \"a\",\n" +
            "      \"serverStorageSnapshotTable\": \"s\",\n" +
            "      \"mountPathStorageUsageTable\": \"m\"\n" +
            "    }\n" +
            "  }\n" +
            "}\n";

    private TopicConfig firstTopicFrom(Path tmp, String fieldFragment) throws IOException {
        String json = String.format(CONFIG_TEMPLATE, fieldFragment);
        Path file = tmp.resolve("config.json");
        Files.write(file, json.getBytes(StandardCharsets.UTF_8));
        AppConfig cfg = new ConfigLoader(file.toString()).load();
        return cfg.getTopics().get(0);
    }

    @Test
    void absentFieldParsesAsHigh(@TempDir Path tmp) throws IOException {
        TopicConfig t = firstTopicFrom(tmp, "");
        assertEquals(PatternExtractRestrictMode.HIGH,
                PatternExtractRestrictMode.parse(t.getPatternExtractRestrictMode()));
    }

    @Test
    void emptyStringParsesAsHigh(@TempDir Path tmp) throws IOException {
        TopicConfig t = firstTopicFrom(tmp, ",\n      \"patternExtractRestrictMode\": \"\"");
        assertEquals(PatternExtractRestrictMode.HIGH,
                PatternExtractRestrictMode.parse(t.getPatternExtractRestrictMode()));
    }

    @Test
    void unknownStringFallsBackToHigh(@TempDir Path tmp) throws IOException {
        TopicConfig t = firstTopicFrom(tmp, ",\n      \"patternExtractRestrictMode\": \"reallyhigh\"");
        assertEquals(PatternExtractRestrictMode.HIGH,
                PatternExtractRestrictMode.parse(t.getPatternExtractRestrictMode()));
    }

    @Test
    void mediumRoundTrips(@TempDir Path tmp) throws IOException {
        TopicConfig t = firstTopicFrom(tmp, ",\n      \"patternExtractRestrictMode\": \"medium\"");
        assertEquals(PatternExtractRestrictMode.MEDIUM,
                PatternExtractRestrictMode.parse(t.getPatternExtractRestrictMode()));
    }

    @Test
    void lowRoundTrips(@TempDir Path tmp) throws IOException {
        TopicConfig t = firstTopicFrom(tmp, ",\n      \"patternExtractRestrictMode\": \"LOW\"");
        assertEquals(PatternExtractRestrictMode.LOW,
                PatternExtractRestrictMode.parse(t.getPatternExtractRestrictMode()));
    }
}
