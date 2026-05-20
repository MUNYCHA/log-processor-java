package org.munycha.logprocessor.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigLoaderTest {

    private static final String SAMPLE_JSON =
            "{\n" +
            "  \"bootstrapServers\": \"localhost:9092\",\n" +
            "  \"telegramBotToken\": \"token-abc\",\n" +
            "  \"telegramChatId\": \"chat-xyz\",\n" +
            "  \"topics\": [\n" +
            "    {\n" +
            "      \"topic\": \"app-logs\",\n" +
            "      \"type\": \"LOG\",\n" +
            "      \"output\": \"/tmp/app.log\",\n" +
            "      \"alertKeywords\": [\"FATAL\", \"PANIC\"],\n" +
            "      \"patternStoreFile\": \"/tmp/patterns.txt\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"topic\": \"metrics\",\n" +
            "      \"type\": \"METRIC\",\n" +
            "      \"output\": \"/tmp/metrics.json\"\n" +
            "    }\n" +
            "  ],\n" +
            "  \"database\": {\n" +
            "    \"url\": \"jdbc:mysql://localhost:3306/db\",\n" +
            "    \"user\": \"u\",\n" +
            "    \"password\": \"p\",\n" +
            "    \"tables\": {\n" +
            "      \"alertLogTable\": \"alert_logs\",\n" +
            "      \"serverStorageSnapshotTable\": \"snap\",\n" +
            "      \"mountPathStorageUsageTable\": \"usage\"\n" +
            "    }\n" +
            "  }\n" +
            "}\n";

    @Test
    void loadsConfigFromExternalFile(@TempDir Path tmp) throws IOException {
        Path file = tmp.resolve("config.json");
        Files.write(file, SAMPLE_JSON.getBytes(StandardCharsets.UTF_8));

        AppConfig config = new ConfigLoader(file.toString()).load();

        assertEquals("localhost:9092", config.getBootstrapServers());
        assertEquals("token-abc", config.getTelegramBotToken());
        assertEquals("chat-xyz", config.getTelegramChatId());

        assertNotNull(config.getTopics());
        assertEquals(2, config.getTopics().size());

        TopicConfig first = config.getTopics().get(0);
        assertEquals("app-logs", first.getTopic());
        assertEquals(TopicType.LOG, first.getType());
        assertTrue(first.hasAlertKeywords());
        assertTrue(first.hasPatternStore());

        TopicConfig second = config.getTopics().get(1);
        assertEquals(TopicType.METRIC, second.getType());

        assertNotNull(config.getDatabase());
        assertEquals("jdbc:mysql://localhost:3306/db", config.getDatabase().getUrl());
    }

    @Test
    void fallsBackToClasspathWhenExternalFileMissing() throws IOException {
        // The repo ships config/consumer_config.json on the classpath.
        AppConfig config = new ConfigLoader("config/consumer_config.json").load();

        assertNotNull(config);
        assertNotNull(config.getTopics());
    }

    @Test
    void throwsWhenConfigMissingFromBothPlaces() {
        assertThrows(FileNotFoundException.class,
                () -> new ConfigLoader("definitely/does/not/exist.json").load());
    }
}
