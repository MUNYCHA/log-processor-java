package org.munycha.logprocessor.config;

import java.util.List;

public class AppConfig {

    private String bootstrapServers;
    private String telegramBotToken;
    private String telegramChatId;

    private List<TopicConfig> topics;

    private DatabaseConfig database;

    public AppConfig() {}

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTelegramBotToken() {
        return telegramBotToken;
    }

    public String getTelegramChatId() {
        return telegramChatId;
    }

    public List<TopicConfig> getTopics() {
        return topics;
    }

    public DatabaseConfig getDatabase() {
        return database;
    }
}