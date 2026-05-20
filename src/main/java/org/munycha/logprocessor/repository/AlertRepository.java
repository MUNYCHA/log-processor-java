package org.munycha.logprocessor.repository;

import org.munycha.logprocessor.config.DatabaseConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;

public class AlertRepository {

    private static final Logger log = LoggerFactory.getLogger(AlertRepository.class);

    private final String url;
    private final String user;
    private final String password;
    private final String table;

    public AlertRepository(DatabaseConfig dbConfig) {
        this.url = dbConfig.getUrl();
        this.user = dbConfig.getUser();
        this.password = dbConfig.getPassword();
        this.table = dbConfig.getTables().getAlertLogTable();
    }

    private Connection getConnection() throws Exception {
        return DriverManager.getConnection(url, user, password);
    }

    public void saveAlert(
            String topic,
            String timestamp,
            String serverName,
            String filePath,
            String message
    ) {
        String sql =
                "INSERT INTO " + table +
                        " (topic, server_name, file_path, event_timestamp, message) " +
                        "VALUES (?, ?, ?, ?, ?)";

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, topic);
            stmt.setString(2, serverName);
            stmt.setString(3, filePath);

            stmt.setTimestamp(
                    4,
                    Timestamp.from(Instant.parse(timestamp))
            );

            stmt.setString(5, message);

            stmt.executeUpdate();

        } catch (Exception e) {
            log.error("Failed to save alert: {}", e.getMessage(), e);
            throw new RuntimeException("Alert DB save failed", e);
        }
    }
}
