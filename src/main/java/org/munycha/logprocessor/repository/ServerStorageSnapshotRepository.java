package org.munycha.logprocessor.repository;

import org.munycha.logprocessor.config.DatabaseConfig;
import org.munycha.logprocessor.model.ServerStorageSnapshot;

import java.sql.*;
import java.time.Instant;

public class ServerStorageSnapshotRepository {

    private final String url;
    private final String user;
    private final String password;
    private final String table;

    public ServerStorageSnapshotRepository(DatabaseConfig dbConfig) {
        this.url = dbConfig.getUrl();
        this.user = dbConfig.getUser();
        this.password = dbConfig.getPassword();
        this.table = dbConfig.getTables().getServerStorageSnapshotTable();
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public long saveSnapshot(ServerStorageSnapshot snapshot) throws SQLException {

        String sql =
                "INSERT INTO " + table +
                        " (system_id, system_name, server_ip, server_name, collected_at) VALUES (?, ?, ?, ?, ?)";

        try (Connection conn = getConnection();
             PreparedStatement stmt =
                     conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {

            stmt.setString(1, snapshot.getSystemId());
            stmt.setString(2, snapshot.getSystemName());
            stmt.setString(3, snapshot.getServerIp());
            stmt.setString(4, snapshot.getServerName());

            stmt.setTimestamp(
                    5,
                    Timestamp.from(Instant.parse(snapshot.getTimestamp()))
            );

            stmt.executeUpdate();

            try (ResultSet rs = stmt.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }

        throw new SQLException("Failed to retrieve snapshot ID");
    }
}
