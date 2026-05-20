package org.munycha.logprocessor.repository;

import org.munycha.logprocessor.config.DatabaseConfig;
import org.munycha.logprocessor.metric.DiskUsage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DiskUsageRepository {

    private final String url;
    private final String user;
    private final String password;
    private final String table;

    public DiskUsageRepository(DatabaseConfig dbConfig) {
        this.url = dbConfig.getUrl();
        this.user = dbConfig.getUser();
        this.password = dbConfig.getPassword();
        this.table = dbConfig.getTables().getMountPathStorageUsageTable();
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public void savePath(long snapshotId, DiskUsage usage) throws SQLException {

        String sql =
                "INSERT INTO " + table +
                        " (server_storage_snapshot_id, path, total_bytes, used_bytes, used_percent) " +
                        "VALUES (?, ?, ?, ?, ?)";

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setLong(1, snapshotId);
            stmt.setString(2, usage.getPath());
            stmt.setLong(3, usage.getTotalBytes());
            stmt.setLong(4, usage.getUsedBytes());
            stmt.setDouble(5, usage.getUsedPercent());

            stmt.executeUpdate();
        }
    }
}
