package org.munycha.kafkaconsumer.db;

import org.munycha.kafkaconsumer.config.DatabaseConfig;
import org.munycha.kafkaconsumer.model.MountPathStorageUsage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MountPathStorageUsageDB {

    private final String url;
    private final String user;
    private final String password;
    private final String table;

    public MountPathStorageUsageDB(DatabaseConfig dbConfig) {
        this.url = dbConfig.getUrl();
        this.user = dbConfig.getUser();
        this.password = dbConfig.getPassword();
        this.table = dbConfig.getTables().getMountPathStorageUsageTable();
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public void savePath(long serverStorageSnapshotId, MountPathStorageUsage mountPathStorageUsage) throws SQLException {

        String sql =
                "INSERT INTO " + table +
                        " (server_storage_snapshot_id, path, total_bytes, used_bytes, used_percent) " +
                        "VALUES (?, ?, ?, ?, ?)";

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setLong(1, serverStorageSnapshotId);
            stmt.setString(2, mountPathStorageUsage.getPath());
            stmt.setLong(3, mountPathStorageUsage.getTotalBytes());
            stmt.setLong(4, mountPathStorageUsage.getUsedBytes());
            stmt.setDouble(5, mountPathStorageUsage.getUsedPercent());

            stmt.executeUpdate();
        }
    }
}
