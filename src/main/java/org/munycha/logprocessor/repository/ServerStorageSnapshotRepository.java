package org.munycha.logprocessor.repository;

import org.munycha.logprocessor.config.DatabaseConfig;
import org.munycha.logprocessor.model.DiskUsage;
import org.munycha.logprocessor.model.ServerStorageSnapshot;

import java.sql.*;
import java.time.Instant;

public class ServerStorageSnapshotRepository {

    private final String url;
    private final String user;
    private final String password;
    private final String snapshotTable;
    private final String diskUsageTable;

    public ServerStorageSnapshotRepository(DatabaseConfig dbConfig) {
        this.url = dbConfig.getUrl();
        this.user = dbConfig.getUser();
        this.password = dbConfig.getPassword();
        this.snapshotTable = dbConfig.getTables().getServerStorageSnapshotTable();
        // Fix B: also store disk_usage table so both inserts share one transaction
        this.diskUsageTable = dbConfig.getTables().getMountPathStorageUsageTable();
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    /**
     * Fix B: saves the snapshot and all its disk usages atomically in a single
     * JDBC transaction. If any disk usage insert fails the entire operation is
     * rolled back — no orphaned snapshot rows left in the database.
     */
    public void saveSnapshotWithDiskUsages(ServerStorageSnapshot snapshot) throws SQLException {

        String snapshotSql =
                "INSERT INTO " + snapshotTable +
                        " (system_id, system_name, server_ip, server_name, collected_at)" +
                        " VALUES (?, ?, ?, ?, ?)";

        String diskUsageSql =
                "INSERT INTO " + diskUsageTable +
                        " (server_storage_snapshot_id, path, total_bytes, used_bytes, used_percent)" +
                        " VALUES (?, ?, ?, ?, ?)";

        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            try {
                long snapshotId;

                try (PreparedStatement stmt =
                             conn.prepareStatement(snapshotSql, Statement.RETURN_GENERATED_KEYS)) {

                    stmt.setString(1, snapshot.getSystemId());
                    stmt.setString(2, snapshot.getSystemName());
                    stmt.setString(3, snapshot.getServerIp());
                    stmt.setString(4, snapshot.getServerName());
                    stmt.setTimestamp(5, Timestamp.from(Instant.parse(snapshot.getTimestamp())));
                    stmt.executeUpdate();

                    try (ResultSet rs = stmt.getGeneratedKeys()) {
                        if (!rs.next()) {
                            throw new SQLException("Failed to retrieve snapshot ID");
                        }
                        snapshotId = rs.getLong(1);
                    }
                }

                try (PreparedStatement stmt = conn.prepareStatement(diskUsageSql)) {
                    for (DiskUsage diskUsage : snapshot.getDiskUsages()) {
                        stmt.setLong(1, snapshotId);
                        stmt.setString(2, diskUsage.getPath());
                        stmt.setLong(3, diskUsage.getTotalBytes());
                        stmt.setLong(4, diskUsage.getUsedBytes());
                        stmt.setDouble(5, diskUsage.getUsedPercent());
                        stmt.addBatch();
                    }
                    stmt.executeBatch();
                }

                conn.commit();

            } catch (Exception e) {
                conn.rollback();
                throw (e instanceof SQLException)
                        ? (SQLException) e
                        : new SQLException("Snapshot transaction failed", e);
            }
        }
    }
}
