package org.munycha.kafkaconsumer.config;

public class TableConfig {
    private String alertLogTable;
    private String serverStorageSnapshotTable;
    private String mountPathStorageUsageTable;

    public TableConfig() {
    }

    public String getAlertLogTable() {
        return alertLogTable;
    }

    public void setAlertLogTable(String alertLogTable) {
        this.alertLogTable = alertLogTable;
    }

    public String getServerStorageSnapshotTable() {
        return serverStorageSnapshotTable;
    }

    public void setServerStorageSnapshotTable(String serverStorageSnapshotTable) {
        this.serverStorageSnapshotTable = serverStorageSnapshotTable;
    }

    public String getMountPathStorageUsageTable() {
        return mountPathStorageUsageTable;
    }

    public void setMountPathStorageUsageTable(String mountPathStorageUsageTable) {
        this.mountPathStorageUsageTable = mountPathStorageUsageTable;
    }
}
