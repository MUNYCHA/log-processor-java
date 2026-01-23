package org.munycha.kafkaconsumer.model;

import java.util.List;

public class ServerStorageSnapshot {
    private String systemId;
    private String systemName;
    private String serverName;
    private String serverIp;
    private String timestamp;
    private List<MountPathStorageUsage> mountPathStorageUsages;

    public ServerStorageSnapshot() {
    }

    public ServerStorageSnapshot(String systemId, String systemName, String serverName, String serverIp, String timestamp, List<MountPathStorageUsage> mountPathStorageUsages) {
        this.systemId = systemId;
        this.systemName = systemName;
        this.serverName = serverName;
        this.serverIp = serverIp;
        this.timestamp = timestamp;
        this.mountPathStorageUsages = mountPathStorageUsages;
    }

    public String getSystemId() {
        return systemId;
    }

    public void setSystemId(String systemId) {
        this.systemId = systemId;
    }

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public String getServerIp() {
        return serverIp;
    }

    public void setServerIp(String serverIp) {
        this.serverIp = serverIp;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public List<MountPathStorageUsage> getMountPathStorageUsages() {
        return mountPathStorageUsages;
    }

    public void setMountPathStorageUsages(List<MountPathStorageUsage> mountPathStorageUsages) {
        this.mountPathStorageUsages = mountPathStorageUsages;
    }
}

