package org.munycha.logprocessor.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ServerStorageSnapshot {

    private String systemId;
    private String systemName;
    private String serverName;
    private String serverIp;
    private String timestamp;
    @JsonProperty("mountPathStorageUsages")
    private List<DiskUsage> diskUsages;

    public ServerStorageSnapshot() {
    }

    public ServerStorageSnapshot(String systemId, String systemName, String serverName, String serverIp,
                                 String timestamp, List<DiskUsage> diskUsages) {
        this.systemId = systemId;
        this.systemName = systemName;
        this.serverName = serverName;
        this.serverIp = serverIp;
        this.timestamp = timestamp;
        this.diskUsages = diskUsages;
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

    public List<DiskUsage> getDiskUsages() {
        return diskUsages;
    }

    public void setDiskUsages(List<DiskUsage> diskUsages) {
        this.diskUsages = diskUsages;
    }
}
