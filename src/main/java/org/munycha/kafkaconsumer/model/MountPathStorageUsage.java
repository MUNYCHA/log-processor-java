package org.munycha.kafkaconsumer.model;

public class MountPathStorageUsage {
    private String path;
    private long totalBytes;
    private long usedBytes;
    private double usedPercent;

    public MountPathStorageUsage() {
    }

    public MountPathStorageUsage(String path, long totalBytes, long usedBytes, double usedPercent) {
        this.path = path;
        this.totalBytes = totalBytes;
        this.usedBytes = usedBytes;
        this.usedPercent = usedPercent;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(long totalBytes) {
        this.totalBytes = totalBytes;
    }

    public long getUsedBytes() {
        return usedBytes;
    }

    public void setUsedBytes(long usedBytes) {
        this.usedBytes = usedBytes;
    }

    public double getUsedPercent() {
        return usedPercent;
    }

    public void setUsedPercent(double usedPercent) {
        this.usedPercent = usedPercent;
    }
}

