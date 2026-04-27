package org.munycha.logprocessor.notification;

public class AlertGroup {

    private final String matchedKeyword;
    private final String serverName;
    private final String filePath;
    private final String topic;
    private final String sampleMessage;
    private final long firstSeenMs;
    private long lastSeenMs;
    private int count;

    public AlertGroup(String matchedKeyword, String serverName, String filePath,
                      String topic, String sampleMessage) {
        this.matchedKeyword = matchedKeyword;
        this.serverName = serverName;
        this.filePath = filePath;
        this.topic = topic;
        this.sampleMessage = sampleMessage;
        this.firstSeenMs = System.currentTimeMillis();
        this.lastSeenMs = this.firstSeenMs;
        this.count = 1;
    }

    public void increment() {
        count++;
        lastSeenMs = System.currentTimeMillis();
    }

    public String getMatchedKeyword() { return matchedKeyword; }
    public String getServerName()     { return serverName; }
    public String getFilePath()       { return filePath; }
    public String getTopic()          { return topic; }
    public String getSampleMessage()  { return sampleMessage; }
    public long getFirstSeenMs()      { return firstSeenMs; }
    public long getLastSeenMs()       { return lastSeenMs; }
    public int getCount()             { return count; }
}
