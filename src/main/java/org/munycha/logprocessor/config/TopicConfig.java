package org.munycha.logprocessor.config;

import java.util.List;

public class TopicConfig {

    private String topic;
    private TopicType type;
    private String output;

    // OPTIONAL (only used when type = LOG)
    private List<String> alertKeywords;

    public TopicConfig() {}

    public TopicConfig(String topic, TopicType type, String output, List<String> alertKeywords) {
        this.topic = topic;
        this.type = type;
        this.output = output;
        this.alertKeywords = alertKeywords;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public TopicType getType() {
        return type;
    }

    public void setType(TopicType type) {
        this.type = type;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public List<String> getAlertKeywords() {
        return alertKeywords;
    }

    public void setAlertKeywords(List<String> alertKeywords) {
        this.alertKeywords = alertKeywords;
    }

    public boolean hasAlertKeywords() {
        return alertKeywords != null && !alertKeywords.isEmpty();
    }
}