package org.munycha.kafkaconsumer.config;


public class TopicConfig {
    private String topic;
    private TopicType type;
    private String output;

    public TopicConfig() {}

    public TopicConfig(String topic, TopicType type, String output) {
        this.topic = topic;
        this.type = type;
        this.output = output;
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
}

