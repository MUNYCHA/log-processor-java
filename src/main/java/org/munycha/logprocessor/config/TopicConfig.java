package org.munycha.logprocessor.config;

import java.util.List;

public class TopicConfig {

    private String topic;
    private TopicType type;
    private String output;

    // OPTIONAL (only used when type = LOG)
    private List<String> alertKeywords;

    // OPTIONAL — if set, enables alert deduplication for this topic
    private String patternStoreFile;

    // OPTIONAL — per-topic regex replacement rules applied BEFORE the
    // built-in normalizer. Lets operators handle app-specific tokens
    // (e.g. "worker-\d+" → "<WORKER>") without modifying core code.
    private List<NormalizationRule> customNormalizationRules;

    // OPTIONAL — "high" (default), "medium", or "low". Controls how
    // aggressively the normalizer collapses variable tokens before
    // fingerprinting. Null/blank/unknown values fall back to HIGH.
    private String patternExtractRestrictMode;

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

    public String getPatternStoreFile() {
        return patternStoreFile;
    }

    public void setPatternStoreFile(String patternStoreFile) {
        this.patternStoreFile = patternStoreFile;
    }

    public boolean hasPatternStore() {
        return patternStoreFile != null && !patternStoreFile.trim().isEmpty();
    }

    public List<NormalizationRule> getCustomNormalizationRules() {
        return customNormalizationRules;
    }

    public void setCustomNormalizationRules(List<NormalizationRule> customNormalizationRules) {
        this.customNormalizationRules = customNormalizationRules;
    }

    public boolean hasCustomNormalizationRules() {
        return customNormalizationRules != null && !customNormalizationRules.isEmpty();
    }

    public String getPatternExtractRestrictMode() {
        return patternExtractRestrictMode;
    }

    public void setPatternExtractRestrictMode(String patternExtractRestrictMode) {
        this.patternExtractRestrictMode = patternExtractRestrictMode;
    }

    /** Per-topic regex replacement rule (deserialized from JSON config). */
    public static class NormalizationRule {
        private String pattern;
        private String replacement;

        public NormalizationRule() {}

        public NormalizationRule(String pattern, String replacement) {
            this.pattern = pattern;
            this.replacement = replacement;
        }

        public String getPattern() { return pattern; }
        public void setPattern(String pattern) { this.pattern = pattern; }
        public String getReplacement() { return replacement; }
        public void setReplacement(String replacement) { this.replacement = replacement; }
    }
}