package org.munycha.logprocessor.log;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Decides whether a log message should be treated as an alert based on a
 * case-insensitive substring match against a fixed keyword set.
 *
 * Empty or null keyword input results in a detector that never matches.
 * Keywords are lowercased and trimmed once at construction; blank/null
 * entries are dropped.
 */
public class AlertDetector {

    private final Set<String> keywords;

    public AlertDetector(Collection<String> keywords) {
        if (keywords == null || keywords.isEmpty()) {
            this.keywords = Collections.emptySet();
            return;
        }
        Set<String> normalized = new HashSet<>();
        for (String k : keywords) {
            if (k == null) continue;
            String trimmed = k.trim();
            if (trimmed.isEmpty()) continue;
            normalized.add(trimmed.toLowerCase());
        }
        this.keywords = Collections.unmodifiableSet(normalized);
    }

    public boolean matches(String message) {
        if (keywords.isEmpty() || message == null) return false;
        String lower = message.toLowerCase();
        for (String k : keywords) {
            if (lower.contains(k)) return true;
        }
        return false;
    }
}
