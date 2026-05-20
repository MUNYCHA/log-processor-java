package org.munycha.logprocessor.log;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class LogMessageNormalizerPatternExtractRestrictModeTest {

    private static LogMessageNormalizer at(PatternExtractRestrictMode mode, String... keywords) {
        Set<String> kw = keywords.length == 0
                ? Collections.<String>emptySet()
                : new HashSet<>(Arrays.asList(keywords));
        return new LogMessageNormalizer(Collections.<LogMessageNormalizer.Rule>emptyList(), mode, kw);
    }

    @Test
    void highMatchesCurrentBehavior() {
        LogMessageNormalizer high = at(PatternExtractRestrictMode.HIGH);
        String[] corpus = {
                "Connection from 10.0.0.5:5432 took 1200ms",
                "worker7 failed at 2026-05-19T10:23:45Z",
                "Pod pod-abc-xyz crashed trace_id=550e8400-e29b-41d4-a716-446655440000",
                "Out of memory: heap=512MB usage=98%",
                "error in /var/log/app.log line 42",
                ""
        };
        for (String msg : corpus) {
            assertEquals(LogMessageNormalizer.normalize(msg), high.normalizeMessage(msg),
                    "HIGH must match the static default for: " + msg);
        }
    }

    @Test
    void mediumCollapsesIdentifierEmbeddedNumbers() {
        LogMessageNormalizer medium = at(PatternExtractRestrictMode.MEDIUM);
        String a = medium.normalizeMessage("worker7 failed");
        String b = medium.normalizeMessage("worker8 failed");
        String c = medium.normalizeMessage("worker123 failed");
        assertEquals(a, b);
        assertEquals(a, c);
    }

    @Test
    void mediumCollapsesPureAlphaHex() {
        LogMessageNormalizer medium = at(PatternExtractRestrictMode.MEDIUM);
        String a = medium.normalizeMessage("token deadbeef rejected");
        String b = medium.normalizeMessage("token feedface rejected");
        assertEquals(a, b);
        // And the canonical form has <HEX> where the all-alpha hex word was.
        assertEquals("token <HEX> rejected", a);
    }

    @Test
    void mediumCollapsesShortMixedHex() {
        LogMessageNormalizer medium = at(PatternExtractRestrictMode.MEDIUM);
        String a = medium.normalizeMessage("checksum 0a3f bad");
        String b = medium.normalizeMessage("checksum f00d bad");
        assertEquals(a, b);
        assertEquals("checksum <HEX> bad", a);
    }

    @Test
    void lowPreservesAlertKeywords() {
        LogMessageNormalizer low = at(PatternExtractRestrictMode.LOW, "timeout", "failed");

        String t1 = low.normalizeMessage("request timeout from backend");
        String t2 = low.normalizeMessage("request timeout from gateway");
        String f1 = low.normalizeMessage("request failed from backend");

        // Same keyword, different surrounding words → collapse to same fingerprint
        assertEquals(t1, t2);
        // Different keyword → distinct fingerprint
        assertNotEquals(t1, f1);
    }

    @Test
    void lowCollapsesArbitraryLiteralsToTok() {
        LogMessageNormalizer low = at(PatternExtractRestrictMode.LOW);
        String a = low.normalizeMessage("database connection refused");
        String b = low.normalizeMessage("database connection blocked");
        // Both literal words >= 3 chars, neither is an alert keyword → collapse identically.
        assertEquals(a, b);
    }
}
