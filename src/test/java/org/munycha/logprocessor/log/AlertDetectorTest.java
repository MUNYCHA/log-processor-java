package org.munycha.logprocessor.log;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AlertDetectorTest {

    @Test
    void nullKeywordsNeverMatch() {
        AlertDetector d = new AlertDetector(null);
        assertFalse(d.matches("FATAL: anything"));
    }

    @Test
    void emptyKeywordsNeverMatch() {
        AlertDetector d = new AlertDetector(Collections.emptyList());
        assertFalse(d.matches("FATAL: anything"));
    }

    @Test
    void matchIsCaseInsensitive() {
        AlertDetector d = new AlertDetector(Arrays.asList("FATAL"));
        assertTrue(d.matches("got a fatal error"));
        assertTrue(d.matches("FATAL"));
        assertTrue(d.matches("Fatal"));
    }

    @Test
    void substringMatchSucceeds() {
        AlertDetector d = new AlertDetector(Arrays.asList("deadlock"));
        assertTrue(d.matches("DETECTED deadlock between threads"));
    }

    @Test
    void noKeywordReturnsFalse() {
        AlertDetector d = new AlertDetector(Arrays.asList("PANIC", "OOM"));
        assertFalse(d.matches("info: started"));
    }

    @Test
    void nullMessageReturnsFalse() {
        AlertDetector d = new AlertDetector(Arrays.asList("ANY"));
        assertFalse(d.matches(null));
    }

    @Test
    void blankAndNullKeywordsAreDropped() {
        AlertDetector d = new AlertDetector(Arrays.asList("  ", null, "", "ERROR"));
        assertTrue(d.matches("an ERROR happened"));
        assertFalse(d.matches("normal log"));
    }

    @Test
    void keywordsAreTrimmedBeforeMatching() {
        AlertDetector d = new AlertDetector(Arrays.asList("  PANIC  "));
        assertTrue(d.matches("hit panic mode"));
    }
}
