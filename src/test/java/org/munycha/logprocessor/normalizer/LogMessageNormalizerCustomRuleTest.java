package org.munycha.logprocessor.normalizer;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LogMessageNormalizerCustomRuleTest {

    @Test
    void customRuleAppliesBeforeBuiltins() {
        LogMessageNormalizer n = new LogMessageNormalizer(Collections.singletonList(
                new LogMessageNormalizer.Rule("worker-\\d+", "<WORKER>")
        ));

        // Without the custom rule, "worker-7" stays as "worker-<N>" (hyphen-separated digit).
        // With the rule, the whole identifier collapses to <WORKER>.
        assertEquals("<WORKER> failed at <TS>",
                n.normalizeMessage("worker-7 failed at 2026-05-19T10:23:45Z"));
        assertEquals("<WORKER> failed at <TS>",
                n.normalizeMessage("worker-99 failed at 2026-05-19T10:23:45Z"));
    }

    @Test
    void multipleCustomRulesApplyInOrder() {
        LogMessageNormalizer n = new LogMessageNormalizer(Arrays.asList(
                new LogMessageNormalizer.Rule("pod-[\\w-]+", "<POD>"),
                new LogMessageNormalizer.Rule("trace_id=\\S+", "trace_id=<TRACE>")
        ));

        String out = n.normalizeMessage(
                "Pod pod-abc-xyz crashed trace_id=550e8400-e29b-41d4-a716-446655440000");
        assertEquals("pod <POD> crashed trace_id=<TRACE>", out);
    }

    @Test
    void noCustomRulesIsEquivalentToStaticConvenience() {
        LogMessageNormalizer n = new LogMessageNormalizer();
        String msg = "Connection from 10.0.0.5:5432 took 1200ms";

        assertEquals(LogMessageNormalizer.normalize(msg), n.normalizeMessage(msg));
    }

    @Test
    void emptyAndNullInput() {
        LogMessageNormalizer n = new LogMessageNormalizer();
        assertEquals("", n.normalizeMessage(""));
        assertEquals("", n.normalizeMessage(null));
        assertEquals("", n.normalizeMessage("   "));
    }
}
