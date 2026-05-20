package org.munycha.logprocessor.log;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogMessageNormalizerTest {

    @Test
    void twoLogsWithSameStructureNormalizeToSamePattern() {
        String a = "2026-05-19T10:23:45.123Z [ERROR] munycha-svc connection from " +
                "192.168.1.100:54321 uuid=550e8400-e29b-41d4-a716-446655440000 failed " +
                "reading \"primary database\" at addr 0xdeadbeef1234 after 1500ms, " +
                "processed 45GB from worker pid=9876 at " +
                "com.foo.Service.process(Service.java:142) retry=3 cpu=87%";

        String b = "2026-05-20T11:45:01.999Z [ERROR] munycha-svc connection from " +
                "10.0.0.5:8080 uuid=11111111-2222-3333-4444-555555555555 failed " +
                "reading \"secondary cache\" at addr 0xcafebabe5678 after 8500ms, " +
                "processed 120GB from worker pid=5555 at " +
                "com.foo.Service.process(Service.java:777) retry=10 cpu=42%";

        assertEquals(LogMessageNormalizer.normalize(a), LogMessageNormalizer.normalize(b));
    }

    @Test
    void differentStructureProducesDifferentPattern() {
        String a = "connection refused";
        String b = "out of memory";
        assertNotEquals(LogMessageNormalizer.normalize(a), LogMessageNormalizer.normalize(b));
    }

    @Test
    void nullAndEmptyMessageReturnEmptyString() {
        assertEquals("", LogMessageNormalizer.normalize(null));
        assertEquals("", LogMessageNormalizer.normalize(""));
    }

    @Test
    void timestampIsCollapsedToPlaceholder() {
        String result = LogMessageNormalizer.normalize("2026-05-19T10:23:45.123Z error");
        assertTrue(result.contains("<TS>"), "expected <TS> placeholder in: " + result);
    }

    @Test
    void ipv4IsCollapsedToPlaceholder() {
        String result = LogMessageNormalizer.normalize("connection from 192.168.1.1");
        assertTrue(result.contains("<IP>"), "expected <IP> placeholder in: " + result);
    }

    @Test
    void uuidIsCollapsedToPlaceholder() {
        String result = LogMessageNormalizer.normalize(
                "request 550e8400-e29b-41d4-a716-446655440000 failed");
        assertTrue(result.contains("<UUID>"), "expected <UUID> placeholder in: " + result);
    }

    @Test
    void customRulesAreAppliedBeforeBuiltInRules() {
        LogMessageNormalizer.Rule rule = new LogMessageNormalizer.Rule(
                "worker-\\d+", "<WORKER>");
        LogMessageNormalizer normalizer =
                new LogMessageNormalizer(Collections.singletonList(rule));

        String result = normalizer.normalizeMessage("task assigned to worker-42 OK");
        assertTrue(result.contains("<WORKER>"), "expected <WORKER> placeholder in: " + result);
    }

    @Test
    void multipleCustomRulesAreAllApplied() {
        LogMessageNormalizer normalizer = new LogMessageNormalizer(Arrays.asList(
                new LogMessageNormalizer.Rule("worker-\\d+", "<WORKER>"),
                new LogMessageNormalizer.Rule("job-[a-z]+", "<JOB>")
        ));

        String result = normalizer.normalizeMessage("worker-7 picked up job-alpha");
        assertTrue(result.contains("<WORKER>"));
        assertTrue(result.contains("<JOB>"));
    }

    @Test
    void nullCustomRulesAreTreatedAsEmpty() {
        LogMessageNormalizer normalizer = new LogMessageNormalizer(null);
        String result = normalizer.normalizeMessage("simple message");
        assertEquals(LogMessageNormalizer.normalize("simple message"), result);
    }
}
