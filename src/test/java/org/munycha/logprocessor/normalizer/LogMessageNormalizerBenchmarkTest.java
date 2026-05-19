package org.munycha.logprocessor.normalizer;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Throughput sanity check for the normalizer.
 *
 * Not a microbenchmark — JIT warmup is best-effort and timing is wall
 * clock. The goal is to detect a regression that would break the
 * 10k msgs/sec target on a single thread for a 12-line representative
 * workload. Threshold is intentionally generous to tolerate slow CI.
 */
class LogMessageNormalizerBenchmarkTest {

    private static final int TARGET_THROUGHPUT = 10_000;
    // Allow CI machines to be slow — pass if we hit at least 25% of target.
    private static final int MIN_ACCEPTABLE = TARGET_THROUGHPUT / 4;

    private static final List<String> WORKLOAD = Arrays.asList(
            "2026-05-19T10:23:45.123Z INFO Connection from 10.0.0.5:5432 took 1200ms",
            "192.168.1.1 - - [19/May/2026:10:23:45 +0000] \"GET /index.html HTTP/1.1\" 200 1234",
            "May 19 10:23:45 host1 sshd[1234]: Failed password for invalid user admin from 192.168.1.5",
            "Exception in thread \"main\" java.lang.NullPointerException",
            "at com.foo.Bar.process(Bar.java:42)",
            "at com.foo.Bar$$Lambda$123/0x000000080104a040.run(Bar.java:11)",
            "{\"level\":\"error\",\"msg\":\"db down\",\"duration_ms\":1500}",
            "Login failed for user=alice ip=10.0.0.1 retry=3",
            "Error processing request id=550e8400-e29b-41d4-a716-446655440000 path=/api/v1/users",
            "Disk usage is 87% on /var (45GB used, took 250ms to scan)",
            "WARN deadbeef feedface accede pure-letter hex words must stay literal",
            "2026/05/19 10:23:45 [error] 12345#0: *67890 upstream timed out"
    );

    @Test
    void normalizeAtLeast10kPerSecond() {
        LogMessageNormalizer n = new LogMessageNormalizer();

        // Warm up the JIT
        for (int i = 0; i < 5_000; i++) {
            for (String s : WORKLOAD) n.normalizeMessage(s);
        }

        int iterations = 20_000;
        long start = System.nanoTime();
        long sink = 0;
        for (int i = 0; i < iterations; i++) {
            for (String s : WORKLOAD) sink += n.normalizeMessage(s).length();
        }
        long elapsedNanos = System.nanoTime() - start;

        long totalMessages = (long) iterations * WORKLOAD.size();
        double seconds = elapsedNanos / 1_000_000_000.0;
        double throughput = totalMessages / seconds;

        System.out.printf("Normalizer throughput: %,.0f msgs/sec (%d msgs in %.2fs, sink=%d)%n",
                throughput, totalMessages, seconds, sink);

        assertTrue(throughput >= MIN_ACCEPTABLE,
                "Throughput " + (long) throughput + " msgs/sec is below the minimum acceptable "
                        + MIN_ACCEPTABLE + " (target " + TARGET_THROUGHPUT + ")");
    }
}
