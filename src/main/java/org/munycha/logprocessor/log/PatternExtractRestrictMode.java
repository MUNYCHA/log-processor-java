package org.munycha.logprocessor.log;

/**
 * How aggressively {@link LogMessageNormalizer} collapses variable
 * tokens before producing a fingerprint.
 *
 * <ul>
 *   <li>{@link #HIGH} — precision-first, the default. Only well-known
 *       token shapes (timestamps, IPs, UUIDs, paths, etc.) are replaced.
 *       Two log lines that differ in any literal word produce different
 *       fingerprints. Safe for alert-once-forever dedup.</li>
 *   <li>{@link #MEDIUM} — additionally collapses identifier-embedded
 *       numbers ({@code worker7} → {@code worker<N>}) and accepts hex
 *       words without the digit+letter requirement.</li>
 *   <li>{@link #LOW} — additionally collapses arbitrary alphabetic
 *       literals (length ≥ 3) to {@code <TOK>}, except for configured
 *       alert keywords. Maximum dedup at the cost of merging genuinely
 *       different alerts.</li>
 * </ul>
 *
 * Null/blank/unknown config values resolve to {@link #HIGH} — the safest
 * fallback under "alert once forever" semantics.
 */
public enum PatternExtractRestrictMode {
    HIGH,
    MEDIUM,
    LOW;

    public static PatternExtractRestrictMode parse(String s) {
        if (s == null) return HIGH;
        String v = s.trim().toLowerCase();
        if (v.isEmpty()) return HIGH;
        if (v.equals("low")) return LOW;
        if (v.equals("medium") || v.equals("med")) return MEDIUM;
        return HIGH;
    }
}
