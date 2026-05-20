package org.munycha.logprocessor.log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Reduces a raw log line to a stable structural pattern.
 *
 * Pipeline (deterministic, pure regex + balance scanning, no learned state):
 *
 *   custom rules — per-topic, optional
 *     ↓
 *   Pass 1a — global regex sweeps. Each rule uses word-boundary anchors
 *             so a pattern matches even when embedded inside a larger
 *             token (e.g. memory address inside a JVM synthetic class
 *             name). Order is most-specific-first; the bare-number
 *             catchall is last.
 *     ↓
 *   Pass 1b — balance-aware extraction of quoted strings, JSON, and
 *             bracketed timestamps/arrays. A small char scanner, not
 *             regex, because regex cannot match balanced brackets.
 *     ↓
 *   Pass 2  — whitespace tokenize, then for each token: detect
 *             key=value pairs, detect IPv6 (predicate-based), lowercase
 *             literals while preserving any embedded UPPERCASE
 *             placeholders.
 *     ↓
 *   Pass 3  — trim.
 *
 * Precision is prioritized over recall: an unrecognized token stays
 * as a literal word rather than being collapsed by an aggressive
 * catch-all. False merges are dangerous under "alert once forever"
 * semantics because the silenced alert never recovers without admin
 * intervention.
 *
 * Thread-safety: each instance is intended to be owned by a single
 * consumer thread. Built-in rules are stateless and safe to share
 * across instances. Custom rules supplied to the constructor are
 * defensively copied.
 */
public final class LogMessageNormalizer {

    /** A user-defined regex replacement applied before any built-in logic. */
    public static final class Rule {
        public final Pattern pattern;
        public final String replacement;

        public Rule(String regex, String replacement) {
            this.pattern = Pattern.compile(regex);
            this.replacement = replacement;
        }
    }

    private static final Pattern KEY_PATTERN = Pattern.compile("[A-Za-z_][\\w.-]*");

    // ===================== PASS 1a — global regex sweeps =====================
    //
    // Each pattern matches one structural element anywhere in the line.
    // The order is load-bearing: longer / more specific patterns must
    // come before shorter / more generic ones that would partially eat
    // their territory.
    //
    // Word boundaries (\b) are used to prevent matching inside identifiers
    // (e.g. so "25minutes" is not matched as a 25-minute duration).

    private static final Pattern[] PRE_PATTERNS = new Pattern[] {
            // 1. Combined date+time (ISO and slash-separated variants)
            Pattern.compile("\\d{4}[-/]\\d{2}[-/]\\d{2}[T ]\\d{1,2}:\\d{2}:\\d{2}(?:[.,]\\d+)?(?:Z|[+-]\\d{2}:?\\d{2})?"),
            // 2. Apache CLF timestamp:  "19/May/2026:10:23:45 +0000"
            Pattern.compile("\\d{1,2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/\\d{4}:\\d{1,2}:\\d{2}:\\d{2}(?: +[+-]\\d{4})?"),
            // 3. Syslog timestamp:  "May 19 10:23:45"
            Pattern.compile("(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) +\\d{1,2} +\\d{2}:\\d{2}:\\d{2}"),
            // 4. URL — \\S+ greedily consumes until whitespace
            Pattern.compile("(?:https?|wss?|ftp)://\\S+"),
            // 5. Email
            Pattern.compile("\\b[\\w.+-]+@[\\w.-]+\\.[A-Za-z]{2,}\\b"),
            // 6. Java stack frame parens: (Foo.java:42)
            Pattern.compile("\\(\\w+\\.(?:java|kt|scala|py|js|cpp|c|go|rb|ts):\\d+\\)"),
            // 7. JVM synthetic lambda + memory address
            Pattern.compile("\\$\\$Lambda\\$\\d+/0x[0-9a-fA-F]+"),
            // 8. UUID
            Pattern.compile("\\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\\b"),
            // 9. MAC address (must precede 0xHEX so the colon-joined hex pairs are recognised as a MAC)
            Pattern.compile("\\b(?:[0-9a-fA-F]{2}[:-]){5}[0-9a-fA-F]{2}\\b"),
            // 10. 0x-prefixed hex
            Pattern.compile("\\b0x[0-9a-fA-F]+\\b"),
            // 11. IPv4:port (before bare IPv4)
            Pattern.compile("\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}:\\d+\\b"),
            // 12. IPv4
            Pattern.compile("\\b\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\b"),
            // 13. Windows path:  C:\foo\bar  (run before date so dates embedded in paths collapse)
            Pattern.compile("\\b[A-Za-z]:\\\\[\\w\\\\.\\-]+"),
            // 14. Unix path:  /foo/bar  (must have at least one /x/ segment to distinguish from a bare slash)
            Pattern.compile("(?<![\\w<])/(?:[\\w.\\-]+/)+[\\w.\\-]*"),
            // 15. Date only:  YYYY-MM-DD or YYYY/MM/DD
            Pattern.compile("\\b\\d{4}[-/]\\d{2}[-/]\\d{2}\\b"),
            // 16. Time only:  HH:MM:SS(.ms)
            Pattern.compile("\\b\\d{1,2}:\\d{2}:\\d{2}(?:[.,]\\d+)?\\b"),
            // 17. Bare hex 8+ chars — REQUIRE at least one digit AND one letter
            //     so English words ("feedface", "deadbeef") do NOT collapse to <HEX>.
            Pattern.compile("\\b(?=[0-9a-fA-F]*\\d)(?=[0-9a-fA-F]*[a-fA-F])[0-9a-fA-F]{8,}\\b"),
            // 18. Size with unit
            Pattern.compile("\\b\\d+(?:\\.\\d+)?(?:KB|MB|GB|TB|KiB|MiB|GiB|TiB|B)\\b"),
            // 19. Duration with unit
            Pattern.compile("\\b\\d+(?:\\.\\d+)?(?:ns|us|ms|s|m|h|d)\\b"),
            // 20. Percent
            Pattern.compile("\\b\\d+(?:\\.\\d+)?%"),
            // 21. Bare number (last numeric rule — catches what nothing else claimed).
            //     Lookbehind blocks `\w` and `<`: numbers attached to a letter or
            //     underscore (identifiers like apache2, java8, req_123) are kept,
            //     while space- or hyphen-separated digits collapse to <N>.
            //     Word-boundary trailing prevents collapsing the "4" in "log4j".
            Pattern.compile("(?<![\\w<])-?\\d+(?:\\.\\d+)?\\b")
    };
    private static final String[] PRE_REPLACEMENTS = new String[] {
            "<TS>",                          //  1
            "<TS>",                          //  2
            "<TS>",                          //  3
            "<URL>",                         //  4
            "<EMAIL>",                       //  5
            "(<FILE>:<LINE>)",               //  6
            "\\$\\$Lambda\\$<N>/<HEX>",      //  7
            "<UUID>",                        //  8
            "<MAC>",                         //  9
            "<HEX>",                         // 10
            "<IP>:<PORT>",                   // 11
            "<IP>",                          // 12
            "<PATH>",                        // 13
            "<PATH>",                        // 14
            "<TS>",                          // 15
            "<TS>",                          // 16
            "<HEX>",                         // 17
            "<SIZE>",                        // 18
            "<DUR>",                         // 19
            "<PCT>",                         // 20
            "<N>"                            // 21
    };

    private static final LogMessageNormalizer DEFAULT = new LogMessageNormalizer();

    // ===================== MEDIUM-mode passes (also run at LOW) =====================
    //
    // Digits embedded inside an identifier ( foo7 / req_123 / log4j ).
    // Lookbehind requires a letter or underscore so we don't double-handle the
    // standalone bare-number case PRE_PATTERN 21 already collapsed.
    private static final Pattern MEDIUM_IDENT_DIGITS = Pattern.compile("(?<=[A-Za-z_])\\d+");

    // All-letter hex words of 8+ chars ( deadbeef, feedface, cafebabe ).
    // PRE_PATTERN 17 deliberately leaves these literal at HIGH because they may
    // be real English words and a false merge silences alerts forever. At
    // MEDIUM that's acceptable.
    private static final Pattern MEDIUM_ALPHA_HEX_8 = Pattern.compile("\\b[a-fA-F]{8,}\\b");

    // Short hex of 4–7 chars with at least one digit and one letter ( 0a3f, f00d ).
    private static final Pattern MEDIUM_MIXED_HEX_47 =
            Pattern.compile("\\b(?=[0-9a-fA-F]*\\d)(?=[0-9a-fA-F]*[a-fA-F])[0-9a-fA-F]{4,7}\\b");

    private final List<Rule> customRules;
    private final PatternExtractRestrictMode mode;
    private final Set<String> keepWords;

    public LogMessageNormalizer() {
        this(Collections.<Rule>emptyList(), PatternExtractRestrictMode.HIGH, Collections.<String>emptySet());
    }

    public LogMessageNormalizer(List<Rule> customRules) {
        this(customRules, PatternExtractRestrictMode.HIGH, Collections.<String>emptySet());
    }

    public LogMessageNormalizer(List<Rule> customRules,
                                PatternExtractRestrictMode mode,
                                Set<String> alertKeywords) {
        this.customRules = customRules == null
                ? Collections.<Rule>emptyList()
                : Collections.unmodifiableList(new ArrayList<Rule>(customRules));
        this.mode = mode == null ? PatternExtractRestrictMode.HIGH : mode;
        this.keepWords = buildKeepWords(alertKeywords);
    }

    private static Set<String> buildKeepWords(Set<String> alertKeywords) {
        if (alertKeywords == null || alertKeywords.isEmpty()) {
            return Collections.emptySet();
        }
        Set<String> out = new HashSet<String>(alertKeywords.size());
        for (String w : alertKeywords) {
            if (w == null) continue;
            String trimmed = w.trim().toLowerCase();
            if (!trimmed.isEmpty()) out.add(trimmed);
        }
        return Collections.unmodifiableSet(out);
    }

    /** Convenience for callers with no custom rules (e.g. unit tests). */
    public static String normalize(String message) {
        return DEFAULT.normalizeMessage(message);
    }

    public String normalizeMessage(String message) {
        if (message == null || message.isEmpty()) return "";

        String s = message;
        for (Rule r : customRules) {
            s = r.pattern.matcher(s).replaceAll(r.replacement);
        }
        for (int i = 0; i < PRE_PATTERNS.length; i++) {
            s = PRE_PATTERNS[i].matcher(s).replaceAll(PRE_REPLACEMENTS[i]);
        }
        if (mode == PatternExtractRestrictMode.MEDIUM || mode == PatternExtractRestrictMode.LOW) {
            s = mediumPass(s);
        }
        s = extractStructures(s);
        s = classifyTokens(s);
        if (mode == PatternExtractRestrictMode.LOW) {
            s = lowPass(s);
        }
        return s.trim();
    }

    // ===================== MEDIUM pass =====================

    private String mediumPass(String s) {
        // Order matters: hex rules first, so they get a chance to claim a
        // digit-bearing run like "0a3f" before MEDIUM_IDENT_DIGITS strips
        // the digit and leaves a stub like "0a<N>f".
        s = MEDIUM_ALPHA_HEX_8.matcher(s).replaceAll("<HEX>");
        s = MEDIUM_MIXED_HEX_47.matcher(s).replaceAll("<HEX>");
        s = MEDIUM_IDENT_DIGITS.matcher(s).replaceAll("<N>");
        return s;
    }

    // ===================== LOW pass =====================
    //
    // Walk tokens of the already-classified output. Any token whose core is
    // a pure alphabetic word of length >= 3 collapses to <TOK> unless it is
    // a configured alert keyword. Tokens that are placeholders, contain a
    // placeholder, or are key=value pairs are left alone.

    private String lowPass(String s) {
        StringBuilder out = new StringBuilder(s.length());
        int n = s.length();
        int i = 0;
        boolean first = true;
        while (i < n) {
            while (i < n && Character.isWhitespace(s.charAt(i))) i++;
            if (i >= n) break;
            int start = i;
            while (i < n && !Character.isWhitespace(s.charAt(i))) i++;
            String token = s.substring(start, i);
            if (!first) out.append(' ');
            out.append(collapseLowToken(token));
            first = false;
        }
        return out.toString();
    }

    private String collapseLowToken(String token) {
        int start = 0;
        int end = token.length();
        while (start < end && isPeelable(token.charAt(start))) start++;
        while (end > start && isPeelable(token.charAt(end - 1))) end--;

        String prefix = token.substring(0, start);
        String core = token.substring(start, end);
        String suffix = token.substring(end);

        if (core.length() < 3) return token;
        if (isPlaceholder(core) || containsPlaceholder(core)) return token;
        if (core.indexOf('=') >= 0) return token;
        for (int i = 0; i < core.length(); i++) {
            if (!Character.isLetter(core.charAt(i))) return token;
        }
        if (keepWords.contains(core.toLowerCase())) return token;
        return prefix + "<TOK>" + suffix;
    }

    // ===================== PASS 1b — balance-aware extraction =====================

    private String extractStructures(String s) {
        StringBuilder out = new StringBuilder(s.length());
        int i = 0;
        int n = s.length();

        while (i < n) {
            char c = s.charAt(i);

            if (c == '"' || c == '\'') {
                int end = findStringEnd(s, i, c);
                out.append("<STR>");
                i = end + 1;
            } else if (c == '{') {
                int end = findBalanced(s, i, '{', '}');
                if (end > i) {
                    out.append("<JSON>");
                    i = end + 1;
                } else {
                    out.append(c);
                    i++;
                }
            } else if (c == '[') {
                int end = findBalanced(s, i, '[', ']');
                if (end > i) {
                    String inner = s.substring(i + 1, end);
                    out.append(classifyBracketed(inner));
                    i = end + 1;
                } else {
                    out.append(c);
                    i++;
                }
            } else {
                out.append(c);
                i++;
            }
        }

        return out.toString();
    }

    private int findStringEnd(String s, int start, char quote) {
        int n = s.length();
        int i = start + 1;
        while (i < n) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < n) {
                i += 2;
                continue;
            }
            if (c == quote) return i;
            i++;
        }
        // Unterminated string — consume to end so we don't loop.
        return n - 1;
    }

    private int findBalanced(String s, int start, char open, char close) {
        int depth = 0;
        int n = s.length();
        for (int i = start; i < n; i++) {
            char c = s.charAt(i);
            if (c == '"' || c == '\'') {
                i = findStringEnd(s, i, c);
                continue;
            }
            if (c == open) {
                depth++;
            } else if (c == close) {
                depth--;
                if (depth == 0) return i;
            }
        }
        return -1;
    }

    /**
     * Decide whether bracketed content is a timestamp, an array, or
     * plain text. Plain-text content is left in place (with brackets)
     * so the tokenizer can classify its contents individually.
     */
    private String classifyBracketed(String inner) {
        boolean hasColonDigit = false;
        for (int i = 0; i + 1 < inner.length(); i++) {
            if (inner.charAt(i) == ':' && Character.isDigit(inner.charAt(i + 1))) {
                hasColonDigit = true;
                break;
            }
        }
        if (hasColonDigit) return "<TS>";
        if (inner.indexOf(',') >= 0) return "<ARR>";
        return "[" + inner + "]";
    }

    // ===================== PASS 2 — token-level handling =====================

    private String classifyTokens(String s) {
        StringBuilder out = new StringBuilder(s.length());
        int n = s.length();
        int i = 0;
        boolean first = true;

        while (i < n) {
            while (i < n && Character.isWhitespace(s.charAt(i))) i++;
            if (i >= n) break;

            int start = i;
            while (i < n && !Character.isWhitespace(s.charAt(i))) i++;
            String token = s.substring(start, i);

            if (!first) out.append(' ');
            out.append(classifyToken(token));
            first = false;
        }

        return out.toString();
    }

    private String classifyToken(String token) {
        int start = 0;
        int end = token.length();
        while (start < end && isPeelable(token.charAt(start))) start++;
        while (end > start && isPeelable(token.charAt(end - 1))) end--;

        String prefix = token.substring(0, start);
        String core = token.substring(start, end);
        String suffix = token.substring(end);

        return prefix + classifyCore(core) + suffix;
    }

    private boolean isPeelable(char c) {
        switch (c) {
            case ',': case ';': case '.': case '!': case '?':
            case '(': case ')': case '[': case ']': case '{': case '}':
                return true;
            default:
                return false;
        }
    }

    private String classifyCore(String core) {
        if (core.isEmpty()) return core;
        if (isPlaceholder(core)) return core;

        // IPv6: predicate-based, only on whole tokens, to avoid false-merging
        // plain HH:MM:SS times (Pass 1a already replaced those).
        if (isIPv6(core)) return "<IP6>";

        // key=value
        int eq = core.indexOf('=');
        if (eq > 0 && eq < core.length() - 1) {
            String key = core.substring(0, eq);
            String value = core.substring(eq + 1);
            if (KEY_PATTERN.matcher(key).matches()) {
                return key.toLowerCase() + "=" + classifyValue(value);
            }
        }

        return lowercaseKeepingPlaceholders(core);
    }

    private String classifyValue(String value) {
        if (value.isEmpty()) return "<VAL>";
        if (isPlaceholder(value)) return value;
        // Plain-text values become <VAL>; values containing one or more
        // placeholders are kept (with placeholders preserved) because
        // they already carry structural meaning.
        if (containsPlaceholder(value)) {
            return lowercaseKeepingPlaceholders(value);
        }
        return "<VAL>";
    }

    private static boolean containsPlaceholder(String s) {
        int n = s.length();
        for (int i = 0; i + 2 < n; i++) {
            if (s.charAt(i) == '<' && scanPlaceholder(s, i) > i) return true;
        }
        return false;
    }

    private static boolean isPlaceholder(String s) {
        int n = s.length();
        if (n < 3 || s.charAt(0) != '<' || s.charAt(n - 1) != '>') return false;
        for (int i = 1; i < n - 1; i++) {
            if (!isPlaceholderChar(s.charAt(i))) return false;
        }
        return true;
    }

    /**
     * Lowercase a string, but preserve any embedded {@code <PLACEHOLDER>}
     * substrings verbatim. A placeholder is any {@code <...>} cluster
     * whose interior is uppercase A-Z, digits, underscore, colon, or
     * nested {@code <>} (the compound case like {@code <IP>:<PORT>}).
     */
    private static String lowercaseKeepingPlaceholders(String s) {
        StringBuilder out = new StringBuilder(s.length());
        int n = s.length();
        int i = 0;
        while (i < n) {
            char c = s.charAt(i);
            if (c == '<') {
                int end = scanPlaceholder(s, i);
                if (end > i) {
                    out.append(s, i, end + 1);
                    i = end + 1;
                    continue;
                }
            }
            out.append(Character.toLowerCase(c));
            i++;
        }
        return out.toString();
    }

    /** Returns the index of the closing {@code >}, or -1 if not a valid placeholder. */
    private static int scanPlaceholder(String s, int from) {
        int n = s.length();
        if (from + 2 >= n || s.charAt(from) != '<') return -1;
        for (int i = from + 1; i < n; i++) {
            char c = s.charAt(i);
            if (c == '>') {
                return i > from + 1 ? i : -1;
            }
            if (!isPlaceholderChar(c)) return -1;
        }
        return -1;
    }

    private static boolean isPlaceholderChar(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == ':' || c == '<' || c == '>';
    }

    // ===================== IPv6 predicate =====================

    private static boolean isIPv6(String s) {
        if (s.length() < 3) return false;
        int colons = 0;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == ':') {
                colons++;
            } else if (!isHex(c)) {
                return false;
            }
        }
        if (s.indexOf("::") >= 0) {
            return colons >= 2 && colons <= 7;
        }
        return colons == 7;
    }

    private static boolean isHex(char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    }
}
