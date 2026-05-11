package org.munycha.logprocessor.normalizer;

import java.util.regex.Pattern;

public final class LogMessageNormalizer {

    // Order matters — apply specific patterns before generic ones
    private static final Pattern UUID =
            Pattern.compile("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");

    private static final Pattern IP_PORT =
            Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}:\\d+");

    private static final Pattern IP =
            Pattern.compile("\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");

    // 0x-prefixed hex — before bare hex so 0xdeadbeef isn't partially matched
    private static final Pattern HEX_PREFIX =
            Pattern.compile("0x[0-9a-fA-F]+");

    // Bare hex strings 8+ chars — catches memory addresses and hashes without 0x
    private static final Pattern HEX_BARE =
            Pattern.compile("\\b[0-9a-fA-F]{8,}\\b");

    // key=value — keeps the key, replaces the value
    private static final Pattern KEY_VALUE =
            Pattern.compile("\\b(\\w+)=\\S+");

    // Conservative rule 7: only tokens that START with a digit
    // Catches: 30s, 95%, 3600, 2048MB — but NOT apache2, java8, log4j
    private static final Pattern DIGIT_LED_TOKEN =
            Pattern.compile("\\b\\d\\S*");

    private static final Pattern WHITESPACE =
            Pattern.compile("\\s+");

    private LogMessageNormalizer() {}

    public static String normalize(String message) {
        String s = message;
        s = UUID.matcher(s).replaceAll("<UUID>");
        s = IP_PORT.matcher(s).replaceAll("<IP>:<PORT>");
        s = IP.matcher(s).replaceAll("<IP>");
        s = HEX_PREFIX.matcher(s).replaceAll("<HEX>");
        s = HEX_BARE.matcher(s).replaceAll("<HEX>");
        s = KEY_VALUE.matcher(s).replaceAll("$1=<VAL>");
        s = DIGIT_LED_TOKEN.matcher(s).replaceAll("<TOKEN>");
        s = s.toLowerCase();
        s = WHITESPACE.matcher(s).replaceAll(" ");
        return s.trim();
    }
}
