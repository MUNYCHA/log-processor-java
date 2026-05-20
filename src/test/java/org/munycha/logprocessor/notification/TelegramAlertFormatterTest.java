package org.munycha.logprocessor.notification;

import org.junit.jupiter.api.Test;
import org.munycha.logprocessor.log.LogEvent;

import java.time.ZoneId;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TelegramAlertFormatterTest {

    private static final ZoneId UTC = ZoneOffset.UTC;

    @Test
    void formatsExpectedAlertBody() {
        LogEvent event = new LogEvent(
                "server-01",
                "/var/log/app.log",
                "app-logs",
                "2026-05-20T10:23:45Z",
                "connection refused"
        );

        String result = new TelegramAlertFormatter(UTC).format(event);

        String expected =
                "ALERT\n" +
                "Time: 2026-05-20 10:23:45\n" +
                "Host: server-01\n" +
                "File: /var/log/app.log\n" +
                "Topic: app-logs\n" +
                "Message: connection refused";

        assertEquals(expected, result);
    }

    @Test
    void timestampReformattedInGivenZone() {
        LogEvent event = new LogEvent(
                "srv", "/p", "t", "2026-05-20T00:00:00Z", "msg");

        // +07:00 zone: 00:00 UTC becomes 07:00 local
        String result = new TelegramAlertFormatter(ZoneId.of("Asia/Bangkok")).format(event);

        assertTrue(result.contains("Time: 2026-05-20 07:00:00"),
                "expected Bangkok-local timestamp, got: " + result);
    }

    @Test
    void includesAllFiveFields() {
        LogEvent event = new LogEvent("h", "/p", "tp", "2026-01-01T00:00:00Z", "m");
        String result = new TelegramAlertFormatter(UTC).format(event);

        assertTrue(result.startsWith("ALERT\n"));
        assertTrue(result.contains("Host: h"));
        assertTrue(result.contains("File: /p"));
        assertTrue(result.contains("Topic: tp"));
        assertTrue(result.contains("Message: m"));
    }
}
