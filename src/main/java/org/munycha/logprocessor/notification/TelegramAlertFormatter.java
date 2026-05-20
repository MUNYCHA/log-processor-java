package org.munycha.logprocessor.notification;

import org.munycha.logprocessor.log.LogEvent;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Renders a LogEvent into the human-readable Telegram alert body.
 *
 * The timestamp on the event is an ISO-8601 string (UTC instant); it is
 * reformatted in the system default zone for display.
 */
public class TelegramAlertFormatter {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final ZoneId zone;

    public TelegramAlertFormatter() {
        this(ZoneId.systemDefault());
    }

    /** Test-friendly constructor: pin the zone so output is deterministic. */
    public TelegramAlertFormatter(ZoneId zone) {
        this.zone = zone;
    }

    public String format(LogEvent event) {
        String when = Instant.parse(event.getTimestamp())
                .atZone(zone)
                .format(TIMESTAMP_FORMATTER);

        return "ALERT\nTime: " + when +
                "\nHost: " + event.getServerName() +
                "\nFile: " + event.getPath() +
                "\nTopic: " + event.getTopic() +
                "\nMessage: " + event.getMessage();
    }
}
