package org.munycha.logprocessor.notification;

/**
 * Sink for outbound alert messages. Implementations choose the transport
 * (Telegram HTTP API, Slack webhook, no-op for staging, etc.).
 */
public interface Notifier {

    void send(String message);
}
