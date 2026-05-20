package org.munycha.logprocessor.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class TelegramNotificationService implements Notifier {

    private static final Logger log = LoggerFactory.getLogger(TelegramNotificationService.class);

    // Telegram allows 20 messages/minute to the same chat = 1 message per 3 seconds.
    // Using 3s as the baseline avoids proactive 429s on group/channel chats.
    private static final long BASE_SEND_INTERVAL_MS = 3_000;

    // Adaptive backoff ceiling: do not wait longer than 60 seconds between sends.
    private static final long MAX_SEND_INTERVAL_MS = 60_000;

    private final String botToken;
    private final String chatId;
    private long lastSend = 0;

    // Tracks the current minimum interval between sends. Doubles on every 429,
    // resets to baseline on every successful send.
    private long minSendIntervalMs = BASE_SEND_INTERVAL_MS;

    public TelegramNotificationService(String botToken, String chatId) {
        this.botToken = botToken;
        this.chatId = chatId;
    }

    @Override
    public synchronized void send(String message) {

        try {
            enforceRateLimit();
        } catch (InterruptedException ignored) {
            return;
        }

        int maxRetries = 3;

        for (int i = 1; i <= maxRetries; i++) {

            try {

                sendRequest(message);
                lastSend = System.currentTimeMillis();
                // Reset adaptive backoff on success — the API is healthy again
                minSendIntervalMs = BASE_SEND_INTERVAL_MS;
                return;

            } catch (SocketTimeoutException e) {

                log.warn("Timeout (attempt {}/{})", i, maxRetries);

                if (i == maxRetries) {
                    log.error("FAILED after {} timeouts, dropped.", maxRetries);
                    return;
                }

                sleep(1000);

            } catch (RetryAfterException e) {

                // Adaptive backoff: double the minimum send interval on every 429, up to the cap.
                // This causes subsequent messages to be sent more slowly, preventing the next
                // burst of alerts from immediately 429-ing again after the retry_after wait.
                minSendIntervalMs = Math.min(minSendIntervalMs * 2, MAX_SEND_INTERVAL_MS);

                log.warn("429 rate-limited, retry_after={}s — backing off, new interval={}s",
                        e.retryAfter, minSendIntervalMs / 1000);

                sleep(e.retryAfter * 1000L);

            } catch (Exception e) {

                log.error("Fatal error: {}", e.getMessage(), e);
                return;
            }
        }

        // Reached only when RetryAfterException exhausts all retries
        log.error("FAILED after {} attempts (persistent 429), dropped.", maxRetries);
    }

    private void enforceRateLimit() throws InterruptedException {
        long now = System.currentTimeMillis();
        long diff = now - lastSend;

        if (diff < minSendIntervalMs) {
            Thread.sleep(minSendIntervalMs - diff);
        }
    }

    private void sendRequest(String message) throws Exception {

        URL url = new URL(
                "https://api.telegram.org/bot" + botToken + "/sendMessage"
        );

        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();

        conn.setRequestMethod("POST");
        conn.setConnectTimeout(7000);
        conn.setReadTimeout(7000);
        conn.setDoOutput(true);
        conn.setRequestProperty(
                "Content-Type", "application/json; charset=UTF-8"
        );

        String body = buildJsonBody(message);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }

        readResponse(conn);
    }

    private String readStream(InputStream is) throws IOException {

        StringBuilder sb = new StringBuilder();
        byte[] buffer = new byte[1024];
        int len;

        while ((len = is.read(buffer)) != -1) {
            sb.append(new String(buffer, 0, len, StandardCharsets.UTF_8));
        }

        return sb.toString();
    }

    private void readResponse(HttpsURLConnection conn) throws Exception {

        int status = conn.getResponseCode();

        if (status == 200) {
            try (InputStream is = conn.getInputStream()) {
                // OK
            }
            return;
        }

        if (status == 429) {

            try (InputStream es = conn.getErrorStream()) {

                String body = readStream(es);

                int retryAfter = extractRetryAfter(body);

                throw new RetryAfterException(retryAfter);
            }
        }

        if (status == 400) {
            throw new Exception("Bad request (JSON likely malformed)");
        }

        throw new Exception("Telegram HTTP error: " + status);
    }

    private int extractRetryAfter(String body) {

        try {
            int idx = body.indexOf("retry_after");
            if (idx == -1) return 5;

            int colon = body.indexOf(":", idx);
            int comma = body.indexOf(",", colon);
            if (comma == -1) comma = body.indexOf("}", colon);

            return Integer.parseInt(
                    body.substring(colon + 1, comma).trim()
            );

        } catch (Exception ignored) {
            return 5;
        }
    }

    private String buildJsonBody(String message) {
        return "{"
                + "\"chat_id\":\"" + chatId + "\","
                + "\"text\":\"" + escapeJson(message) + "\""
                + "}";
    }

    private String escapeJson(String s) {
        return s
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }

    private void sleep(long ms) {
        try { Thread.sleep(ms); }
        catch (InterruptedException ignored) {}
    }

    private static class RetryAfterException extends Exception {
        final int retryAfter;
        RetryAfterException(int retryAfter) {
            this.retryAfter = retryAfter;
        }
    }
}
