package org.munycha.logprocessor.telegram;

import javax.net.ssl.HttpsURLConnection;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;

public class TelegramNotifier {

    private final String botToken;
    private final String chatId;
    private final BlockingQueue<String> messageQueue = new LinkedBlockingQueue<>(1000);
    private volatile boolean workerRunning = false;
    private final Object workerLock = new Object();

    // Track last send time for rate limiting
    private volatile long lastSendTime = 0;

    public TelegramNotifier(String botToken, String chatId) {
        this.botToken = botToken;
        this.chatId = chatId;
        startWorker();
    }

    // Public method - just adds to queue, returns immediately
    public void sendMessage(String message) {
        if (message == null || message.trim().isEmpty()) {
            return;
        }

        // Escape HTML entities to avoid breaking JSON
        String escaped = escapeJson(message);

        // Add to queue (non-blocking)
        boolean added = messageQueue.offer(escaped);
        if (!added) {
            System.err.println("[TelegramNotifier] Queue full (" + messageQueue.size() +
                    " messages), dropping: " + (message.length() > 50 ?
                    message.substring(0, 50) + "..." : message));
        }
    }

    private void startWorker() {
        synchronized (workerLock) {
            if (!workerRunning) {
                workerRunning = true;
                Thread worker = new Thread(this::processQueue, "TelegramNotifier-Worker");
                worker.setDaemon(true);
                worker.start();
            }
        }
    }

    private void processQueue() {
    //    System.out.println("[TelegramNotifier] Worker started, chatId: " + chatId);

        while (workerRunning) {
            try {
                // Take message from queue (blocks if empty)
                String message = messageQueue.poll(1, TimeUnit.SECONDS);

                if (message != null) {
                    sendMessageWithRateLimit(message);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("[TelegramNotifier] Error in worker: " + e.getMessage());
                // Continue processing other messages
            }
        }

        synchronized (workerLock) {
            workerRunning = false;
        }
        System.out.println("[TelegramNotifier] Worker stopped");
    }

    private void sendMessageWithRateLimit(String message) {
        int maxRetries = 3;
        int attempt = 0;

        while (attempt < maxRetries && workerRunning) {
            attempt++;

            try {
                // Enforce minimum 1.1 seconds between sends
                enforceRateLimit();

                // Send the message
                sendRequest(message);

                // Update last send time after successful send
                lastSendTime = System.currentTimeMillis();

                return; // Success!

            } catch (RateLimitException e) {
                int waitSeconds = e.getRetryAfterSeconds();
                System.err.println("[TelegramNotifier] Rate limited. Waiting "
                        + waitSeconds + " seconds (attempt " + attempt + "/" + maxRetries + ")");

                if (attempt == maxRetries) {
                    System.err.println("[TelegramNotifier] FAILED after max retries");
                    return;
                }

                try {
                    Thread.sleep((waitSeconds + 1) * 1000L);
                    lastSendTime = 0; // Reset to allow immediate retry
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }

            } catch (SocketTimeoutException e) {
                System.err.println("[TelegramNotifier] Timeout (attempt "
                        + attempt + "/" + maxRetries + ")");

                if (attempt == maxRetries) {
                    System.err.println("[TelegramNotifier] FAILED after max retries");
                    return;
                }

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                    return;
                }

            } catch (Exception e) {
                System.err.println("[TelegramNotifier] Error: " + e.getMessage());
                return; // Don't retry on other errors
            }
        }
    }

    private void enforceRateLimit() throws InterruptedException {
        long now = System.currentTimeMillis();
        long timeSinceLastSend = now - lastSendTime;

        // Telegram allows ~1 message per second to same chat
        // Use 1.1 seconds for safety margin
        long minDelay = 1100; // 1.1 seconds

        if (timeSinceLastSend < minDelay) {
            long sleepTime = minDelay - timeSinceLastSend;
            Thread.sleep(sleepTime);
        }
    }

    private URL buildUrl() throws IOException {
        return new URL("https://api.telegram.org/bot" + botToken + "/sendMessage");
    }

    private void sendRequest(String message) throws Exception {
        URL url = buildUrl();
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();

        // IMPORTANT: Set ALL connection properties BEFORE getting output stream
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(10000);
        conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
        conn.setRequestProperty("Accept", "application/json");

        // Write request body
        String body = buildJsonBody(message);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
            os.flush();
        }

        int responseCode = conn.getResponseCode();

        // Handle Telegram rate limit (HTTP 429)
        if (responseCode == 429) {
            String response = readErrorResponse(conn);
            int retryAfter = extractRetryAfter(response);
            throw new RateLimitException(retryAfter);
        }

        // Handle other HTTP errors
        if (responseCode != 200) {
            String errorMsg = "HTTP " + responseCode;
            if (responseCode >= 400 && responseCode < 500) {
                // Client error - don't retry immediately
                errorMsg += " - Client error, check bot token and chat ID";
                // Try to get error message
                String errorBody = readErrorResponse(conn);
                if (!errorBody.isEmpty()) {
                    errorMsg += " - Response: " + errorBody;
                }
            }
            throw new Exception(errorMsg);
        }

        // Success - consume response
        try (InputStream is = conn.getInputStream()) {
            // Java 1.8 compatible: read stream in chunks
            byte[] buffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = is.read(buffer)) != -1) {
                // Just consume the response
            }
        }
    }

    private String buildJsonBody(String message) {
        // Use HTML parse mode for better formatting and escaping
        return "{"
                + "\"chat_id\":\"" + chatId + "\","
                + "\"text\":\"" + message + "\","
                + "\"parse_mode\":\"HTML\""
                + "}";
    }

    private String readErrorResponse(HttpsURLConnection conn) throws IOException {
        InputStream errorStream = conn.getErrorStream();
        if (errorStream != null) {
            try (InputStream is = errorStream;
                 ByteArrayOutputStream result = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[1024];
                int length;
                while ((length = is.read(buffer)) != -1) {
                    result.write(buffer, 0, length);
                }
                return result.toString(StandardCharsets.UTF_8.name());
            }
        }
        return "";
    }

    private int extractRetryAfter(String jsonResponse) {
        // Telegram response format:
        // {"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":10}}

        if (jsonResponse.contains("\"retry_after\":")) {
            try {
                String[] parts = jsonResponse.split("\"retry_after\":");
                if (parts.length > 1) {
                    String number = parts[1].split("[,\\}]")[0].trim();
                    return Integer.parseInt(number);
                }
            } catch (Exception e) {
                // If parsing fails, use default
                System.err.println("[TelegramNotifier] Could not parse retry_after from: " + jsonResponse);
            }
        }
        return 5; // Default 5 seconds if we can't parse it
    }

    private String escapeJson(String s) {
        if (s == null) return "";

        // First escape for HTML (since we use HTML parse_mode)
        s = s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");

        // Then escape for JSON
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }

    // Custom exception for rate limiting
    private static class RateLimitException extends Exception {
        private final int retryAfterSeconds;

        public RateLimitException(int retryAfterSeconds) {
            super("Rate limited by Telegram, retry after " + retryAfterSeconds + " seconds");
            this.retryAfterSeconds = retryAfterSeconds;
        }

        public int getRetryAfterSeconds() {
            return retryAfterSeconds;
        }
    }

    // Get current queue size (for monitoring)
    public int getQueueSize() {
        return messageQueue.size();
    }

    // Stop the worker (optional)
    public void shutdown() {
        workerRunning = false;
    }
}