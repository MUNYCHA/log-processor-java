package org.munycha.logprocessor.telegram;

import javax.net.ssl.HttpsURLConnection;
import java.io.*;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TelegramNotifier {

    private static final int CONNECT_TIMEOUT_MS = 7000;
    private static final int READ_TIMEOUT_MS = 7000;
    private static final int MAX_RETRIES = 3;
    private static final int RATE_LIMIT_MS = 1100; // Telegram-safe

    private static final Pattern RETRY_AFTER_PATTERN =
            Pattern.compile("\"retry_after\"\\s*:\\s*(\\d+)");

    private final String botToken;
    private final String chatId;

    private volatile long lastSendTs = 0;

    public TelegramNotifier(String botToken, String chatId) {
        this.botToken = botToken;
        this.chatId = chatId;
    }

    /**
     * Blocking call.
     * MUST be executed from a background thread / executor.
     */
    public void sendMessage(String message) {

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                enforceRateLimit();

                HttpsURLConnection conn = openConnection();
                writeRequest(conn, message);

                int code = conn.getResponseCode();

                if (code >= 200 && code < 300) {
                    drain(conn.getInputStream());
                    lastSendTs = System.currentTimeMillis();
                    return;
                }

                if (code == 429) {
                    int retryAfter = extractRetryAfter(conn);
                    System.err.println("[TelegramNotifier] 429 rate limited, retry_after=" + retryAfter + "s");
                    Thread.sleep((retryAfter + 1L) * 1000);
                    continue;
                }

                String errorBody = readError(conn);
                System.err.println("[TelegramNotifier] HTTP " + code + " error: " + errorBody);
                return;

            } catch (SocketTimeoutException e) {
                System.err.println("[TelegramNotifier] Timeout attempt " + attempt + "/" + MAX_RETRIES);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                System.err.println("[TelegramNotifier] Fatal error: " + e.getMessage());
                return;
            }
        }

        System.err.println("[TelegramNotifier] Message dropped after retries");
    }

    // ----------------------------------------------------------------------

    private void enforceRateLimit() throws InterruptedException {
        long now = System.currentTimeMillis();
        long diff = now - lastSendTs;

        if (diff < RATE_LIMIT_MS) {
            Thread.sleep(RATE_LIMIT_MS - diff);
        }
    }

    private HttpsURLConnection openConnection() throws Exception {
        URL url = new URL("https://api.telegram.org/bot" + botToken + "/sendMessage");
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();

        conn.setRequestMethod("POST");
        conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
        conn.setReadTimeout(READ_TIMEOUT_MS);
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");

        return conn;
    }

    private void writeRequest(HttpsURLConnection conn, String message) throws IOException {
        String body = buildJson(message);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }
    }

    private String buildJson(String message) {
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
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    private void drain(InputStream is) throws IOException {
        try (InputStream in = is) {
            while (in.read() != -1) {
                // discard
            }
        }
    }

    private String readError(HttpsURLConnection conn) {
        InputStream es = conn.getErrorStream();
        if (es == null) return "<no body>";

        try (BufferedReader br = new BufferedReader(new InputStreamReader(es))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        } catch (IOException e) {
            return "<failed to read error body>";
        }
    }

    private int extractRetryAfter(HttpsURLConnection conn) {
        String body = readError(conn);
        Matcher m = RETRY_AFTER_PATTERN.matcher(body);
        return m.find() ? Integer.parseInt(m.group(1)) : 3;
    }
}
