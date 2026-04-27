package org.munycha.logprocessor.notification;

import org.munycha.logprocessor.model.LogEvent;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class AlertAggregator {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final int FLUSH_INTERVAL_SECONDS = 15;
    private static final Pattern HAS_NUMBER = Pattern.compile(".*\\d+.*");

    private final TelegramNotificationService notifier;
    private final ExecutorService telegramExecutor;
    private final long cooldownMs;
    private final int thresholdCount;
    private final DrainParser drain = new DrainParser();

    // fingerprint → accumulated group waiting to be sent
    private final Map<String, AlertGroup> pending = new HashMap<>();
    // fingerprint → last time a Telegram was sent for this fingerprint
    private final Map<String, Long> lastSentTime = new HashMap<>();

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor();

    public AlertAggregator(TelegramNotificationService notifier,
                           ExecutorService telegramExecutor,
                           long cooldownMs,
                           int thresholdCount) {
        this.notifier = notifier;
        this.telegramExecutor = telegramExecutor;
        this.cooldownMs = cooldownMs;
        this.thresholdCount = thresholdCount;

        scheduler.scheduleAtFixedRate(
                this::flush,
                FLUSH_INTERVAL_SECONDS, FLUSH_INTERVAL_SECONDS, TimeUnit.SECONDS
        );
    }

    // Called by KafkaTopicConsumer for every matched alert log
    public synchronized void accept(LogEvent event, String matchedKeyword) {
        String fingerprint = buildFingerprint(matchedKeyword, event);
        long now = System.currentTimeMillis();

        Long lastSent = lastSentTime.get(fingerprint);
        boolean inCooldown = lastSent != null && (now - lastSent) < cooldownMs;

        if (!inCooldown) {
            lastSentTime.put(fingerprint, now);
            AlertGroup existing = pending.remove(fingerprint);
            if (existing != null) {
                existing.increment();
                submitTelegram(buildStillFiringMessage(existing));
            } else {
                submitTelegram(buildFirstMessage(event, matchedKeyword));
            }
            return;
        }

        // In cooldown — accumulate into bucket
        AlertGroup group = pending.get(fingerprint);
        if (group == null) {
            group = new AlertGroup(matchedKeyword, event.getServerName(),
                    event.getPath(), event.getTopic(), event.getMessage());
            pending.put(fingerprint, group);
        } else {
            group.increment();
        }

        // Count threshold hit — send early without waiting for flush
        if (group.getCount() >= thresholdCount) {
            lastSentTime.put(fingerprint, now);
            pending.remove(fingerprint);
            submitTelegram(buildStillFiringMessage(group));
        }
    }

    // Runs every 15 seconds — sends any bucket whose cooldown has expired
    private synchronized void flush() {
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<String, AlertGroup>> it = pending.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry<String, AlertGroup> entry = it.next();
            AlertGroup group = entry.getValue();

            Long lastSent = lastSentTime.get(entry.getKey());
            boolean cooldownExpired = lastSent == null || (now - lastSent) >= cooldownMs;

            if (cooldownExpired && group.getCount() > 0) {
                lastSentTime.put(entry.getKey(), now);
                it.remove();
                submitTelegram(buildStillFiringMessage(group));
            }
        }
    }

    private void submitTelegram(String message) {
        telegramExecutor.submit(() -> notifier.sendMessage(message));
    }

    private String buildFingerprint(String keyword, LogEvent event) {
        String msg = event.getMessage();
        String msgKey = HAS_NUMBER.matcher(msg).matches() ? drain.parseTemplate(msg) : msg;
        return keyword + "|" + event.getServerName() + "|" + msgKey;
    }

    private String buildFirstMessage(LogEvent event, String matchedKeyword) {
        String time = Instant.parse(event.getTimestamp())
                .atZone(ZoneId.systemDefault())
                .format(FORMATTER);

        return "ALERT\n" +
                "Time:    " + time + "\n" +
                "Host:    " + event.getServerName() + "\n" +
                "File:    " + event.getPath() + "\n" +
                "Topic:   " + event.getTopic() + "\n" +
                "Keyword: " + matchedKeyword + "\n" +
                "Message: " + event.getMessage();
    }

    private String buildStillFiringMessage(AlertGroup group) {
        return "ALERT (still firing)\n" +
                "Keyword: " + group.getMatchedKeyword() + "\n" +
                "Host:    " + group.getServerName() + "\n" +
                "File:    " + group.getFilePath() + "\n" +
                "Topic:   " + group.getTopic() + "\n" +
                "Count:   " + group.getCount() + " more since last notification\n" +
                "First:   " + formatMs(group.getFirstSeenMs()) + "\n" +
                "Last:    " + formatMs(group.getLastSeenMs()) + "\n" +
                "Message: " + group.getSampleMessage();
    }

    private String formatMs(long ms) {
        return Instant.ofEpochMilli(ms)
                .atZone(ZoneId.systemDefault())
                .format(FORMATTER);
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
