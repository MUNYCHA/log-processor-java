package org.munycha.logprocessor.bootstrap;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Builds the two executors the application runs on:
 *
 * <ul>
 *   <li>A single-threaded Telegram sender so concurrent topics can't
 *       hammer the API in parallel and cause 429s.</li>
 *   <li>A fixed-size consumer pool sized to the number of topics so
 *       each poll loop runs on its own thread.</li>
 * </ul>
 */
public final class ExecutorFactory {

    private static final int TELEGRAM_QUEUE_CAPACITY = 1000;

    private ExecutorFactory() {}

    /**
     * One queue, one sender thread. Excess alerts beyond the queue
     * capacity are silently discarded (DiscardPolicy) so a Telegram
     * outage cannot stall the consumer threads.
     */
    public static ExecutorService telegramExecutor() {
        return new ThreadPoolExecutor(
                1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(TELEGRAM_QUEUE_CAPACITY),
                new ThreadPoolExecutor.DiscardPolicy()
        );
    }

    public static ExecutorService consumerExecutor(int topicCount) {
        return Executors.newFixedThreadPool(topicCount);
    }
}
