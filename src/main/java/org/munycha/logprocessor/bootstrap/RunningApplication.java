package org.munycha.logprocessor.bootstrap;

import org.munycha.logprocessor.kafka.TopicPollLoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Handle to a fully-started application. Holds the poll loops and the
 * two executors so {@link #shutdown()} can stop them in the correct
 * order:
 *
 * <ol>
 *   <li>Signal every poll loop ({@code running=false}, Kafka wakeup).</li>
 *   <li>Wait up to 15s for in-flight batches to finish.</li>
 *   <li>Drain queued Telegram alerts within 30s before exit.</li>
 * </ol>
 */
public class RunningApplication {

    private static final Logger log = LoggerFactory.getLogger(RunningApplication.class);

    private static final long CONSUMER_SHUTDOWN_WAIT_S = 15;
    private static final long TELEGRAM_DRAIN_WAIT_S = 30;

    private final List<TopicPollLoop> pollLoops;
    private final ExecutorService consumerExecutor;
    private final ExecutorService telegramExecutor;

    public RunningApplication(List<TopicPollLoop> pollLoops,
                              ExecutorService consumerExecutor,
                              ExecutorService telegramExecutor) {
        this.pollLoops = pollLoops;
        this.consumerExecutor = consumerExecutor;
        this.telegramExecutor = telegramExecutor;
    }

    public void shutdown() {
        log.info("Signalling poll loops to stop...");
        for (TopicPollLoop pollLoop : pollLoops) {
            pollLoop.shutdown();
        }

        consumerExecutor.shutdown();
        try {
            if (!consumerExecutor.awaitTermination(CONSUMER_SHUTDOWN_WAIT_S, TimeUnit.SECONDS)) {
                log.warn("Poll loop threads did not stop in time, forcing.");
                consumerExecutor.shutdownNow();
            }
        } catch (InterruptedException ignored) {
            consumerExecutor.shutdownNow();
        }

        log.info("Draining pending Telegram alerts...");
        telegramExecutor.shutdown();
        try {
            if (!telegramExecutor.awaitTermination(TELEGRAM_DRAIN_WAIT_S, TimeUnit.SECONDS)) {
                log.warn("Telegram executor did not drain in time, forcing.");
                telegramExecutor.shutdownNow();
            }
        } catch (InterruptedException ignored) {
            telegramExecutor.shutdownNow();
        }

        log.info("Shutdown complete.");
    }
}
