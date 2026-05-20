package org.munycha.logprocessor.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.munycha.logprocessor.log.AlertDetector;
import org.munycha.logprocessor.log.AlertPatternStore;
import org.munycha.logprocessor.log.LogEvent;
import org.munycha.logprocessor.log.LogMessageNormalizer;
import org.munycha.logprocessor.repository.AlertRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Writer;
import java.util.Optional;

/**
 * Per-record handler for LOG-type topics.
 *
 * <ol>
 *   <li>Deserializes the record into a LogEvent.</li>
 *   <li>Appends the raw message to the batch buffer (always — even
 *       non-alert messages are persisted to the topic's output file).</li>
 *   <li>If the message matches no alert keyword, returns empty.</li>
 *   <li>If a pattern store is configured, normalizes the message and
 *       suppresses already-seen patterns.</li>
 *   <li>Persists the alert to the database and returns the event so the
 *       poll loop can enqueue a Telegram delivery.</li>
 * </ol>
 *
 * The pattern store may be null when dedup is disabled for the topic.
 */
public class LogRecordHandler implements RecordHandler {

    private static final Logger log = LoggerFactory.getLogger(LogRecordHandler.class);

    private final ObjectMapper mapper;
    private final AlertDetector alertDetector;
    private final LogMessageNormalizer normalizer;
    private final AlertPatternStore patternStore;
    private final AlertRepository alertRepository;

    public LogRecordHandler(ObjectMapper mapper,
                            AlertDetector alertDetector,
                            LogMessageNormalizer normalizer,
                            AlertPatternStore patternStore,
                            AlertRepository alertRepository) {
        this.mapper = mapper;
        this.alertDetector = alertDetector;
        this.normalizer = normalizer;
        this.patternStore = patternStore;
        this.alertRepository = alertRepository;
    }

    @Override
    public Optional<LogEvent> handle(ConsumerRecord<String, String> record, Writer batchBuffer) {
        try {
            LogEvent event = mapper.readValue(record.value(), LogEvent.class);
            String msg = event.getMessage();

            batchBuffer.write(msg);
            batchBuffer.write(System.lineSeparator());

            if (!alertDetector.matches(msg)) {
                return Optional.empty();
            }

            if (patternStore != null) {
                String pattern = normalizer.normalizeMessage(msg);
                if (patternStore.isKnown(pattern)) {
                    log.debug("Suppressed: pattern already known: {}", pattern);
                    return Optional.empty();
                }
                saveAlert(event);
                patternStore.add(pattern);
                return Optional.of(event);
            }

            saveAlert(event);
            return Optional.of(event);

        } catch (Exception e) {
            throw new RuntimeException("Log processing failed", e);
        }
    }

    private void saveAlert(LogEvent event) {
        alertRepository.saveAlert(
                event.getTopic(),
                event.getTimestamp(),
                event.getServerName(),
                event.getPath(),
                event.getMessage()
        );
    }
}
