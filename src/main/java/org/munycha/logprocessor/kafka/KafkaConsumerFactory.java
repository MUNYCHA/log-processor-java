package org.munycha.logprocessor.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaConsumerFactory {

    private final Properties consumerProps;

    public KafkaConsumerFactory(String bootstrapServers, String groupId) {
        consumerProps = new Properties();

        // ===== REQUIRED =====
        consumerProps.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrapServers
        );

        consumerProps.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );
        consumerProps.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName()
        );

        consumerProps.put(
                ConsumerConfig.CLIENT_ID_CONFIG,
                "log-processor-" + java.util.UUID.randomUUID()
        );

        consumerProps.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                groupId
        );

        // ===== CRITICAL SAFETY =====

        // Disable auto-commit — application controls commits
        consumerProps.put(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                "false"
        );

        // Deterministic startup behavior
        consumerProps.put(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "latest"
        );

        // ===== POLL SAFETY =====

        consumerProps.put(
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                "200"
        );

        consumerProps.put(
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,
                "10000"
        );

        consumerProps.put(
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,
                "300000"
        );
    }

    public KafkaConsumer<String, String> createConsumer() {
        return new KafkaConsumer<>(consumerProps);
    }

    public Properties getConsumerProps() {
        return consumerProps;
    }
}
