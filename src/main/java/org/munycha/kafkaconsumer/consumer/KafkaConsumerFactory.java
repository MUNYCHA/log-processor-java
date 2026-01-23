package org.munycha.kafkaconsumer.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;


public class KafkaConsumerFactory {

    private final Properties consumerProps;

    public KafkaConsumerFactory(String bootstrapServers, String topic) {
        consumerProps = new Properties();

        // Kafka broker address(es)
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Deserializers for key and value — both are plain strings
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Use a unique group ID per topic to isolate consumption
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "file-log-consumer-" + topic);

        // Start consuming from the latest offset if no committed offset is found
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    }


    public KafkaConsumer<String, String> createConsumer() {
        return new KafkaConsumer<>(consumerProps);
    }


    public Properties getConsumerProps() {
        return consumerProps;
    }
}
