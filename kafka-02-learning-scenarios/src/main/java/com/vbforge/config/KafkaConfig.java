package com.vbforge.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.vbforge.config.Constants.*;


/**
 * Configuration class for Kafka Producers & Consumers
 * Central configurations across all scenarios
 * Works with BOTH local Kafka and Docker Kafka
 */
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);


    // ==========================================================================================
    // PRODUCER CONFIGS
    // ==========================================================================================
    /**
     * Creates standard producer configuration
     * @return Properties configured for Kafka producer
     */
    public static Properties createProducerConfig() {
        Properties props = new Properties();

        // Kafka broker address (Docker exposed on localhost:9092)
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // Serializers convert Java Objects to bytes for transmission
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Producer acknowledgement configuration
        // 'all' means wait for all replicas to acknowledge (safest but slowest)
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // Number of retries if sending fails
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        // Batch size in bytes (improves throughput)
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // Wait up to 10ms to batch more messages together
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        // Compression (optional - good for performance)
        // props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return props;
    }

    /**
     * Creates producer config with custom settings for keyed messages
     * Ensures partitioner respects message keys
     */
    public static Properties createKeyedProducerConfig() {
        
        // Use default partitioner (respects keys)
        // No additional config needed - default behavior routes same keys to same partition
        return createProducerConfig();
    }


    // ==========================================================================================
    // CONSUMER CONFIGS
    // ==========================================================================================
    /**
     * Creates standard consumer configuration with auto-commit enabled
     * @param groupId The consumer group ID
     * @return Properties configured for Kafka consumer
     */
    public static Properties createConsumerConfig(String groupId) {

        log.info("Creating consumer config with groupId: {}", groupId);

        Properties props = new Properties();

        // Kafka broker address
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // Consumer group ID - consumers in same group share workload
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Deserializers convert bytes back to Java Objects
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // What to do if there's no initial offset or offset is out of range
        // 'earliest' - start from beginning, 'latest' - start from the end
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Auto-commit offset every 1 second
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        // Maximum records returned in a single poll
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // Heartbeat interval (important for consumer groups)
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        
        // Session timeout
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "45000");

        return props;
    }

    /**
     * Creates consumer config with manual offset commit disabled.
     * Use this when want to control exactly when offsets are committed.
     * @param groupId The consumer group ID
     * @return Properties configured for manual commit
     */
    public static Properties createManualCommitConsumerConfig(String groupId) {
        Properties props = createConsumerConfig(groupId);

        // Disable auto-commit - we'll commit manually
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return props;
    }

    /**
     * Creates consumer config with batch processing optimizations
     * @param groupId The consumer group ID
     * @param maxPollRecords Maximum records per poll
     * @return Properties configured for batch processing
     */
    public static Properties createBatchConsumerConfig(String groupId, int maxPollRecords) {
        Properties props = createManualCommitConsumerConfig(groupId);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));
        return props;
    }



}