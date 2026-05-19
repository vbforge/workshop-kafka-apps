package com.vbforge.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static com.vbforge.config.Constants.BOOTSTRAP_SERVERS;

/**
 * Central Kafka configuration factory for all scenarios.
 *
 * Provides:
 *  - createProducerConfig()             — idempotent, async-ready
 *  - createConsumerConfig(groupId)      — auto-commit, earliest offset
 *  - createManualCommitConsumerConfig() — for scenario_05 manual offsets
 *  - createBatchConsumerConfig()        — for scenario_06 high-throughput
 */
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);


    // =========================================================================
    // PRODUCER
    // =========================================================================

    /**
     * Standard producer configuration used across all scenarios.
     *
     * Key decisions:
     *  - enable.idempotence=true: guarantees exactly-once delivery to a single
     *    partition across retries. Kafka automatically enforces acks=all and
     *    retries=MAX_VALUE when this is set — do NOT set those manually alongside it.
     *  - linger.ms=10: producer waits up to 10ms to batch messages together before
     *    sending. Only effective when messages arrive faster than the linger window.
     *    In scenario_02 with 500ms between sends, each message ships individually —
     *    linger has no effect there but is correct config for higher-throughput scenarios.
     *  - batch.size=16384: max bytes per batch (16KB). Works together with linger.ms.
     */
    public static Properties createProducerConfig() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Idempotent producer: prevents duplicate messages on retry.
        // Implicitly sets acks=all and retries=Integer.MAX_VALUE — don't override them.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // Batching: accumulate up to 16KB or wait up to 10ms before sending.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG,  10);

        return props;
    }


    // =========================================================================
    // CONSUMER — auto-commit (scenarios 01–04)
    // =========================================================================

    /**
     * Standard consumer configuration with auto-commit enabled.
     * Suitable for scenarios where "at-least-once" processing is acceptable
     * and you don't need precise control over when offsets are committed.
     *
     * @param groupId Consumer group ID — determines workload sharing behaviour.
     *                Consumers with the same groupId share partitions (load balance).
     *                Consumers with different groupIds each get all messages (broadcast).
     */
    public static Properties createConsumerConfig(String groupId) {
        log.info("Creating consumer config — groupId: {}", groupId);

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,          groupId);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // earliest: start from the beginning if no committed offset exists for this group.
        // latest:   start from the end (miss any messages sent before consumer started).
        // Use 'earliest' during development so you never miss messages on restart.
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Auto-commit: Kafka commits the offset every 1 second in the background.
        // Simple but imprecise — if your app crashes between poll() and commit,
        // messages may be reprocessed. See createManualCommitConsumerConfig() for control.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,      "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        // Max records returned per poll() call.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        // Heartbeat & session: consumer sends heartbeat every 3s.
        // If broker sees no heartbeat for 45s, it considers consumer dead and rebalances.
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,    "45000");

        return props;
    }


    // =========================================================================
    // CONSUMER — manual commit (scenario_05)
    // =========================================================================

    /**
     * Consumer config with auto-commit disabled.
     * Use when you need to control exactly when offsets are committed —
     * typically after you've confirmed the message was fully processed.
     *
     * Pair with consumer.commitSync() or consumer.commitAsync() in your code.
     *
     * @param groupId Consumer group ID
     */
    public static Properties createManualCommitConsumerConfig(String groupId) {
        Properties props = createConsumerConfig(groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }


    // =========================================================================
    // CONSUMER — batch processing (scenario_06)
    // =========================================================================

    /**
     * Consumer config optimized for batch processing with manual commit.
     * Increases max records per poll for higher throughput scenarios.
     *
     * @param groupId        Consumer group ID
     * @param maxPollRecords How many records to fetch per poll() call
     */
    public static Properties createBatchConsumerConfig(String groupId, int maxPollRecords) {
        Properties props = createManualCommitConsumerConfig(groupId);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(maxPollRecords));
        return props;
    }

}
