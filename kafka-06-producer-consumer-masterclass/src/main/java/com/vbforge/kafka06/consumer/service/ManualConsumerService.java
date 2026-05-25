package com.vbforge.kafka06.consumer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Demonstrates the <strong>low-level Kafka Consumer API</strong> — without {@code @KafkaListener}.
 *
 * <p>Why would you use this instead of {@code @KafkaListener}?
 * <ul>
 *   <li>You need to seek to a specific offset (e.g. replay from offset 0)</li>
 *   <li>You need to consume from a specific partition only</li>
 *   <li>You want full manual control over the poll loop</li>
 *   <li>You are building a one-shot "read and return" endpoint (like this one)</li>
 * </ul>
 *
 * <p><strong>Thread-safety note:</strong>
 * A {@link KafkaConsumer} is NOT thread-safe. The original code injected a shared
 * {@code Consumer} bean which would break under concurrent requests.
 * This refactored version creates a <em>fresh consumer per request</em> and closes it
 * in a {@code try-with-resources} block — safe, correct, and no shared state.
 */
@Slf4j
@Service
public class ManualConsumerService {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.groups.manual}")
    private String manualGroupId;

    /**
     * Reads messages from a specific topic partition starting at the given offset.
     *
     * <p>Flow:
     * <ol>
     *   <li>Create a fresh {@link KafkaConsumer} with its own connection</li>
     *   <li>{@code assign()} — subscribe to one specific partition (not the whole topic)</li>
     *   <li>{@code seek()}   — jump to the requested offset instead of reading from last committed</li>
     *   <li>{@code poll()}   — fetch available records within 2 seconds</li>
     *   <li>Consumer is closed automatically by try-with-resources</li>
     * </ol>
     *
     * @param topicName the Kafka topic to read from
     * @param partition partition number (0-based)
     * @param offset    the offset to start reading from (inclusive)
     * @return list of message values (deserialized from JSON)
     */
    public List<Object> readMessages(String topicName, int partition, long offset) {
        log.info("[MANUAL-CONSUMER] Reading topic={} partition={} offset={}", topicName, partition, offset);

        // Creating the consumer inside try-with-resources ensures it is always closed,
        // even if an exception is thrown. This releases the TCP connection to the broker.
        try (KafkaConsumer<String, Object> consumer = createConsumer()) {

            // assign() gives us direct control over which partition to read.
            // Unlike subscribe(), assign() does NOT join a consumer group for rebalancing.
            TopicPartition tp = new TopicPartition(topicName, partition);
            consumer.assign(Collections.singletonList(tp));

            // seek() sets the read position. Without this, the consumer would start
            // at the last committed offset for this group — which may be at the end.
            consumer.seek(tp, offset);

            // poll() fetches records. The Duration is the max time to wait if the
            // partition has no new records. 2 seconds is generous for a demo endpoint.
            ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(2));

            log.info("[MANUAL-CONSUMER] Fetched {} record(s)", records.count());

            for (ConsumerRecord<String, Object> record : records) {
                log.info("[MANUAL-CONSUMER] key={} value={} partition={} offset={}",
                        record.key(), record.value(), record.partition(), record.offset());
            }

            // Convert the Iterable<ConsumerRecord> to a plain List of values
            return StreamSupport.stream(records.spliterator(), false)
                    .map(ConsumerRecord::value)
                    .collect(Collectors.toList());
        }
    }

    /**
     * Creates a brand-new {@link KafkaConsumer} for a single request.
     *
     * <p>We use raw {@link Properties} here (instead of Spring's consumer factory)
     * to show exactly what properties the Kafka client needs — educational intent.
     *
     * <p>Key properties:
     * <ul>
     *   <li>{@code enable.auto.commit = false} — we do not commit offsets because
     *       this is a read-only inspection endpoint, not a processing pipeline.</li>
     *   <li>{@code auto.offset.reset = earliest} — if the group has never committed,
     *       start from the very beginning of the partition.</li>
     * </ul>
     */
    private KafkaConsumer<String, Object> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, manualGroupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());

        // Trust all packages so JsonDeserializer can reconstruct MessageEvent
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        // We are inspecting messages, not processing them — no commit needed
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // If this group has never read this partition, start from the beginning
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(props);
    }
}
