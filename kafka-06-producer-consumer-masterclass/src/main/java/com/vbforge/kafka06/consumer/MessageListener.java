package com.vbforge.kafka06.consumer;

import com.vbforge.kafka06.model.MessageEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * All {@code @KafkaListener} consumer scenarios in one place.
 *
 * <p>Each listener method demonstrates a distinct real-world pattern.
 * Read each method's Javadoc — it explains both WHAT it does and WHY you would use it.
 *
 * <p>Every listener references a {@code containerFactory} bean defined in
 * {@link com.vbforge.kafka06.config.KafkaConsumerConfig}.
 */
@Slf4j
@Component
public class MessageListener {

    // =========================================================
    // SCENARIO A: Multiple consumer groups on the same topic
    // =========================================================

    /**
     * <b>Consumer group A-1</b>: standard listener on the general topic.
     *
     * <p>Consumer groups explained:
     * Each group maintains its own offset pointer per partition.
     * Two groups on the same topic both receive EVERY message — they are independent.
     * This is how you broadcast an event to multiple downstream systems
     * (e.g. billing service AND notification service both consume "order-placed").
     */
    @KafkaListener(
            topics = "${kafka.topics.general}",
            groupId = "group-general-A",
            containerFactory = "generalListenerFactory"
    )
    public void consumeGeneralGroupA(Object record) {
        log.info("[GROUP-A] Received: {}", record);
    }

    /**
     * <b>Consumer group A-2</b>: a SECOND independent group on the same topic.
     *
     * <p>Both A-1 and A-2 receive every message because they are different groups.
     * If you added a third instance to group A-1 instead, messages would be
     * split between instances (load balancing within a group).
     */
    @KafkaListener(
            topics = "${kafka.topics.general}",
            groupId = "group-general-B",
            containerFactory = "generalListenerFactory"
    )
    public void consumeGeneralGroupB(Object record) {
        log.info("[GROUP-B] Received: {}", record);
    }

    // =========================================================
    // SCENARIO B: Rich metadata extraction
    // =========================================================

    /**
     * <b>Metadata-aware listener</b>: shows how to extract Kafka headers.
     *
     * <p>Beyond the message payload, Kafka attaches metadata to every record.
     * Spring Kafka makes these available via {@code @Header(KafkaHeaders.XXX)}.
     *
     * <p>Useful in production for:
     * <ul>
     *   <li>Audit logging: "who sent this and from which partition?"</li>
     *   <li>Idempotency checks: "have I already processed offset X?"</li>
     *   <li>Debugging consumer lag</li>
     * </ul>
     *
     * @param record   the full Kafka record (key + value + metadata)
     * @param event    the deserialized {@link MessageEvent} payload
     * @param groupId  the consumer group this listener belongs to
     * @param offset   the offset of this record within its partition
     * @param partition the partition this record came from
     */
    @KafkaListener(
            topics = "${kafka.topics.general}",
            groupId = "group-metadata",
            containerFactory = "generalListenerFactory"
    )
    public void consumeWithMetadata(
            ConsumerRecord<String, MessageEvent> record,
            @Payload MessageEvent event,
            @Header(KafkaHeaders.GROUP_ID) String groupId,
            @Header(KafkaHeaders.OFFSET) long offset,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition
    ) {
        log.info("[METADATA] group={} partition={} offset={}", groupId, partition, offset);
        log.info("[METADATA] key={} event={}", record.key(), event);
    }

    // =========================================================
    // SCENARIO C: Read from beginning — specific partition + offset
    // =========================================================

    /**
     * <b>Replay from offset 0</b>: reads the entire history of partition 0.
     *
     * <p>{@code @TopicPartition} + {@code @PartitionOffset} let you pin a listener
     * to a specific partition AND starting offset. This is useful for:
     * <ul>
     *   <li>Replaying events after a bug fix</li>
     *   <li>Bootstrapping a new service from historical data</li>
     *   <li>Testing: always start from a known state</li>
     * </ul>
     *
     * <p>Note: this listener will re-read all historical records on startup.
     * In production you would control this with a specific offset, not always 0.
     */
    @KafkaListener(
            groupId = "group-replay",
            containerFactory = "generalListenerFactory",
            topicPartitions = {
                    @TopicPartition(
                            topic = "${kafka.topics.general}",
                            partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0")
                    )
            }
    )
    public void consumeFromBeginning(Object record) {
        log.info("[REPLAY] From offset 0, partition 0: {}", record);
    }

    // =========================================================
    // SCENARIO D: Message filtering by key
    // =========================================================

    /**
     * <b>Priority filter</b>: receives ONLY messages with key {@code "priority1"}.
     *
     * <p>The filtering is done in {@code priorityListenerFactory} via
     * {@code setRecordFilterStrategy}. Records that do not match are discarded
     * before this method is ever called — no if-statement needed here.
     *
     * <p>Use case: different teams own different message types. Each team's
     * consumer only sees its own messages, even though they share a topic.
     */
    @KafkaListener(
            topics = "${kafka.topics.priority}",
            containerFactory = "priorityListenerFactory"
    )
    public void consumePriorityOnly(Object record) {
        log.info("[PRIORITY-1] HIGH-PRIORITY message received: {}", record);
    }

    /**
     * <b>Non-priority filter</b>: receives every message EXCEPT {@code "priority1"}.
     * The inverse filter is configured in {@code otherPriorityListenerFactory}.
     */
    @KafkaListener(
            topics = "${kafka.topics.priority}",
            containerFactory = "otherPriorityListenerFactory"
    )
    public void consumeNonPriority(Object record) {
        log.info("[OTHER-PRIORITY] Standard message received: {}", record);
    }

    // =========================================================
    // SCENARIO E: Error handling with automatic retry
    // =========================================================

    /**
     * <b>Error handler demo</b>: always throws an exception to trigger retries.
     *
     * <p>The {@code errorHandlerListenerFactory} wraps this listener with a
     * {@link org.springframework.kafka.listener.DefaultErrorHandler} configured for
     * 2 retries with a 500 ms backoff. Watch the logs — you will see:
     * <pre>
     *   [ERROR-HANDLER] Retry #1 | ...
     *   [ERROR-HANDLER] Retry #2 | ...
     *   (after 2 retries: record is skipped and processing moves on)
     * </pre>
     *
     * <p>In production: use {@code DeadLetterPublishingRecoverer} to route
     * permanently-failed records to a DLT (covered in kafka-07).
     */
    @KafkaListener(
            topics = "${kafka.topics.priority}",
            containerFactory = "errorHandlerListenerFactory"
    )
    public void consumeWithErrorHandling(Object record) {
        log.info("[ERROR-HANDLER] Processing: {}", record);
        // Deliberately fail to demonstrate retry behaviour
        throw new RuntimeException("Simulated processing failure — watch the retry log");
    }

    // =========================================================
    // SCENARIO F: Transactional consumer (read-committed)
    // =========================================================

    /**
     * <b>Transactional consumer</b>: only sees messages from committed transactions.
     *
     * <p>The {@code transactionalListenerFactory} sets isolation level to
     * {@code read_committed}. This means:
     * <ul>
     *   <li>Messages from a completed (committed) transaction → visible here</li>
     *   <li>Messages from a rolled-back transaction → NEVER visible here</li>
     *   <li>Messages from an in-progress transaction → NOT visible until committed</li>
     * </ul>
     *
     * <p>Test with:
     * <pre>
     *   POST /api/producer/transactional/success,success   → this listener fires twice
     *   POST /api/producer/transactional/success,fail      → this listener fires ZERO times
     * </pre>
     */
    @KafkaListener(
            topics = "${kafka.topics.transactional}",
            containerFactory = "transactionalListenerFactory"
    )
    public void consumeTransactional(Object record) {
        log.info("[TRANSACTIONAL] Committed message received: {}", record);
    }
}
