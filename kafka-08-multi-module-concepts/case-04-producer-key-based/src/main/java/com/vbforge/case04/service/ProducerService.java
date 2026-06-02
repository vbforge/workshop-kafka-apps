package com.vbforge.case04.service;

import com.vbforge.case04.model.KeyedMessage;
import com.vbforge.case04.model.KeyedSendResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

// JUNIOR NOTE: This is THE central lesson of case-04.
//
// Kafka uses the message KEY to decide which partition a message goes to.
// The contract is: same key → same partition, always (as long as partition count doesn't change).
//
// Why does this matter?
//   Kafka guarantees ordering only WITHIN a partition.
//   If you have messages about the same entity (same user, same order, same sensor)
//   and they scatter across partitions, consumers can process them out of order.
//   By keying on entity ID, you guarantee all messages for that entity
//   land in the same partition and are consumed in order.
//
// Three patterns demonstrated:
//
//   Pattern 1: Keyed send with DEFAULT partitioner (murmur2 hash)
//              - Use this for most real-world cases
//              - Deterministic but opaque (you can't predict the partition without hashing)
//              - Keys spread evenly across partitions
//
//   Pattern 2: Keyed send with CUSTOM partitioner (region-based)
//              - "eu-*" → partition 0, "us-*" → partition 1, "asia-*" → partition 2
//              - Human-readable routing, useful when partition assignment has meaning
//
//   Pattern 3: Send WITHOUT a key (null key)
//              - Kafka round-robins across partitions (or uses sticky partitioning)
//              - No ordering guarantee — messages for the same entity can go anywhere
//              - Useful for pure throughput (logs, metrics) where ordering doesn't matter

@Service
@Slf4j
public class ProducerService {

    // JUNIOR NOTE: Two KafkaTemplate beans — injected by qualifier name.
    // @Qualifier must match the @Bean method name exactly.
    // Without @Qualifier, Spring would fail with "expected single matching bean but found 2".
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTemplate<String, Object> customKafkaTemplate;

    @Value("${kafka.topic.keyed}")
    private String topic;

    @Value("${kafka.producer.send-timeout-seconds}")
    private int sendTimeoutSeconds;

    public ProducerService(
            @Qualifier("kafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate,
            @Qualifier("customKafkaTemplate") KafkaTemplate<String, Object> customKafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.customKafkaTemplate = customKafkaTemplate;
    }

    //PATTERN 1: Keyed send — default partitioner (murmur2)

    // JUNIOR NOTE: kafkaTemplate.send(topic, key, value) — the key is the second argument.
    // This is the most common overload in production code.
    // The default partitioner computes: murmur2(key.getBytes()) % numPartitions.
    // Run this with the same key 10 times — you'll get the same partition every time.
    // Run it with a different key — likely a different partition (not guaranteed, depends on hash).

    public KeyedSendResult sendWithKey(String key, String content) {
        KeyedMessage message = buildMessage(key, content);

        log.info(">>> [DEFAULT-PARTITIONER] Sending key='{}' message ID: {}", key, message.getId());

        long start = System.currentTimeMillis();

        try {

            SendResult<String, Object> result = kafkaTemplate
                    .send(topic, key, message)
                    .get(sendTimeoutSeconds, TimeUnit.SECONDS);

            long duration = System.currentTimeMillis() - start;

            RecordMetadata metadata = result.getRecordMetadata();

            log.info(">>> [DEFAULT-PARTITIONER] key='{}' -> partition={} offset={}",
                    key, metadata.partition(), metadata.offset());

            return buildSendResult(key, message, metadata, duration);

        } catch (TimeoutException e) {
            throw new RuntimeException("Send timed out after " + sendTimeoutSeconds + "s", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Send interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Broker rejected message: " + e.getCause().getMessage(), e);
        }

    }


    //PATTERN 2: Keyed send — custom partitioner (region-based)

    // JUNIOR NOTE: Same send call — same API. The partitioning difference is entirely
    // in the producer config (PARTITIONER_CLASS_CONFIG = CustomPartitioner.class).
    // The service code is identical; the routing behaviour is different.
    // This is important: your service/business logic doesn't need to know or care
    // which partitioner is active. It's an infrastructure concern, not a business concern.

    public KeyedSendResult sendWithCustomPartitioner(String key, String content) {

        KeyedMessage message = buildMessage(key, content);

        log.info(">>> [CUSTOM-PARTITIONER] Sending key='{}' message ID: {}", key, message.getId());

        long start = System.currentTimeMillis();

        try{
            SendResult<String, Object> result = customKafkaTemplate
                    .send(topic, key, message)
                    .get(sendTimeoutSeconds, TimeUnit.SECONDS);

            long duration = System.currentTimeMillis() - start;

            RecordMetadata metadata = result.getRecordMetadata();

            log.info(">>> [CUSTOM-PARTITIONER] key='{}' -> partition={} offset={}", key, metadata.partition(), metadata.offset());

            return buildSendResult(key, message, metadata, duration);


        }catch (TimeoutException e){
            throw new RuntimeException("Send timed out after " + sendTimeoutSeconds + "s", e);
        }catch (InterruptedException e){
            Thread.currentThread().interrupt();
            throw new RuntimeException("Send interrupted", e);
        }catch (ExecutionException e){
            throw new RuntimeException("Broker rejected message: " + e.getCause().getMessage(), e);
        }

    }


    //PATTERN 3: Send WITHOUT a key (null key)

    // JUNIOR NOTE: kafkaTemplate.send(topic, value) — no key argument.
    // Without a key, Kafka uses "sticky partitioning" (since Kafka 2.4):
    //   - The producer picks one partition and sticks to it for a batch window
    //   - Then moves to the next partition for the next batch
    // Before 2.4, it was pure round-robin per message.
    //
    // The observable effect: messages WITHOUT a key spread across all partitions.
    // Run this endpoint 6 times — you'll see the partition value vary (0, 1, 2, ...).
    //
    // When to use null-key sends:
    //   - You genuinely don't care about ordering (metrics, debug logs, heartbeats)
    //   - You want maximum throughput spread across all partitions
    //
    // When NOT to use null-key sends:
    //   - Anything where messages for the same entity must be processed in order
    //   - Transactional data (orders, payments, user profile changes)

    public KeyedSendResult sendWithoutKey(String content) {

        KeyedMessage message = buildMessage(null, content);

        log.info(">>> [NO-KEY] Sending keyless message ID: {} - partition will be sticky/round-robin", message.getId());

        long start = System.currentTimeMillis();

        try{
            // JUNIOR NOTE: send(topic, value) — two-argument overload, no key.
            // This is equivalent to send(topic, null, value) under the hood.
            SendResult<String, Object> result = kafkaTemplate
                    .send(topic, message)
                    .get(sendTimeoutSeconds, TimeUnit.SECONDS);

            long duration = System.currentTimeMillis() - start;

            RecordMetadata meta = result.getRecordMetadata();

            log.info(">>> [NO-KEY] Keyless message -> partition={} offset={}", meta.partition(), meta.offset());

            return buildSendResult(null, message, meta, duration);

        }catch (TimeoutException e) {
            throw new RuntimeException("Send timed out after " + sendTimeoutSeconds + "s", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Send interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Broker rejected message: " + e.getCause().getMessage(), e);
        }

    }


    //helper methods

    private KeyedMessage buildMessage(String entityKey, String content) {
        if (content == null || content.isBlank()) {
            content = "Message from case-04 (no content provided)";
        }
        return KeyedMessage.builder()
                .id(UUID.randomUUID().toString())
                .entityKey(entityKey != null ? entityKey : "null-key")
                .content(content)
                .timestamp(LocalDateTime.now())
                .build();
    }

    private KeyedSendResult buildSendResult(
            String key, KeyedMessage message, RecordMetadata meta, long durationMs) {

        return KeyedSendResult.builder()
                .messageId(message.getId())
                .key(key != null ? key : "null")
                .content(message.getContent())
                .partition(meta.partition())
                .offset(meta.offset())
                .brokerTimestamp(meta.timestamp())
                .sendDurationMs(durationMs)
                .respondedAt(LocalDateTime.now())
                .build();
    }


}


















