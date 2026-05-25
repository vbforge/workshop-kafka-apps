package com.vbforge.kafka06.producer.service;

import com.vbforge.kafka06.model.MessageEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Service layer for all Kafka producer patterns.
 *
 * <p>This class owns the "how to send" logic. Controllers delegate to it — they
 * should not know about KafkaTemplate directly. This separation makes the code
 * easier to test and easier to extend.
 *
 * <p>Patterns covered here:
 * <ol>
 *   <li>Synchronous send     — blocks until Kafka acknowledges</li>
 *   <li>Async send (callback) — fire-and-forget with a completion callback</li>
 *   <li>Async send (Future)  — non-blocking, caller can await the result</li>
 *   <li>Keyed send           — attaches a key so Kafka routes to a stable partition</li>
 *   <li>Transactional send   — atomic batch: all messages commit or none do</li>
 * </ol>
 */
@Slf4j
@Service
public class ProducerService {

    // ---- Topic names from application.yml ----
    @Value("${kafka.topics.general}")
    private String generalTopic;

    @Value("${kafka.topics.priority}")
    private String priorityTopic;

    @Value("${kafka.topics.transactional}")
    private String transactionalTopic;

    /**
     * Standard (non-transactional) template.
     * Used for sync, async, and keyed sends.
     */
    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Transactional template — requires a {@code TRANSACTIONAL_ID_CONFIG} in its factory.
     * {@code @Qualifier} disambiguates when Spring finds multiple KafkaTemplate beans.
     */
    private final KafkaTemplate<String, Object> transactionalKafkaTemplate;

    public ProducerService(
            @Qualifier("kafkaTemplate") KafkaTemplate<String, Object> kafkaTemplate,
            @Qualifier("transactionalKafkaTemplate") KafkaTemplate<String, Object> transactionalKafkaTemplate
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.transactionalKafkaTemplate = transactionalKafkaTemplate;
    }

    // =========================================================
    // PATTERN 1: Synchronous send (blocking)
    // =========================================================

    /**
     * Sends a message and WAITS (blocks the calling thread) until Kafka
     * writes the record to a partition leader and returns the offset.
     *
     * <p>Use case: you need guaranteed delivery confirmation before
     * responding to the client (e.g. order placement).
     *
     * <p>Trade-off: throughput is lower because each send waits for the broker.
     *
     * @return the event that was sent (with partition + offset logged)
     * @throws ExecutionException   if Kafka reports a send failure
     * @throws InterruptedException if the waiting thread is interrupted
     */
    public MessageEvent sendSync() throws ExecutionException, InterruptedException {
        MessageEvent event = buildEvent("sync", "Sent synchronously — thread blocked until broker acknowledged");

        // kafkaTemplate.send() always returns a CompletableFuture.
        // Calling .get() on it blocks until Kafka responds.
        SendResult<String, Object> result = kafkaTemplate.send(generalTopic, event).get();

        log.info("[SYNC] topic={} partition={} offset={}",
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());

        return event;
    }

    // =========================================================
    // PATTERN 2: Asynchronous send — fire-and-forget with callback
    // =========================================================

    /**
     * Sends a message WITHOUT waiting for Kafka's acknowledgement.
     * The caller gets the response immediately; Kafka delivery is handled
     * in a background thread via {@code whenComplete}.
     *
     * <p>Use case: high-throughput pipelines where losing an occasional
     * message is acceptable, or where the caller should not be slowed down
     * by broker latency.
     *
     * <p>Trade-off: the client receives a 200 OK even if Kafka later rejects the message.
     *
     * @return the event (sent fire-and-forget; no delivery guarantee from this method)
     */
    public MessageEvent sendAsync() {
        MessageEvent event = buildEvent("async-callback", "Sent asynchronously — broker result handled in background callback");

        kafkaTemplate.send(generalTopic, event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("[ASYNC-CALLBACK] OK — partition={} offset={}",
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("[ASYNC-CALLBACK] FAILED — {}", ex.getMessage(), ex);
                    }
                });

        return event;
    }

    // =========================================================
    // PATTERN 3: Asynchronous send — returns CompletableFuture
    // =========================================================

    /**
     * Sends a message and returns the {@link CompletableFuture} directly to the caller.
     * The REST controller chains further transformations onto it before returning
     * to the HTTP client — so the HTTP response reflects the actual Kafka result
     * without blocking the Tomcat thread.
     *
     * <p>Use case: reactive / non-blocking REST APIs where you want the HTTP response
     * to encode success/failure but still do not want to block a thread.
     *
     * @return a future that resolves to the {@link SendResult} once Kafka acknowledges
     */
    public CompletableFuture<SendResult<String, Object>> sendAsyncFuture() {
        MessageEvent event = buildEvent("async-future", "Sent asynchronously — CompletableFuture returned to controller");
        return kafkaTemplate.send(generalTopic, event);
    }

    // =========================================================
    // PATTERN 4: Keyed send — deterministic partition routing
    // =========================================================

    /**
     * Sends a message to the priority topic with an explicit {@code key}.
     *
     * <p>How Kafka uses the key:
     * Kafka hashes the key and maps the hash to a partition number.
     * All messages with the same key always land on the same partition.
     * This guarantees ordering within a key — critical for event sourcing.
     *
     * <p>In this demo the key also drives consumer-side filtering:
     * <ul>
     *   <li>key = "priority1" → consumed by {@code priorityListenerFactory}</li>
     *   <li>any other key     → consumed by {@code otherPriorityListenerFactory}</li>
     * </ul>
     *
     * @param key the routing key (e.g. "priority1", "normal", "batch-job")
     * @return the event that was sent
     */
    public MessageEvent sendWithKey(String key) throws ExecutionException, InterruptedException {
        String payload = "priority1".equals(key)
                ? "HIGH-PRIORITY message — routed to priority consumer"
                : "Standard message — routed to general consumer";

        MessageEvent event = buildEvent(key, payload);

        SendResult<String, Object> result = kafkaTemplate.send(priorityTopic, key, event).get();

        log.info("[KEYED] key={} partition={} offset={}",
                key,
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());

        return event;
    }

    // =========================================================
    // PATTERN 5: Transactional send — exactly-once semantics
    // =========================================================

    /**
     * Sends multiple messages atomically inside a Kafka transaction.
     *
     * <p>What "transactional" means here:
     * All messages are buffered until {@code executeInTransaction} completes without exception.
     * If ANY message fails (or an exception is thrown), the entire batch is rolled back —
     * consumers configured with {@code read_committed} will never see those messages.
     *
     * <p>Simulated with the {@code keys} list:
     * <ul>
     *   <li>"success" → message is queued for the transaction</li>
     *   <li>anything else → throws {@link RuntimeException}, rolling back all previous messages</li>
     * </ul>
     *
     * <p>Try it:
     * <pre>
     *   POST /api/producer/transactional/success,success,success  → all 3 committed
     *   POST /api/producer/transactional/success,fail             → nothing committed
     * </pre>
     *
     * @param keys comma-separated list of "success" or any other string (causes rollback)
     * @return the events that were committed (empty if rolled back)
     */
    public List<MessageEvent> sendTransactional(String keys) {
        List<MessageEvent> committed = new ArrayList<>();

        transactionalKafkaTemplate.executeInTransaction(ops -> {
            for (String key : keys.split(",")) {
                String trimmed = key.trim();

                if (!"success".equals(trimmed)) {
                    // This exception causes executeInTransaction to roll back everything sent so far
                    log.error("[TRANSACTIONAL] Encountered key='{}' — rolling back entire transaction", trimmed);
                    throw new RuntimeException("Transactional rollback triggered by key: " + trimmed);
                }

                MessageEvent event = buildEvent(trimmed, "Transactional message — part of an atomic batch");
                ops.send(transactionalTopic, trimmed, event);
                committed.add(event);

                log.info("[TRANSACTIONAL] Queued message with key='{}'", trimmed);

                // Small delay so you can observe "in-flight" messages in Conduktor / logs
                sleep(300);
            }
            return null; // return type is ignored; we use `committed` list instead
        });

        log.info("[TRANSACTIONAL] Committed {} message(s)", committed.size());
        return committed;
    }

    // =========================================================
    // Private helpers
    // =========================================================

    /**
     * Factory method: creates a {@link MessageEvent} with the current timestamp.
     * Centralising this avoids repeating {@code LocalDateTime.now()} everywhere.
     */
    private MessageEvent buildEvent(String category, String payload) {
        return MessageEvent.builder()
                .category(category)
                .createdAt(LocalDateTime.now())
                .payload(payload)
                .build();
    }

    /**
     * Wraps {@code Thread.sleep()} and handles {@link InterruptedException} cleanly.
     * Always restore the interrupt flag when catching InterruptedException.
     */
    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // restore the interrupt flag
        }
    }
}
