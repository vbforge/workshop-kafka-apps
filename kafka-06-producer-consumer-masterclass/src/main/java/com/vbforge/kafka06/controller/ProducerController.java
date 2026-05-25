package com.vbforge.kafka06.controller;

import com.vbforge.kafka06.model.MessageEvent;
import com.vbforge.kafka06.producer.service.ProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * REST controller for Kafka producer demo endpoints.
 *
 * <p><b>Responsibility:</b> HTTP mapping only. All Kafka logic lives in {@link ProducerService}.
 * Controllers should be thin — no business logic, no KafkaTemplate usage here.
 *
 * <p>Endpoint reference:
 * <pre>
 * ┌───────────────────────────────────────────┬──────────┬──────────────────────────────────────────────┐
 * │ Endpoint                                  │ Method   │ Pattern                                      │
 * ├───────────────────────────────────────────┼──────────┼──────────────────────────────────────────────┤
 * │ /api/producer/send-sync                   │ POST     │ Synchronous — thread blocks until ACK        │
 * │ /api/producer/send-async-callback         │ POST     │ Async callback — fire-and-forget             │
 * │ /api/producer/send-async-future           │ POST     │ Async future — non-blocking HTTP response    │
 * │ /api/producer/send-with-key/{key}         │ POST     │ Keyed — partition routing + key filtering    │
 * │ /api/producer/transactional/{keys}        │ POST     │ Transactional — atomic batch                 │
 * └───────────────────────────────────────────┴──────────┴──────────────────────────────────────────────┘
 * </pre>
 */
@Slf4j
@RestController
@RequestMapping("/api/producer")
@RequiredArgsConstructor   // Lombok: generates a constructor for all final fields (replaces @Autowired)
public class ProducerController {

    private final ProducerService producerService;

    // =========================================================
    // POST /api/producer/send-sync
    // =========================================================

    /**
     * Synchronous send — the HTTP response is only returned AFTER Kafka acknowledges.
     *
     * <p>The calling thread is blocked. This gives you a strong delivery guarantee:
     * if you get a 200 OK, the message is in Kafka. If Kafka is down, you get a 500.
     *
     * @return the sent event with HTTP 200, or HTTP 500 if Kafka is unavailable
     */
    @PostMapping("/send-sync")
    public ResponseEntity<MessageEvent> sendSync() throws ExecutionException, InterruptedException {
        MessageEvent event = producerService.sendSync();
        return ResponseEntity.ok(event);
    }

    // =========================================================
    // POST /api/producer/send-async-callback
    // =========================================================

    /**
     * Async fire-and-forget — the HTTP 200 is returned immediately.
     *
     * <p>The Kafka send result is handled in a background thread callback.
     * The client does NOT know if Kafka accepted the message. Check the logs.
     *
     * @return the event (sent in background; no delivery confirmation to client)
     */
    @PostMapping("/send-async-callback")
    public ResponseEntity<MessageEvent> sendAsyncCallback() {
        MessageEvent event = producerService.sendAsync();
        return ResponseEntity.ok(event);
    }

    // =========================================================
    // POST /api/producer/send-async-future
    // =========================================================

    /**
     * Async with future — non-blocking but the HTTP response reflects the real result.
     *
     * <p>Spring MVC supports returning a {@link CompletableFuture} from a controller method.
     * It releases the Tomcat thread immediately and resumes when the future completes.
     * The HTTP client waits for the full result but the server thread is not blocked.
     *
     * @return a future that resolves to HTTP 200 on success, HTTP 500 on failure
     */
    @PostMapping("/send-async-future")
    public CompletableFuture<ResponseEntity<String>> sendAsyncFuture() {
        CompletableFuture<SendResult<String, Object>> future = producerService.sendAsyncFuture();

        return future
                .thenApply(result -> {
                    log.info("[FUTURE] Delivered to partition={} offset={}",
                            result.getRecordMetadata().partition(),
                            result.getRecordMetadata().offset());
                    return ResponseEntity.ok("Delivered — partition="
                            + result.getRecordMetadata().partition()
                            + " offset=" + result.getRecordMetadata().offset());
                })
                .exceptionally(ex -> {
                    log.error("[FUTURE] Delivery failed: {}", ex.getMessage(), ex);
                    return ResponseEntity.internalServerError()
                            .body("Delivery failed: " + ex.getMessage());
                });
    }

    // =========================================================
    // POST /api/producer/send-with-key/{key}
    // =========================================================

    /**
     * Keyed send — routes the message to a deterministic partition based on the key.
     *
     * <p>Consumer-side filtering is activated for this topic:
     * <ul>
     *   <li>key = "priority1" → consumed by {@code consumePriorityOnly}</li>
     *   <li>any other key     → consumed by {@code consumeNonPriority}</li>
     * </ul>
     *
     * @param key the Kafka message key (try "priority1" or anything else)
     * @return the sent event
     */
    @PostMapping("/send-with-key/{key}")
    public ResponseEntity<MessageEvent> sendWithKey(@PathVariable String key)
            throws ExecutionException, InterruptedException {
        MessageEvent event = producerService.sendWithKey(key);
        return ResponseEntity.ok(event);
    }

    // =========================================================
    // POST /api/producer/transactional/{keys}
    // =========================================================

    /**
     * Transactional send — all messages commit atomically or none do.
     *
     * <p>Pass comma-separated keys via the path:
     * <ul>
     *   <li>{@code /api/producer/transactional/success,success,success} → 3 messages committed</li>
     *   <li>{@code /api/producer/transactional/success,fail}            → 0 messages committed (rollback)</li>
     * </ul>
     *
     * @param keys comma-separated list of "success" or anything else (triggers rollback)
     * @return committed events (empty list if rolled back), or HTTP 500 on exception
     */
    @PostMapping("/transactional/{keys}")
    public ResponseEntity<List<MessageEvent>> sendTransactional(@PathVariable String keys) {
        List<MessageEvent> result = producerService.sendTransactional(keys);
        return ResponseEntity.ok(result);
    }
}
