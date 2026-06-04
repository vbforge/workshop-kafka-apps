package com.vbforge.case10.service;

import com.vbforge.case10.exception.NonRetryableException;
import com.vbforge.case10.model.TaskMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

// JUNIOR NOTE: This is THE central lesson of case-10.
//
// The deliveryAttempts map tracks how many times each message ID has been
// delivered to this listener. It enables the "eventually-succeed" pattern:
// fail on attempts 1 and 2, succeed on attempt 3.
//
// This simulates the most common production retry scenario:
//   - A downstream service (DB, external API) is briefly unavailable
//   - First 1-2 delivery attempts hit the outage window
//   - Exponential backoff gives the service time to recover
//   - Attempt 3 arrives after the service is back → succeeds
//
// The three failure modes:
//
//   "always-fail"       → throws TransientProcessingException on every attempt
//                         → retries until maxElapsedTime exhausted → recovery
//                         → demonstrates the retry budget ceiling
//
//   "eventually-succeed"→ fails until deliveryAttempts[id] == succeedOnAttempt
//                         → demonstrates that backoff gives transient conditions
//                           time to resolve → SUCCESS before recovery runs
//
//   "non-retryable"     → throws NonRetryableException immediately
//                         → no retries, straight to recovery
//                         → compare log timing: much faster than always-fail

@Service
@Slf4j
public class RetryConsumerService {

    // JUNIOR NOTE: ConcurrentHashMap because the retry listener and the consumer
    // both touch this map (conceptually). In practice Spring calls the listener on
    // the same thread, but defensive concurrency is a good habit.
    private final ConcurrentHashMap<String, AtomicInteger> deliveryAttempts = new ConcurrentHashMap<>();

    private final AtomicInteger successCount      = new AtomicInteger(0);
    private final AtomicInteger exhaustedCount    = new AtomicInteger(0);
    private final AtomicInteger nonRetryableCount = new AtomicInteger(0);

    @KafkaListener(
            topics = "${kafka.topic.name}",
            groupId = "${kafka.consumer.groupID}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, TaskMessage> record) {
        TaskMessage msg = record.value();

        // Track delivery attempts per message ID
        int attempt = deliveryAttempts
                .computeIfAbsent(msg.getId(), k -> new AtomicInteger(0))
                .incrementAndGet();

        log.info(">>> [CONSUMER] attempt={} partition={} offset={} id={} failureMode={}",
                attempt, record.partition(), record.offset(), msg.getId(), msg.getFailureMode());

        switch (msg.getFailureMode()) {

            case "always-fail" -> {
                // JUNIOR NOTE: Always throws. The error handler will retry with exponential
                // backoff until maxElapsedTime is exhausted, then run recovery.
                // Watch the timestamps between retry log lines — you'll see:
                //   attempt 1 → 500ms gap → attempt 2 → 1000ms gap → attempt 3 → 2000ms gap...
                log.warn(">>> [CONSUMER] attempt={} FAILING (always-fail) id={}", attempt, msg.getId());
                throw new RuntimeException("Simulated persistent transient failure — attempt " + attempt);
            }

            case "eventually-succeed" -> {
                int target = msg.getSucceedOnAttempt();
                if (target > 0 && attempt >= target) {
                    // JUNIOR NOTE: We've hit the target attempt — succeed now.
                    // The backoff delays gave the "downstream service" time to recover.
                    // This is the happy path of the retry pattern.
                    successCount.incrementAndGet();
                    log.info(">>> [CONSUMER] OK SUCCEEDED on attempt={} (target={}) id={}",
                            attempt, target, msg.getId());
                } else {
                    // Fail this attempt — backoff will kick in before next attempt
                    log.warn(">>> [CONSUMER] attempt={}/{} FAILING (eventually-succeed) id={}",
                            attempt, target, msg.getId());
                    throw new RuntimeException(
                            "Transient failure on attempt " + attempt + ", will succeed on " + target);
                }
            }

            case "non-retryable" -> {
                nonRetryableCount.incrementAndGet();
                log.warn(">>> [CONSUMER] attempt={} NON-RETRYABLE id={}", attempt, msg.getId());
                throw new NonRetryableException(
                        "Permanent failure — no retries — id=" + msg.getId());
            }

            default -> {
                successCount.incrementAndGet();
                log.info(">>> [CONSUMER] OK Processed (default) attempt={} id={}", attempt, msg.getId());
            }
        }
    }

    public int getSuccessCount()      { return successCount.get(); }
    public int getExhaustedCount()    { return exhaustedCount.get(); }
    public int getNonRetryableCount() { return nonRetryableCount.get(); }
    public ConcurrentHashMap<String, AtomicInteger> getDeliveryAttempts() { return deliveryAttempts; }

}
