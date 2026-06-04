package com.vbforge.case09.service;

import com.vbforge.case09.exception.FatalProcessingException;
import com.vbforge.case09.exception.TransientProcessingException;
import com.vbforge.case09.model.TaskMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

// JUNIOR NOTE: This is THE central lesson of case-09.
//
// The consumer deliberately throws different exception types based on the message's
// failureMode field. This makes error handler behaviour directly observable:
//
//   failureMode="none"      → happy path, processes cleanly
//   failureMode="transient" → throws TransientProcessingException
//                             → DefaultErrorHandler retries up to maxAttempts times
//                             → you see "Retry attempt #1", "#2" in logs
//                             → after maxAttempts exhausted: logs "recovery" and commits past it
//
//   failureMode="fatal"     → throws FatalProcessingException (registered as non-retryable)
//                             → DefaultErrorHandler skips retries IMMEDIATELY
//                             → you see only ONE attempt in logs, then straight to recovery
//                             → this is faster and correct — retrying won't fix bad data
//
//   failureMode="npe"       → throws NullPointerException (also registered as non-retryable)
//                             → same behaviour as fatal — no retries
//
// The key observable difference between "transient" and "fatal":
//   transient: logs show attempt #1, #2, #3 → then recovery
//   fatal/npe: logs show attempt #1 → then immediately recovery (no #2 or #3)
//
// After ANY failure path, the consumer continues to the NEXT message.
// The partition is NOT blocked — this is the fundamental advantage of DefaultErrorHandler
// over old SeekToCurrentErrorHandler. Processing moves forward.

@Service
@Slf4j
public class TaskConsumerService {

    private final AtomicInteger successCount  = new AtomicInteger(0);
    private final AtomicInteger retryCount    = new AtomicInteger(0);
    private final AtomicInteger recoveredCount = new AtomicInteger(0);

    @KafkaListener(
            topics = "${kafka.topic.name}",
            groupId = "${kafka.consumer.groupID}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, TaskMessage> record) {
        TaskMessage msg = record.value();
        log.info(">>> [CONSUMER] Received partition={} offset={} id={} failureMode={}",
                record.partition(), record.offset(), msg.getId(), msg.getFailureMode());

        switch (msg.getFailureMode()) {

            case "none" -> {
                successCount.incrementAndGet();
                log.info(">>> [CONSUMER] ✓ Processed successfully | id={} content={}",
                        msg.getId(), msg.getContent());
            }

            case "transient" -> {
                retryCount.incrementAndGet();
                // JUNIOR NOTE: TransientProcessingException is NOT in the non-retryable list.
                // DefaultErrorHandler will retry this up to (maxAttempts - 1) more times.
                // Watch the retry listener log: "Retry attempt #1", "#2" etc.
                // After all retries exhausted, the handler logs recovery and commits past it.
                log.warn(">>> [CONSUMER] ✗ Throwing TransientProcessingException for id={}", msg.getId());
                throw new TransientProcessingException(
                        "Simulated transient failure for message id=" + msg.getId());
            }

            case "fatal" -> {
                recoveredCount.incrementAndGet();
                // JUNIOR NOTE: FatalProcessingException IS in the non-retryable list.
                // DefaultErrorHandler will NOT retry — it goes straight to recovery.
                // Watch the logs: no "Retry attempt" lines, just immediate recovery.
                log.warn(">>> [CONSUMER] ✗ Throwing FatalProcessingException for id={}", msg.getId());
                throw new FatalProcessingException(
                        "Simulated fatal failure for message id=" + msg.getId());
            }

            case "npe" -> {
                recoveredCount.incrementAndGet();
                // JUNIOR NOTE: NullPointerException is also non-retryable in our config.
                // This demonstrates that you can classify JVM-level exceptions too,
                // not just your own custom exceptions.
                log.warn(">>> [CONSUMER] ✗ Throwing NullPointerException for id={}", msg.getId());
                throw new NullPointerException(
                        "Simulated NPE for message id=" + msg.getId());
            }

            default -> {
                log.warn(">>> [CONSUMER] Unknown failureMode='{}' — treating as success", msg.getFailureMode());
                successCount.incrementAndGet();
            }
        }
    }

    public int getSuccessCount()   { return successCount.get(); }
    public int getRetryCount()     { return retryCount.get(); }
    public int getRecoveredCount() { return recoveredCount.get(); }

}
