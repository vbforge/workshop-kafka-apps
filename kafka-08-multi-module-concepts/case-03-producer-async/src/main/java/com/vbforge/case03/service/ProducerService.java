package com.vbforge.case03.service;

import com.vbforge.case03.model.AsyncSendReceipt;
import com.vbforge.case03.model.MyMessageObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

// JUNIOR NOTE: This is THE central lesson of case-03.
//
// case-01 used kafkaTemplate.send() — fire-and-forget. No visibility into success or failure.
// case-02 used kafkaTemplate.send().get() — blocked the thread until broker confirmed.
// case-03 uses kafkaTemplate.send() + callbacks / CompletableFuture chaining.
//
// The key insight: async doesn't mean "ignore failures".
// It means: "don't block the calling thread — but STILL handle success and failure,
// just in a callback that runs later on a different thread."
//
// Three patterns demonstrated:
//
//   Pattern 1: whenComplete() callback — most common in production.
//              The callback fires (on a different thread) when the broker ACKs.
//              HTTP response goes back immediately.
//
//   Pattern 2: thenAccept() + exceptionally() — split success and failure handlers.
//              Cleaner separation of concerns when the two paths have different logic.
//
//   Pattern 3: sendAndForget() — truly fire-and-forget with minimal logging.
//              Appropriate for non-critical messages where loss is acceptable
//              (metrics, telemetry, heartbeats). Add a no-op callback anyway
//              so you at least see failures in logs.
//
// Thread model to understand:
//   - The calling thread (HTTP request thread) runs sendFireAndCallback(), builds the
//     AsyncSendReceipt, and returns HTTP 202 — all before the broker ACKs.
//   - The Kafka producer's I/O thread runs the callback/whenComplete handler later.
//   - These are TWO DIFFERENT threads. Never share mutable state between them without
//     proper synchronization. In our case we just log, so there's no shared state issue.

@Service
@Slf4j
@RequiredArgsConstructor
public class ProducerService {

    public static final String ACCEPTED = "ACCEPTED";
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topic.async}")
    private String topic;

    @Value("${kafka.default.message}")
    private String defaultMessage;


    // ===================================================================
    // SEND PATTERN 1: whenComplete() callback — recommended production pattern
    // ===================================================================

    // JUNIOR NOTE: whenComplete(result, throwable) is the most natural async pattern.
    // Both success and failure arrive in the same lambda.
    // When the broker ACKs → result is non-null, throwable is null.
    // When the send fails   → result is null, throwable contains the exception.
    //
    // This is the pattern you'll see most in real codebases.
    // The HTTP response (AsyncSendReceipt) is returned immediately — before this
    // callback fires. That's the whole point of async.
    public AsyncSendReceipt sendFireAndCallback(String content){
        MyMessageObject message = buildMessage(content);
        log.info(">>> [CALLBACK] Submitting message ID: {} - returning immediately", message.getId());

        kafkaTemplate.send(topic, message.getId(), message)
                .whenComplete((result, throwable) -> {
                    // JUNIOR NOTE: This lambda runs on the Kafka I/O thread, not the HTTP thread.
                    // The HTTP response has already been sent by the time this runs.
                    if(throwable != null){
                        log.error(">>> [CALLBACK] FAILED to deliver message ID: {} | error: {}",
                                message.getId(), throwable.getMessage());
                    } else {
                        RecordMetadata metadata = result.getRecordMetadata();
                        log.info(">>> [CALLBACK] Delivered message ID: {} | partition={} offset={}",
                                message.getId(), metadata.partition(), metadata.offset());
                    }
                });

        // We return before the broker ACKs. The receipt says "ACCEPTED", not "CONFIRMED".
        return buildAsyncSendReceipt(message);

    }


    // ===================================================================
    // SEND PATTERN 2: thenAccept() + exceptionally() — split handlers
    // ===================================================================

    // JUNIOR NOTE: Sometimes success and failure handling have completely different logic —
    // e.g. success triggers a downstream notification, failure triggers an alert + dead-letter.
    // Splitting them into thenAccept and exceptionally keeps each path clean.
    //
    // thenAccept(consumer)  → runs ONLY on success, receives the SendResult
    // exceptionally(fn)     → runs ONLY on failure, receives the Throwable, must return a value
    //
    // Important: exceptionally() returns a CompletableFuture<T> where T must match the
    // original type. Since we're in a void context (thenAccept returns CompletableFuture<Void>),
    // exceptionally here returns null — that's fine, we just need it for the side-effect (logging).
    public AsyncSendReceipt sendWithSplitHandlers(String content) {

        MyMessageObject message = buildMessage(content);
        log.info(">>> [SPLIT] Submitting message ID: {} - returning immediately", message.getId());

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(topic, message.getId(), message);

        future.thenAccept(result -> {
            // JUNIOR NOTE: Runs on success only. result is guaranteed non-null here.
            RecordMetadata meta = result.getRecordMetadata();
            log.info(">>> [SPLIT] SUCCESS: message ID: {} | partition={} offset={} brokerTs={}",
                    message.getId(), meta.partition(), meta.offset(), meta.timestamp());
            // In a real app this is where you'd:
            //   - emit a domain event ("OrderConfirmedEvent")
            //   - update a database status ("PUBLISHED")
            //   - send a notification
        });

        future.exceptionally(throwable -> {
            // JUNIOR NOTE: Runs on failure only. throwable is guaranteed non-null here.
            // Note the return null — exceptionally requires a return value to complete the future.
            // We're using it purely for the side effect (logging/alerting).
            log.error(">>> [SPLIT] FAILURE: message ID: {} | error: {}",
                    message.getId(), throwable.getMessage());
            // In a real app this is where you'd:
            //   - publish to a dead-letter topic
            //   - trigger an alert (PagerDuty, Slack, etc.)
            //   - record the failure in a retry table
            return null;
        });

        return buildAsyncSendReceipt(message);


    }


    // ===================================================================
    // SEND PATTERN 3: Fire-and-forget with minimal logging
    // ===================================================================

    // JUNIOR NOTE: "Fire-and-forget" has a spectrum.
    //
    // The purest form is calling kafkaTemplate.send() and literally ignoring the returned Future.
    // That works but you'll never see failures — they'll be completely silent.
    //
    // A better approach: attach a minimal error-only callback so failures still surface in logs,
    // but you don't care about success confirmations. This is appropriate for:
    //   - Application metrics (counters, timings)
    //   - Heartbeat events
    //   - Debug-level telemetry
    //   - Anything where "best-effort delivery" is explicitly acceptable per business requirements
    //
    // You'd never use this for orders, payments, or anything with a financial audit trail.

    public AsyncSendReceipt sendFireAndForget(String content) {

        MyMessageObject message = buildMessage(content);
        log.info(">>> [FIRE-FORGET] Submitting message ID: {} - minimal callback", message.getId());

        kafkaTemplate.send(topic, message.getId(), message)
                .exceptionally(throwable -> {
                    // JUNIOR NOTE: We only care about errors. No success logging.
                    // This keeps the I/O thread's workload minimal.
                    log.error(">>> [FIRE-FORGET] Message ID: {} FAILED silently: {}",
                            message.getId(), throwable.getMessage());
                    return null;
                });

        return buildAsyncSendReceipt(message);

    }



    // helper methods

    private MyMessageObject buildMessage(String content) {
        if(content == null || content.isBlank()) {
            content = defaultMessage;
        }
        return MyMessageObject.builder()
                .id(UUID.randomUUID().toString())
                .content(content)
                .timestamp(LocalDateTime.now())
                .build();
    }

    private AsyncSendReceipt buildAsyncSendReceipt(MyMessageObject message) {

        return AsyncSendReceipt.builder()
                .messageId(message.getId())
                .content(message.getContent())
                .acceptedAt(LocalDateTime.now())
                .status(ACCEPTED)
                .build();

    }


}




















