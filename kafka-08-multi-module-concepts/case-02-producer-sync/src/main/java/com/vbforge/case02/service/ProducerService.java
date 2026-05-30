package com.vbforge.case02.service;

//import com.vbforge.case02.model.SendResult;
import com.vbforge.case02.model.MyMessageObject;
import com.vbforge.case02.model.SendResultMetadata;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

// JUNIOR NOTE: This is THE central lesson of case-02.
//
// In case-01, we called kafkaTemplate.send(...) and returned immediately.
// The send happened in the background — we had no idea if Kafka actually accepted it.
// That's async / fire-and-forget.
//
// In case-02, we call kafkaTemplate.send(...).get() — we BLOCK the calling thread
// until the Kafka broker either:
//   a) confirms the message was written to the partition log  → we get RecordMetadata
//   b) times out waiting for that confirmation                → TimeoutException
//   c) broker refuses or fails                               → ExecutionException
//
// The trade-off is always the same:
//   Sync  → guaranteed ordering guarantee, broker-confirmed delivery, BUT slower (waits per message)
//   Async → higher throughput, lower latency, BUT "best effort" — you must handle failures separately
//
// When to use sync in production:
//   - Financial transactions, audit trails, order confirmations
//   - Anything where "I need to know it's committed before I proceed"
//
// When NOT to use sync:
//   - High-throughput pipelines (telemetry, logs, events) — the blocking kills your throughput

@Service
@Slf4j
@RequiredArgsConstructor
public class ProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topic.sync}")
    private String topic;

    @Value("${kafka.default.message}")
    private String defaultMessage;

    @Value("${kafka.producer.send-timeout-seconds}")
    private int sendTimeoutSeconds;


    // ===================================================================
    // SEND METHOD 1: Basic blocking send — .get() with no timeout
    // ===================================================================

    // JUNIOR NOTE: .get() with no timeout arguments blocks INDEFINITELY.
    // This is dangerous in production — if Kafka is slow or down, your
    // HTTP request thread hangs forever (or until the server kills it).
    // We expose this in the API so you can see what "naked blocking" looks like.
    // In reality: NEVER use this in production. Always pass a timeout.

    public SendResultMetadata sendBlocking(String content) {
        MyMessageObject message = buildMessage(content);
        log.info(">>> [BLOCKING] Sending message ID: {} — thread will block until ACK", message.getId());

        long start = System.currentTimeMillis();
        try {
            // .get() — no timeout. Thread blocks here until broker responds.
            SendResult<String, Object> result = kafkaTemplate
                    .send(topic, message.getId(), message)
                    .get(); // ← THE BLOCKING CALL

            long duration = System.currentTimeMillis() - start;
            RecordMetadata meta = result.getRecordMetadata();

            log.info(">>> [BLOCKING] ACK received in {}ms | partition={} offset={}",
                    duration, meta.partition(), meta.offset());

            return buildSendResult(message, meta, duration);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // restore interrupt flag — always do this
            throw new RuntimeException("Send interrupted while waiting for broker ACK", e);
        } catch (ExecutionException e) {
            // ExecutionException wraps the actual Kafka error
            // Unwrap it to get the real cause (e.g. TopicAuthorizationException, NetworkException)
            throw new RuntimeException("Broker rejected the message: " + e.getCause().getMessage(), e);
        }
    }


    // ===================================================================
    // SEND METHOD 2: Blocking send WITH timeout — the production-safe pattern
    // ===================================================================

    // JUNIOR NOTE: .get(timeout, unit) is the correct production pattern.
    // If the broker doesn't ACK within the timeout → TimeoutException is thrown.
    // Your application can then decide: retry? alert? return 503?
    // The timeout here is YOUR application's deadline — separate from Kafka's
    // internal REQUEST_TIMEOUT_MS_CONFIG (which is the broker's response window).

    public SendResultMetadata sendWithTimeout(String content) {
        MyMessageObject message = buildMessage(content);
        log.info(">>> [TIMEOUT] Sending message ID: {} — will wait max {}s for ACK",
                message.getId(), sendTimeoutSeconds);

        long start = System.currentTimeMillis();
        try {
            SendResult<String, Object> result = kafkaTemplate
                    .send(topic, message.getId(), message)
                    .get(sendTimeoutSeconds, TimeUnit.SECONDS); // ← BOUNDED blocking

            long duration = System.currentTimeMillis() - start;
            RecordMetadata meta = result.getRecordMetadata();

            log.info(">>> [TIMEOUT] ACK received in {}ms | partition={} offset={}",
                    duration, meta.partition(), meta.offset());

            return buildSendResult(message, meta, duration);

        } catch (TimeoutException e) {
            // JUNIOR NOTE: TimeoutException here is java.util.concurrent.TimeoutException,
            // NOT Kafka's own TimeoutException. The Future's .get() threw this because
            // the broker didn't respond within sendTimeoutSeconds.
            // At this point the message MAY or MAY NOT have been written to Kafka —
            // we simply don't know. This is a key operational concern in sync sends.
            long duration = System.currentTimeMillis() - start;
            log.error(">>> [TIMEOUT] No ACK after {}ms — broker too slow or unreachable", duration);
            throw new RuntimeException(
                    "Kafka send timed out after " + sendTimeoutSeconds + "s — broker did not ACK", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Send interrupted while waiting for broker ACK", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Broker rejected the message: " + e.getCause().getMessage(), e);
        }
    }


    // ===================================================================
    // SEND METHOD 3: Sync send with custom timeout passed by the caller
    // ===================================================================

    // JUNIOR NOTE: Sometimes callers want different timeouts for different
    // message priorities. A critical payment message might wait 10s,
    // a low-priority notification only 2s. This overload gives that flexibility
    // without duplicating the whole send logic.

    public SendResultMetadata sendWithCustomTimeout(String content, int timeoutSeconds) {
        MyMessageObject message = buildMessage(content);
        log.info(">>> [CUSTOM-TIMEOUT] Sending message ID: {} — custom timeout {}s",
                message.getId(), timeoutSeconds);

        long start = System.currentTimeMillis();
        try {
            SendResult<String, Object> result = kafkaTemplate
                    .send(topic, message.getId(), message)
                    .get(timeoutSeconds, TimeUnit.SECONDS);

            long duration = System.currentTimeMillis() - start;
            RecordMetadata meta = result.getRecordMetadata();

            log.info(">>> [CUSTOM-TIMEOUT] ACK received in {}ms | partition={} offset={}",
                    duration, meta.partition(), meta.offset());

            return buildSendResult(message, meta, duration);

        } catch (TimeoutException e) {
            long duration = System.currentTimeMillis() - start;
            log.error(">>> [CUSTOM-TIMEOUT] No ACK after {}ms (custom timeout={}s)", duration, timeoutSeconds);
            throw new RuntimeException(
                    "Kafka send timed out after " + timeoutSeconds + "s", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Send interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Broker error: " + e.getCause().getMessage(), e);
        }
    }


    // ===================================================================
    // HELPERS
    // ===================================================================

    private MyMessageObject buildMessage(String content) {
        if (content == null || content.isBlank()) {
            content = defaultMessage;
        }
        return MyMessageObject.builder()
                .id(UUID.randomUUID().toString())
                .content(content)
                .timestamp(LocalDateTime.now())
                .build();
    }

    private SendResultMetadata buildSendResult(
            MyMessageObject message, RecordMetadata meta, long durationMs) {

        return SendResultMetadata.builder()
                .message(message)
                .partition(meta.partition())
                .offset(meta.offset())
                .brokerTimestamp(meta.timestamp())
                .sendDurationMs(durationMs)
                .respondedAt(LocalDateTime.now())
                .build();
    }

}
