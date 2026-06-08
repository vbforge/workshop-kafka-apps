package com.vbforge.case13.service;

import com.vbforge.case13.model.OrderMessage;
import com.vbforge.case13.model.ProducerResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

// JUNIOR NOTE: This service is the centrepiece of case-13.
//
// Spring Kafka 4.x BREAKING CHANGE vs earlier versions:
//   In Spring Kafka 3.x, @Transactional(transactionManager="kafkaTransactionManager")
//   would automatically start a Kafka transaction from ANY calling thread — including
//   an HTTP request thread (Tomcat).
//
//   In Spring Kafka 4.x, that implicit wiring no longer works from non-Kafka threads.
//   @Transactional only activates Kafka transactions automatically when the method is
//   called from INSIDE a Kafka listener container thread (i.e. triggered by @KafkaListener).
//   From an HTTP thread it throws:
//     IllegalStateException: No transaction is in process
//
// The fix: use kafkaTemplate.executeInTransaction() — the explicit, always-works API.
//
//   kafkaTemplate.executeInTransaction(ops -> {
//       ops.send(...);   // all sends here are inside ONE Kafka transaction
//       ops.send(...);
//       return result;   // normal return → commit
//                        // thrown exception → abort
//   });
//
// This API is the correct approach for HTTP-triggered Kafka transactions in Spring Kafka 4.x.
// It is also the explicit, readable way to wrap sends regardless of framework version —
// zero ambiguity about whether a transaction is active.
//
// KafkaTransactionManager is still needed in KafkaConfig — it is used by the listener
// container when @KafkaListener methods call kafkaTemplate.send() inside a consumer
// transaction. We keep it there for the full picture.

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionalProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topic.orders}")
    private String ordersTopic;

    @Value("${kafka.topic.processed}")
    private String processedTopic;

    @Value("${kafka.topic.probe}")
    private String probeTopic;


    // ===================================================================
    // COMMITTED TRANSACTION — both sends visible to consumers
    // ===================================================================

    public ProducerResponse sendCommitted(double amount) {
        OrderMessage order = buildOrder(amount, false);
        log.info(">>> [TX-PRODUCER] BEGIN transaction orderId={}", order.getOrderId());

        // JUNIOR NOTE: executeInTransaction() is the Spring Kafka 4.x way to wrap
        // multiple sends in one atomic Kafka transaction from a non-listener thread.
        //
        // The lambda receives an KafkaOperations instance (`ops`) that is bound to
        // a single transactional producer for the duration of the lambda.
        // All ops.send() calls join the SAME transaction.
        // Normal lambda return → commit. Thrown exception → abort.
        //
        // The return value of the lambda is passed back as the return value of
        // executeInTransaction() — we use AtomicReference to capture the SendResult
        // from send#1 for the ProducerResponse metadata.
        AtomicReference<SendResult<String, Object>> firstResult = new AtomicReference<>();

        kafkaTemplate.executeInTransaction(ops -> {
            // Send 1: write to processed topic
            SendResult<String, Object> result = sendSyncViaOps(ops, processedTopic, order.getOrderId(), order);
            firstResult.set(result);
            log.info(">>> [TX-PRODUCER] send#1 to={} partition={} offset={}",
                    processedTopic, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());

            // Send 2: write a probe record to the probe topic
            OrderMessage probeRecord = buildProbe(order.getOrderId());
            SendResult<String, Object> probeResult = sendSyncViaOps(ops, probeTopic, probeRecord.getOrderId(), probeRecord);
            log.info(">>> [TX-PRODUCER] send#2 to={} partition={} offset={}",
                    probeTopic, probeResult.getRecordMetadata().partition(), probeResult.getRecordMetadata().offset());

            // JUNIOR NOTE: Lambda returns normally → executeInTransaction() calls
            // producer.commitTransaction(). Both records become visible to
            // read_committed consumers simultaneously after the commit.
            return null;
        });

        log.info(">>> [TX-PRODUCER] COMMIT transaction orderId={}", order.getOrderId());

        RecordMetadata meta = firstResult.get().getRecordMetadata();
        return ProducerResponse.builder()
                .orderId(order.getOrderId())
                .status("COMMITTED")
                .partition(meta.partition())
                .offset(meta.offset())
                .sentAt(LocalDateTime.now())
                .build();
    }


    // ===================================================================
    // ROLLED BACK TRANSACTION — sends are aborted, consumers see nothing
    // ===================================================================

    public ProducerResponse sendRolledBack(double amount) {
        OrderMessage order = buildOrder(amount, true);
        log.info(">>> [TX-PRODUCER] BEGIN transaction (will rollback) orderId={}", order.getOrderId());

        try {
            kafkaTemplate.executeInTransaction(ops -> {
                // Send 1: write to processed topic — record is queued in the transaction
                SendResult<String, Object> result = sendSyncViaOps(ops, processedTopic, order.getOrderId(), order);
                log.info(">>> [TX-PRODUCER] send#1 queued (not yet committed) to={} partition={} offset={}",
                        processedTopic, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());

                // JUNIOR NOTE: Even though sendSyncViaOps() returned a real offset,
                // the record is NOT visible to read_committed consumers yet.
                // They wait for a COMMIT control batch marker before advancing.
                // We are about to throw — so they will see ABORT instead, and skip.

                log.warn(">>> [TX-PRODUCER] Simulated processing failure — about to throw");
                // JUNIOR NOTE: Throwing from inside the executeInTransaction lambda
                // causes Spring Kafka to call producer.abortTransaction() before
                // re-throwing the exception. The broker writes an ABORT control batch.
                // The record at the offset above becomes permanently invisible to
                // read_committed consumers. The offset still physically exists in the
                // Kafka log (append-only), but is skipped over on consume.
                throw new RuntimeException(
                        "Simulated rollback: payment validation failed for orderId=" + order.getOrderId());
            });
        } catch (RuntimeException e) {
            log.warn(">>> [TX-PRODUCER] ABORT transaction orderId={} — ex={}", order.getOrderId(), e.getMessage());
            throw e; // re-throw so the caller (controller or consumer) can handle it
        }

        // Unreachable — the throw above always fires
        return null;
    }


    // ===================================================================
    // INITIAL PRODUCER — sends to orders topic (single send, auto-wrapped)
    // ===================================================================

    // JUNIOR NOTE: sendOrder() puts a message onto the orders input topic.
    // This is the "trigger" for the full pipeline demo (POST /send → orders topic
    // → OrderConsumerService → sendCommitted() or sendRolledBack()).
    //
    // A single send from a transactional producer outside an explicit transaction
    // is auto-wrapped by Spring Kafka in a minimal single-send transaction.
    // That's fine here — we don't need multi-send atomicity for the input topic.
    public ProducerResponse sendOrder(double amount, boolean rollback) {
        OrderMessage order = buildOrder(amount, rollback);
        log.info(">>> [PRODUCER] Sending order orderId={} rollback={}", order.getOrderId(), rollback);

        // JUNIOR NOTE: For a single send from a transactional producer outside
        // executeInTransaction(), Spring Kafka 4.x auto-wraps it in its own
        // mini transaction — begin, send, commit. No explicit wrapper needed.
        SendResult<String, Object> result = sendSync(ordersTopic, order.getOrderId(), order);
        RecordMetadata meta = result.getRecordMetadata();

        return ProducerResponse.builder()
                .orderId(order.getOrderId())
                .status("SENT_TO_ORDERS")
                .partition(meta.partition())
                .offset(meta.offset())
                .sentAt(LocalDateTime.now())
                .build();
    }


    // ===================================================================
    // HELPERS
    // ===================================================================

    // Used inside executeInTransaction lambda — ops is the transactional KafkaOperations
    @SuppressWarnings("unchecked")
    private SendResult<String, Object> sendSyncViaOps(
            org.springframework.kafka.core.KafkaOperations<String, Object> ops,
            String topic, String key, Object value) {
        try {
            return (SendResult<String, Object>) ops.send(topic, key, value).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Send failed to topic=" + topic + ": " + e.getMessage(), e);
        }
    }

    // Used outside transactions (e.g. sendOrder)
    private SendResult<String, Object> sendSync(String topic, String key, Object value) {
        try {
            return kafkaTemplate.send(topic, key, value).get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Send failed to topic=" + topic + ": " + e.getMessage(), e);
        }
    }

    private OrderMessage buildOrder(double amount, boolean rollback) {
        return OrderMessage.builder()
                .orderId("ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase())
                .customerId("CUST-" + (int) (Math.random() * 1000 + 100))
                .amount(amount)
                .rollback(rollback)
                .timestamp(LocalDateTime.now())
                .build();
    }

    private OrderMessage buildProbe(String originalOrderId) {
        return OrderMessage.builder()
                .orderId("PROBE-" + originalOrderId)
                .customerId("SYSTEM")
                .amount(0)
                .rollback(false)
                .timestamp(LocalDateTime.now())
                .build();
    }

}
