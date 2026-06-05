package com.vbforge.case11.service;

import com.vbforge.case11.exception.NonRetryableOrderException;
import com.vbforge.case11.model.OrderMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
 
import java.util.concurrent.atomic.AtomicInteger;
 
// JUNIOR NOTE: This listener is the same structure as case-09/10.
// The NEW behaviour is entirely in KafkaConfig — the DeadLetterPublishingRecoverer.
// From the listener's perspective nothing changed: it throws exceptions, Spring catches them.
// What changed is WHAT HAPPENS after all retries are exhausted:
//   case-09/10: log and commit past the record (silent loss)
//   case-11:    publish to DLT with headers, THEN commit past the record (preserved + observable)
//
// The listener doesn't need to know about the DLT at all.
// This is separation of concerns — business logic (process order) is decoupled from
// infrastructure concern (what to do with unprocessable records).
 
@Service
@Slf4j
public class OrderConsumerService {
 
    private final AtomicInteger successCount      = new AtomicInteger(0);
    private final AtomicInteger dltRoutedCount    = new AtomicInteger(0);
 
    @KafkaListener(
            topics = "${kafka.topic.name}",
            groupId = "${kafka.consumer.groupID}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, OrderMessage> record) {
        OrderMessage order = record.value();
        log.info(">>> [MAIN-CONSUMER] partition={} offset={} orderId={} failureMode={}",
                record.partition(), record.offset(), order.getOrderId(), order.getFailureMode());
 
        switch (order.getFailureMode()) {
 
            case "none" -> {
                successCount.incrementAndGet();
                log.info(">>> [MAIN-CONSUMER] ✓ Processed order={} amount={}",
                        order.getOrderId(), order.getAmount());
            }
 
            case "transient" -> {
                // JUNIOR NOTE: Throws on every attempt — exhausts retry budget →
                // DeadLetterPublishingRecoverer publishes to DLT.
                // Watch logs: retries fire with exponential delays, then
                // "[DLT-RECOVERER] Publishing to DLT" appears, then DltConsumerService receives it.
                dltRoutedCount.incrementAndGet();
                log.warn(">>> [MAIN-CONSUMER] ✗ Transient failure on order={}", order.getOrderId());
                throw new RuntimeException(
                        "Simulated transient failure — order=" + order.getOrderId());
            }
 
            case "non-retryable" -> {
                // JUNIOR NOTE: NonRetryableOrderException is registered as non-retryable.
                // No retries — goes straight to DLT in one attempt.
                // Useful for: invalid amount, unknown customer, incompatible schema.
                dltRoutedCount.incrementAndGet();
                log.warn(">>> [MAIN-CONSUMER] ✗ Non-retryable failure on order={} amount={}",
                        order.getOrderId(), order.getAmount());
                throw new NonRetryableOrderException(
                        "Invalid order data — orderId=" + order.getOrderId()
                                + " amount=" + order.getAmount());
            }
 
            default -> {
                successCount.incrementAndGet();
                log.warn(">>> [MAIN-CONSUMER] Unknown failureMode — treating as success");
            }
        }
    }
 
    public int getSuccessCount()   { return successCount.get(); }
    public int getDltRoutedCount() { return dltRoutedCount.get(); }
 
}