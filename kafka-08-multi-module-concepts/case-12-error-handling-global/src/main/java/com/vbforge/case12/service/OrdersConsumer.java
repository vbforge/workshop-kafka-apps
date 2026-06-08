package com.vbforge.case12.service;

import com.vbforge.case12.exception.NonRetryableException;
import com.vbforge.case12.model.GenericMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
 
import java.util.concurrent.atomic.AtomicInteger;
 
// JUNIOR NOTE: Three listener classes, each covering a different topic.
// All three benefit from the SAME global error handler configured once in KafkaConfig.
// The listeners themselves contain zero error handling logic — they just throw on failure.
// Error routing, retries, DLT publishing are all infrastructure concerns handled globally.
// This is exactly how a real microservice with multiple topics should be structured.
 
@Service
@Slf4j
public class OrdersConsumer {
 
    private final AtomicInteger processed = new AtomicInteger(0);
 
    @KafkaListener(
            topics = "${kafka.topic.orders}",
            groupId = "${kafka.consumer.ordersGroupID}",
            containerFactory = "ordersContainerFactory"
    )
    public void consume(ConsumerRecord<String, GenericMessage> record) {
        GenericMessage msg = record.value();
        log.info("[ORDERS] partition={} offset={} id={} failureMode={}",
                record.partition(), record.offset(), msg.getId(), msg.getFailureMode());
 
        switch (msg.getFailureMode()) {
            case "none" -> {
                processed.incrementAndGet();
                log.info("[ORDERS] ✓ Processed order id={}", msg.getId());
            }
            case "transient" -> {
                log.warn("[ORDERS] ✗ Transient failure id={}", msg.getId());
                throw new RuntimeException("Transient order processing failure id=" + msg.getId());
            }
            case "non-retryable" -> {
                log.warn("[ORDERS] ✗ Non-retryable failure id={}", msg.getId());
                throw new NonRetryableException("Invalid order data id=" + msg.getId());
            }
            default -> processed.incrementAndGet();
        }
    }
 
    public int getProcessed() { return processed.get(); }
 
}