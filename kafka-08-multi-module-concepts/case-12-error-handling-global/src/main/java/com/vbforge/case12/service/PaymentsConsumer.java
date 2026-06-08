package com.vbforge.case12.service;

import com.vbforge.case12.exception.NonRetryableException;
import com.vbforge.case12.model.GenericMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
 
import java.util.concurrent.atomic.AtomicInteger;
 
@Service
@Slf4j
public class PaymentsConsumer {
 
    private final AtomicInteger processed = new AtomicInteger(0);
 
    @KafkaListener(
            topics = "${kafka.topic.payments}",
            groupId = "${kafka.consumer.paymentsGroupID}",
            containerFactory = "paymentsContainerFactory"
    )
    public void consume(ConsumerRecord<String, GenericMessage> record) {
        GenericMessage msg = record.value();
        log.info("[PAYMENTS] partition={} offset={} id={} failureMode={}",
                record.partition(), record.offset(), msg.getId(), msg.getFailureMode());
 
        switch (msg.getFailureMode()) {
            case "none" -> {
                processed.incrementAndGet();
                log.info("[PAYMENTS] ✓ Processed payment id={}", msg.getId());
            }
            case "transient" -> {
                log.warn("[PAYMENTS] ✗ Transient failure id={}", msg.getId());
                throw new RuntimeException("Payment gateway timeout id=" + msg.getId());
            }
            case "non-retryable" -> {
                log.warn("[PAYMENTS] ✗ Non-retryable failure id={}", msg.getId());
                throw new NonRetryableException("Invalid payment amount id=" + msg.getId());
            }
            default -> processed.incrementAndGet();
        }
    }
 
    public int getProcessed() { return processed.get(); }
 
}