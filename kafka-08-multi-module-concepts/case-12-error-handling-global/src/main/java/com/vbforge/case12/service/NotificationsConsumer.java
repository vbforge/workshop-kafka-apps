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
public class NotificationsConsumer {
 
    private final AtomicInteger processed = new AtomicInteger(0);
 
    @KafkaListener(
            topics = "${kafka.topic.notifications}",
            groupId = "${kafka.consumer.notificationsGroupID}",
            containerFactory = "notificationsContainerFactory"
    )
    public void consume(ConsumerRecord<String, GenericMessage> record) {
        GenericMessage msg = record.value();
        log.info("[NOTIFICATIONS] partition={} offset={} id={} failureMode={}",
                record.partition(), record.offset(), msg.getId(), msg.getFailureMode());
 
        switch (msg.getFailureMode()) {
            case "none" -> {
                processed.incrementAndGet();
                log.info("[NOTIFICATIONS] ✓ Processed notification id={}", msg.getId());
            }
            case "transient" -> {
                log.warn("[NOTIFICATIONS] ✗ Transient failure id={}", msg.getId());
                throw new RuntimeException("Notification service unavailable id=" + msg.getId());
            }
            case "non-retryable" -> {
                log.warn("[NOTIFICATIONS] ✗ Non-retryable failure id={}", msg.getId());
                throw new NonRetryableException("Unknown recipient id=" + msg.getId());
            }
            default -> processed.incrementAndGet();
        }
    }
 
    public int getProcessed() { return processed.get(); }
 
}