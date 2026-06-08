package com.vbforge.case12.service;

import com.vbforge.case12.model.GenericMessage;
import com.vbforge.case12.model.ProducerResponse;
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
 
@Service
@Slf4j
@RequiredArgsConstructor
public class ProducerService {
 
    private final KafkaTemplate<String, Object> kafkaTemplate;
 
    @Value("${kafka.topic.orders}")
    private String ordersTopic;
 
    @Value("${kafka.topic.payments}")
    private String paymentsTopic;
 
    @Value("${kafka.topic.notifications}")
    private String notificationsTopic;
 
    public ProducerResponse send(String topicType, String failureMode) {
        if (failureMode == null || failureMode.isBlank()) failureMode = "none";
 
        String targetTopic = switch (topicType.toLowerCase()) {
            case "payments"      -> paymentsTopic;
            case "notifications" -> notificationsTopic;
            default              -> ordersTopic;
        };
 
        GenericMessage message = GenericMessage.builder()
                .id(UUID.randomUUID().toString())
                .type(topicType)
                .content("Message for " + topicType)
                .failureMode(failureMode)
                .timestamp(LocalDateTime.now())
                .build();
 
        log.info(">>> [PRODUCER] Sending to={} failureMode={} id={}", targetTopic, failureMode, message.getId());
 
        try {
            SendResult<String, Object> result = kafkaTemplate
                    .send(targetTopic, message.getId(), message)
                    .get(5, TimeUnit.SECONDS);
 
            RecordMetadata meta = result.getRecordMetadata();
            return ProducerResponse.builder()
                    .messageId(message.getId())
                    .topic(targetTopic)
                    .failureMode(failureMode)
                    .partition(meta.partition())
                    .offset(meta.offset())
                    .sentAt(LocalDateTime.now())
                    .build();
 
        } catch (TimeoutException e) {
            throw new RuntimeException("Send timed out", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Send interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Broker error: " + e.getCause().getMessage(), e);
        }
    }
 
}