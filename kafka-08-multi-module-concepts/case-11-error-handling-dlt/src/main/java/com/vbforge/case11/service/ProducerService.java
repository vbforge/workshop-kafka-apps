package com.vbforge.case11.service;

import com.vbforge.case11.model.OrderMessage;
import com.vbforge.case11.model.ProducerResponse;
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
 
    @Value("${kafka.topic.name}")
    private String topic;
 
    public ProducerResponse send(String failureMode, double amount) {
        if (failureMode == null || failureMode.isBlank()) failureMode = "none";
 
        OrderMessage order = OrderMessage.builder()
                .orderId("ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase())
                .customerId("CUST-" + (int)(Math.random() * 1000 + 100))
                .amount(amount)
                .failureMode(failureMode)
                .timestamp(LocalDateTime.now())
                .build();
 
        log.info(">>> [PRODUCER] Sending orderId={} failureMode={} amount={}",
                order.getOrderId(), failureMode, amount);
 
        try {
            SendResult<String, Object> result = kafkaTemplate
                    .send(topic, order.getOrderId(), order)
                    .get(5, TimeUnit.SECONDS);
 
            RecordMetadata meta = result.getRecordMetadata();
            return ProducerResponse.builder()
                    .orderId(order.getOrderId())
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