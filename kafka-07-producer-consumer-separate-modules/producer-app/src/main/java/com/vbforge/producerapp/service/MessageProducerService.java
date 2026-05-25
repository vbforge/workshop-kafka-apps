package com.vbforge.producerapp.service;

import com.vbforge.producerapp.model.MessageEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class MessageProducerService {

    private final KafkaTemplate<String, MessageEvent> kafkaTemplate;
//    private static final String TOPIC = "t2-topic";
    private static final String TOPIC = "orders-topic";

    public CompletableFuture<SendResult<String, MessageEvent>> sendMessage(String message, String sender, String priority) {
        MessageEvent event = MessageEvent.builder()
                .id(UUID.randomUUID().toString())
                .message(message)
                .sender(sender)
                .timestamp(LocalDateTime.now())
                .priority(priority != null ? priority : "NORMAL")
                .build();

        log.info("Sending message: {} to topic: {}", event, TOPIC);

        CompletableFuture<SendResult<String, MessageEvent>> future = kafkaTemplate.send(TOPIC, event.getId(), event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Message sent successfully: offset={}, partition={}",
                        result.getRecordMetadata().offset(),
                        result.getRecordMetadata().partition());
            } else {
                log.error("Failed to send message: {}", ex.getMessage());
            }
        });

        return future;
    }

}
