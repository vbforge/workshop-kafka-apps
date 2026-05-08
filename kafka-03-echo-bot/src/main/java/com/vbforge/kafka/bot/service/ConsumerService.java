package com.vbforge.kafka.bot.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class ConsumerService {

    private final SimpMessagingTemplate simpMessagingTemplate;

    @Value("${app.kafka.topic}")
    private String topic;

    private final List<String> receivedMessages = new ArrayList<>();

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consume(String message) {
        log.info("Received message from Kafka topic [{}]: {}", topic, message);
        String stamped = message + " [received at " + LocalDateTime.now() + "]";

        synchronized (receivedMessages) {
            receivedMessages.add(0, stamped);
            if(receivedMessages.size() > 10) {
                receivedMessages.remove(10);
            }
        }
        simpMessagingTemplate.convertAndSend("/topic/messages", message);
    }

    public List<String> getRecentMessages() {
        synchronized (receivedMessages) {
            return new ArrayList<>(receivedMessages);
        }
    }


}
