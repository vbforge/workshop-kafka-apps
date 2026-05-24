package com.vbforge.kafkanotificationdemo.service;

import com.vbforge.kafkanotificationdemo.model.Notification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class NotificationProducer {

    private static final Logger logger = LoggerFactory.getLogger(NotificationProducer.class);

    @Value("${app.kafka.topic.notification}")
    private String topicName;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public NotificationProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendNotification(Notification notification){
        String message = String.format("%s|%s|%s",
                notification.getId(),
                notification.getType(),
                notification.getMessage());

        logger.info("Sending notification to Kafka: {}", message);

        CompletableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(topicName, notification.getId(), message);

        future.whenComplete((result, ex) -> {

            if(ex == null){
                logger.info("Successfully sent notification with key=[{}] and offset=[{}]",
                        notification.getId(), result.getRecordMetadata().offset());
            } else {
                logger.error("Failed to send notification with key=[{}] due to: {}",
                        notification.getId(), ex.getMessage());
            }

        });

    }

}






