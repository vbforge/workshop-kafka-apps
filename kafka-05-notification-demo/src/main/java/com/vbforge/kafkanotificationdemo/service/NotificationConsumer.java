package com.vbforge.kafkanotificationdemo.service;

import com.vbforge.kafkanotificationdemo.model.Notification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class NotificationConsumer {

    private static final Logger logger = LoggerFactory.getLogger(NotificationConsumer.class);

    // Thread-safe list to store processed notifications
    private final List<Notification> processedNotifications = new CopyOnWriteArrayList<>();

    @KafkaListener(topics = "${app.kafka.topic.notification}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeNotification(String message){
        try{
            logger.info("Received notification from Kafka: {}", message);

            // Parse the message (format: id|type|message)
            String[] parts = message.split("\\|", 3);
            if (parts.length >= 3) {
                Notification notification = new Notification();
                notification.setId(parts[0]);
                notification.setType(parts[1]);
                notification.setMessage(parts[2]);
                notification.setTimestamp(LocalDateTime.now());
                notification.setProcessed(true);

                // Simulate processing the notification
                processNotification(notification);

                // Store in our in-memory list for display
                processedNotifications.add(notification);

                logger.info("Successfully processed notification: {}", notification.getId());
            } else {
                logger.warn("Invalid message format received: {}", message);
            }
        }catch (Exception e){
            logger.error("Error processing notification: {}", e.getMessage(), e);
        }
    }

    private void processNotification(Notification notification) {
        // Simulate some processing time
        try {
            Thread.sleep(1000); // 1 second processing time
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        logger.info("Processing notification of type '{}': {}",
                notification.getType(), notification.getMessage());
    }

    public List<Notification> getProcessedNotifications() {
        return new ArrayList<>(processedNotifications);
    }

    public void clearProcessedNotifications() {
        processedNotifications.clear();
    }


}
