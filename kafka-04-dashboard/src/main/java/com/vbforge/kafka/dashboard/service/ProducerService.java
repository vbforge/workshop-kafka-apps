package com.vbforge.kafka.dashboard.service;

import com.vbforge.kafka.dashboard.model.DashboardEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

/**
 * Publishes DashboardEvent messages to Kafka.
 *
 * KEY LESSON — Partition routing:
 *
 * kafkaTemplate.send(topic, key, value)
 *
 * Kafka uses the key to decide which partition the message goes to.
 * The default partitioner hashes the key and routes to: hash(key) % numPartitions
 *
 * This means:
 *   - Same sender name → always the same partition
 *   - Different senders → distributed across partitions
 *
 * This is why partition awareness matters: consumer group members each own
 * a subset of partitions. If all messages go to one partition, only one
 * consumer instance processes them regardless of group size.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ProducerService {

    private final KafkaTemplate<String, DashboardEvent> kafkaTemplate;

    @Value("${app.kafka.topic}")
    private String topic;

    /**
     * Sends a dashboard event to Kafka.
     *
     * The sender name is used as the partition key — so messages from the
     * same sender always land on the same partition (key-ordered delivery).
     */
    public void send(String sender, String content, String category) {
        DashboardEvent event = DashboardEvent.builder()
                .sender(sender)
                .content(content)
                .category(category)
                .sentAt(LocalDateTime.now())
                .build();

        // Using sender as the partition key — demonstrates key-based routing
        CompletableFuture<SendResult<String, DashboardEvent>> future =
                kafkaTemplate.send(topic, sender, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send event from sender [{}]: {}", sender, ex.getMessage());
            } else {
                log.info("Sent event | sender={} | topic={} | partition={} | offset={}",
                        sender,
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

}














