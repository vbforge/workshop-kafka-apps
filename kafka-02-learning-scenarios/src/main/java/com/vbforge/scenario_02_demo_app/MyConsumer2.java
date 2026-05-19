package com.vbforge.scenario_02_demo_app;

import com.vbforge.config.KafkaConfig;
import com.vbforge.config.Utility;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import java.time.Duration;
import java.util.Collections;
 
import static com.vbforge.config.Constants.*;
 
/**
 * MyConsumer2 — second consumer for Experiment 1 (broadcast vs load-balance).
 *
 * Uses a DIFFERENT group ID (consumer-group-topic-demo-2) than MyConsumer.
 *
 * Expected result when both are running simultaneously:
 *  - Both consumers receive ALL messages → broadcast behaviour
 *
 * To observe load-balancing instead:
 *  - Change CONSUMER_GROUP_DEMO_2 → CONSUMER_GROUP_DEMO (same group as MyConsumer)
 *  - Each message will be delivered to only ONE of the two consumers
 *  - With 3 partitions and 2 consumers, partitions are split between them
 *
 * STOP: Ctrl+C in terminal only.
 */
public class MyConsumer2 {
 
    private static final Logger logger = LoggerFactory.getLogger(MyConsumer2.class);
 
    private KafkaConsumer<String, String> consumer;
 
    public static void main(String[] args) {
        new MyConsumer2().run();
    }
 
    private void run() {
 
        logger.info("=== Quick Demo App Consumer2 ===");
        logger.info("Group ID: {} (different from MyConsumer → broadcast)", CONSUMER_GROUP_DEMO_2);
        Utility.verifyConfiguration();
 
        consumer = new KafkaConsumer<>(KafkaConfig.createConsumerConfig(CONSUMER_GROUP_DEMO_2)); //or could be CONSUMER_GROUP_DEMO to test load-balance
 
        registerShutdownHook();
 
        try {
            consumer.subscribe(Collections.singletonList(TOPIC_DEMO));
            logger.info("Subscribed to topic: {} | group: {}", TOPIC_DEMO, CONSUMER_GROUP_DEMO_2);
            logger.info("Listening for messages... (Ctrl+C to stop)");
 
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(DEFAULT_POLL_TIMEOUT_MS));
 
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("==================================");
                    logger.info("[Consumer2] Received message:");
                    logger.info("  Key:       {}", record.key());
                    logger.info("  Value:     {}", record.value());
                    logger.info("  Partition: {}", record.partition());
                    logger.info("  Offset:    {}", record.offset());
                    logger.info("  Timestamp: {}", record.timestamp());
                    logger.info("==================================");
                }
            }
 
        } catch (WakeupException e) {
            logger.info("WakeupException — shutting down");
        } finally {
            consumer.close();
            logger.info("Consumer2 closed.");
        }
    }
 
    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received — calling consumer.wakeup()");
            consumer.wakeup();
        }, "consumer2-shutdown-hook"));
    }
}