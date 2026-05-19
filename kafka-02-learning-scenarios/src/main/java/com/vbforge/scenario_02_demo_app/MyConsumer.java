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
 * MyConsumer — basic consumer for the demo app scenario.
 *
 * Demonstrates:
 *  - Subscribing to a topic and polling in a loop
 *  - Graceful shutdown via ShutdownHook + consumer.wakeup()
 *  - Reading message metadata (partition, offset, key, timestamp)
 *
 * Uses consumer-group-topic-demo.
 * Run alongside MyConsumer2 to see broadcast behavior
 * (different group IDs → both consumers receive all messages).
 *
 * STOP: Press Ctrl+C in the terminal. Do NOT use the IDE Stop button —
 * that sends SIGKILL and bypasses the shutdown hook.
 */
public class MyConsumer {
 
    private static final Logger logger = LoggerFactory.getLogger(MyConsumer.class);
 
    private KafkaConsumer<String, String> consumer;
 
    public static void main(String[] args) {
        new MyConsumer().run();
    }
 
    private void run() {
 
        logger.info("=== Quick Demo App Consumer ===");
        Utility.verifyConfiguration();
 
        consumer = new KafkaConsumer<>(KafkaConfig.createConsumerConfig(CONSUMER_GROUP_DEMO));
 
        registerShutdownHook();
 
        try {
            consumer.subscribe(Collections.singletonList(TOPIC_DEMO));
            logger.info("Subscribed to topic: {} | group: {}", TOPIC_DEMO, CONSUMER_GROUP_DEMO);
            logger.info("Listening for messages... (Ctrl+C to stop)");
 
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(DEFAULT_POLL_TIMEOUT_MS));
 
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("==================================");
                    logger.info("Received message:");
                    logger.info("  Key:       {}", record.key());
                    logger.info("  Value:     {}", record.value());
                    logger.info("  Partition: {}", record.partition());
                    logger.info("  Offset:    {}", record.offset());
                    logger.info("  Timestamp: {}", record.timestamp());
                    logger.info("==================================");
                }
            }
 
        } catch (WakeupException e) {
            logger.info("WakeupException - shutting down");
        } finally {
            consumer.close(); // commits pending offsets, releases partition assignment
            logger.info("Consumer closed.");
        }
    }
 
    /**
     * Shutdown hook: Ctrl+C → SIGINT → hook fires → wakeup() interrupts poll() immediately.
     * consumer.close() is called on the main thread in the finally block — required,
     * because KafkaConsumer is NOT thread-safe.
     */
    private void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received - calling consumer.wakeup()");
            consumer.wakeup();
        }, "consumer-shutdown-hook"));
    }
}