package com.vbforge.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Simple Kafka Consumer connecting to Docker Kafka.
 * 
 * Features:
 * - Connects to Kafka running in Docker (localhost:9092)
 * - Subscribes to demo_topic_example
 * - Continuous polling with graceful shutdown support
 * - Logs message details including partition and offset
 * 
 * Before running:
 * 1. Start Kafka in Docker: docker-compose up -d
 * 2. Ensure topic 'demo_topic_example' exists
 * 3. Run this consumer
 * 4. Send messages via ProducerDemo or console producer
 * 
 * To stop gracefully: Press Ctrl+C
 *
 * By the way this class is duplicated just for investigation purpose, when we can observe multiple consumers running simultaneously:
 *   Expected behavior occur:
 *       - First consumer gets some partitions (e.g., 0, 1)
 *       - Second consumer gets remaining partitions (e.g., 2)
 *       - Messages are distributed across both consumers
 */
public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    // Volatile flag for shutdown coordination
    private static volatile boolean keepRunning = true;

    public static void main(String[] args) {

        log.info("🚀 Starting Kafka Consumer");

        String groupId = "my-java-application";
        String topic = "demo_topic_example";

        // Create Consumer Properties
        Properties properties = new Properties();

        // Connect to Docker Kafka on localhost:9092
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // Deserializers (convert bytes back to Java objects)
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        // Consumer group configuration
        properties.setProperty("group.id", groupId);
        
        // Where to start reading from:
        // - "earliest": from beginning of topic
        // - "latest": only new messages
        // - "none": error if no offset committed
        properties.setProperty("auto.offset.reset", "earliest");
        
        // Auto-commit offsets (simpler for basic demo)
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000");

        // Create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Register shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("⚠️ Shutdown signal received. Stopping consumer...");
            keepRunning = false;
            consumer.wakeup(); // Interrupts the poll() method
        }));

        try {
            // Subscribe to the topic
            consumer.subscribe(Arrays.asList(topic));
            log.info("✅ Subscribed to topic: {}", topic);

            // Poll for data continuously
            while (keepRunning) {
                log.debug("Polling for messages...");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                    log.info("📨 Received message:");
                    log.info("   Key:       {}", record.key());
                    log.info("   Value:     {}", record.value());
                    log.info("   Topic:     {}", record.topic());
                    log.info("   Partition: {}", record.partition());
                    log.info("   Offset:    {}", record.offset());
                    log.info("   Timestamp: {}", record.timestamp());
                    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                }
            }

        } catch (WakeupException e) {
            // Expected exception from consumer.wakeup() during shutdown
            log.info("👋 Consumer wakeup called - shutting down gracefully");
        } catch (Exception e) {
            log.error("❌ Unexpected error in consumer loop", e);
        } finally {
            // Always close the consumer to commit offsets and leave group
            log.info("🔒 Closing consumer...");
            consumer.close();
            log.info("✅ Consumer closed successfully");
        }

        log.info("🏁 Consumer finished");
    }
}