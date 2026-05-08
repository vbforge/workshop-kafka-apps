package com.vbforge.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Producer demonstrating asynchronous callbacks with metadata logging.
 * 
 * Key Concept: Callbacks allow non-blocking handling of send results.
 * - Executes when Kafka acknowledges the message (or error occurs)
 * - Does NOT block the main thread
 * - Perfect for logging, metrics, or audit trails
 * 
 * Features:
 * - Sends 10 messages with 1-second delay between each
 * - Logs metadata (topic, partition, offset, timestamp) on success
 * - Logs errors if send fails
 * - Messages go to partitions using default sticky partitioner
 * 
 * Before running:
 * 1. Start Kafka in Docker: docker-compose up -d
 * 2. Ensure topic 'demo_topic_example' exists (auto-created if not)
 * 3. Run this producer
 * 
 * Expected behavior:
 * - Each message's metadata is logged asynchronously
 * - Multiple messages may batch to same partition
 * - Order of callbacks may not match send order
 * 
 * Why callbacks matter:
 * - Know exactly where your message landed (partition + offset)
 * - Detect failures without blocking
 * - Implement custom retry logic per message
 */
public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {

        log.info("🚀 Starting Kafka Producer with Callback (Docker version)");
        log.info("📊 Demonstrating asynchronous metadata logging");
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        // Create Producer Properties
        Properties properties = new Properties();

        // Connect to Docker Kafka on localhost:9092
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        
        // Optional: Add reliability settings
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "3");
        
        // Enable idempotence (prevents duplicates)
        properties.setProperty("enable.idempotence", "true");

        // Counter for tracking
        final int[] successCount = {0};
        final int[] failureCount = {0};

        // Create the producer (using try-with-resources for auto-close)
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            // Send 10 messages with 1-second delay between each
            for (int i = 1; i <= 10; i++) {
                
                String message = "Message #" + i + " sent at " + System.currentTimeMillis();
                
                // Create producer record (no key, just value)
                ProducerRecord<String, String> producerRecord = 
                    new ProducerRecord<>("demo_topic_example", message);

                // Send data asynchronously with callback
                final int messageNumber = i;
                producer.send(producerRecord, (metadata, exception) -> {
                    if (exception == null) {
                        // SUCCESS: Log metadata for this message
                        successCount[0]++;
                        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                        log.info("✅ Message #{} sent successfully!", messageNumber);
                        log.info("   Topic:     {}", metadata.topic());
                        log.info("   Partition: {}", metadata.partition());
                        log.info("   Offset:    {}", metadata.offset());
                        log.info("   Timestamp: {}", metadata.timestamp());
                        log.info("   Serialized key size:   {} bytes", metadata.serializedKeySize());
                        log.info("   Serialized value size: {} bytes", metadata.serializedValueSize());
                        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                    } else {
                        // ERROR: Log the failure
                        failureCount[0]++;
                        log.error("❌ Message #{} failed to send!", messageNumber, exception);
                    }
                });
                
                log.info("📤 Message #{} sent asynchronously (waiting for callback)...", i);
                
                // Wait 1 second between sends to make observation easier
                Thread.sleep(1000);
            }

            // Flush any remaining messages and wait for completion
            log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            log.info("🔄 Flushing producer...");
            producer.flush();
            
            // Give callbacks time to complete
            Thread.sleep(500);
            
        } catch (InterruptedException e) {
            log.error("Thread interrupted", e);
            Thread.currentThread().interrupt();
        }

        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("🏁 Producer finished");
        log.info("📊 Summary: {} successful, {} failed", successCount[0], failureCount[0]);
        log.info("🎯 Key insight: Callbacks are ASYNCHRONOUS - they run after Kafka acknowledges!");
    }
}