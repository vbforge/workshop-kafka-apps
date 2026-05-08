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
 * Consumer demonstrating graceful shutdown with proper resource cleanup.
 * 
 * What is Graceful Shutdown?
 * - Allows consumer to finish processing current messages
 * - Commits offsets before exiting
 * - Leaves consumer group properly
 * - No data loss or duplicate processing
 * 
 * Key Features:
 * - Shutdown hook with consumer.wakeup()
 * - WakeupException handling
 * - Proper offset commit on close()
 * - Clean consumer group departure
 * - Supports multiple instances (rebalancing)
 * 
 * Why Graceful Shutdown Matters:
 * - Prevents data loss (offsets committed)
 * - Avoids consumer group delays (leave cleanly)
 * - Enables faster rebalancing
 * - Clean resource cleanup
 * 
 * Before running:
 * 1. Start Kafka in Docker: docker-compose up -d
 * 2. Ensure topic 'demo_topic_example' exists with 3 partitions
 * 3. Run MULTIPLE instances to see rebalancing on shutdown
 * 
 * How to test:
 * - Run 1-3 consumer instances
 * - Press Ctrl+C to shutdown gracefully
 * - Watch partition rebalancing
 * - Verify offsets are committed
 */
public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {

        log.info("🚀 Starting Kafka Consumer with Graceful Shutdown");
        log.info("🛡️ Demonstrating proper resource cleanup");
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        String groupId = "my-java-application";
        String topic = "demo_topic_example";

        // Create Consumer Properties
        Properties properties = new Properties();

        // Connect to Docker Kafka on localhost:9092
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // Deserializers
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        // Consumer group configuration
        properties.setProperty("group.id", groupId);
        
        // Start reading from beginning of topic
        properties.setProperty("auto.offset.reset", "earliest");
        
        // Auto-commit offsets (simpler for demo)
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Track consumer instance
        final String instanceId = String.valueOf(Thread.currentThread().getId());
        log.info("📌 Consumer instance ID: {}", instanceId);

        // ── Graceful Shutdown Hook Setup ─────────────────────────────────────
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            log.info("⚠️ SHUTDOWN SIGNAL DETECTED (Ctrl+C)");
            log.info("   Instance: {}", instanceId);
            log.info("   Calling consumer.wakeup() to interrupt poll()...");
            consumer.wakeup();  // This will cause WakeupException in poll()
            
            try {
                log.info("   Waiting for main thread to finish processing...");
                mainThread.join();
                log.info("   Main thread completed");
            } catch (InterruptedException e) {
                log.error("   Interrupted while waiting for main thread");
                Thread.currentThread().interrupt();
            }
            log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        }));

        try {
            // Subscribe to topic
            consumer.subscribe(Arrays.asList(topic));
            log.info("✅ Subscribed to topic: {}", topic);
            log.info("   Consumer group: {}", groupId);
            log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            log.info("💡 Press Ctrl+C to gracefully shut down");
            log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

            int messageCount = 0;
            int pollCount = 0;
            
            // Poll loop
            while (true) {
                pollCount++;
                log.debug("Polling... (poll #{})", pollCount);

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    messageCount += records.count();
                    log.info("📨 Received {} message(s) (total: {})", records.count(), messageCount);
                    
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                        log.info("📬 Message Details:");
                        log.info("   Key:       {}", record.key());
                        log.info("   Value:     {}", record.value());
                        log.info("   Topic:     {}", record.topic());
                        log.info("   Partition: {}", record.partition());
                        log.info("   Offset:    {}", record.offset());
                        log.info("   Timestamp: {}", record.timestamp());
                        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                        
                        // Simulate processing time
                        // Thread.sleep(10);
                    }
                } else {
                    // Log every 20th empty poll to show we're alive
                    if (pollCount % 20 == 0) {
                        log.info("💓 Heartbeat: {} messages processed so far, still polling...", messageCount);
                    }
                }
            }

        } catch (WakeupException e) {
            log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            log.info("👋 WakeupException caught - initiating graceful shutdown");
            log.info("   This exception is EXPECTED and NORMAL during shutdown");
            log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        } catch (Exception e) {
            log.error("❌ Unexpected exception in consumer loop", e);
        } finally {
            log.info("🔒 Closing consumer...");
            log.info("   - Committing final offsets");
            log.info("   - Leaving consumer group");
            log.info("   - Closing network connections");
            consumer.close();  // Commits offsets and leaves consumer group
            log.info("✅ Consumer closed successfully");
        }

        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("🏁 Consumer finished - graceful shutdown complete!");
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }
}