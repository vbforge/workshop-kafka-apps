package com.vbforge.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Consumer demonstrating Cooperative Sticky Assignor for incremental rebalancing.
 * 
 * What is Cooperative Sticky Assignor?
 * - Modern partition assignment strategy (Kafka 2.4+)
 * - Performs INCREMENTAL rebalancing (only reassigns affected partitions)
 * - Consumers KEEP their existing partitions during rebalance
 * - More efficient than eager rebalancing (stop-the-world)
 * 
 * How it works:
 * - One consumer: gets ALL partitions [0, 1, 2]
 * - Two consumers: distribution like [0, 1] and [2]
 * - Three consumers: even distribution [0], [1], [2]
 * - When consumer leaves: ONLY its partitions are reassigned
 * 
 * Key Features:
 * - Incremental rebalancing (not stop-the-world)
 * - Sticky: tries to keep previous assignments
 * - Graceful shutdown with consumer.wakeup()
 * - No unnecessary partition movement
 * 
 * Before running:
 * 1. Start Kafka in Docker: docker-compose up -d
 * 2. Ensure topic 'demo_topic_example' exists with 3 partitions
 * 3. Run MULTIPLE instances of this consumer to see rebalancing
 * 
 * Why Cooperative Sticky matters:
 * - Reduces rebalancing downtime
 * - Preserves partition assignments when possible
 * - Better for large consumer groups
 * - Default in modern Kafka clients
 */
public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {

        log.info("🚀 Starting Kafka Consumer with Cooperative Sticky Assignor");
        log.info("🔄 Demonstrating incremental rebalancing");
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
        
        // CRITICAL: Cooperative Sticky Assignor for incremental rebalancing
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        
        // Optional: Static group membership (for even faster rebalancing)
        // properties.setProperty("group.instance.id", "consumer-instance-1");
        
        // Auto-commit offsets (simpler for demo)
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("auto.commit.interval.ms", "1000");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Track consumer instance identifier
        final String instanceId = String.valueOf(Thread.currentThread().getId());
        log.info("📌 Consumer instance ID: {}", instanceId);

        // ── Graceful Shutdown Setup ─────────────────────────────────────────
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("⚠️ Shutdown signal detected!");
            log.info("   Calling consumer.wakeup() to interrupt poll()...");
            consumer.wakeup();  // Interrupts the poll() method
            
            try {
                log.info("   Waiting for main thread to finish...");
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            // Subscribe to topic
            consumer.subscribe(Arrays.asList(topic));
            log.info("✅ Subscribed to topic: {}", topic);
            log.info("   Using partition assignment strategy: CooperativeStickyAssignor");
            log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

            // Poll loop
            int pollCount = 0;
            while (true) {
                pollCount++;
                log.debug("Polling... (call #{})", pollCount);

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    log.info("📨 Received {} message(s)", records.count());
                    
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                        log.info("📬 Message from partition: {}", record.partition());
                        log.info("   Key:       {}", record.key());
                        log.info("   Value:     {}", record.value());
                        log.info("   Offset:    {}", record.offset());
                        log.info("   Timestamp: {}", record.timestamp());
                        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                    }
                } else {
                    // Log every 10th empty poll to show we're alive
                    if (pollCount % 10 == 0) {
                        log.info("💓 Heartbeat: No messages received, still polling...");
                    }
                }
            }

        } catch (WakeupException e) {
            log.info("👋 WakeupException caught - starting graceful shutdown");
        } catch (Exception e) {
            log.error("❌ Unexpected exception in consumer loop", e);
        } finally {
            log.info("🔒 Closing consumer...");
            consumer.close();  // Commits offsets and leaves consumer group
            log.info("✅ Consumer closed successfully");
        }

        log.info("🏁 Consumer finished");
    }
}