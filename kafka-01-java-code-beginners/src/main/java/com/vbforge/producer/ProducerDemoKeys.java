package com.vbforge.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Producer demonstrating key-based message routing to partitions.
 * 
 * Key Concept: Messages with the same key ALWAYS go to the same partition.
 * Kafka uses: partition = hash(key) % numPartitions
 * 
 * Features:
 * - Sends 20 messages (2 rounds × 10 messages)
 * - Uses keys: "id_1" through "id_10"
 * - Logs which partition each key maps to
 * - Shows that same key = same partition across batches
 * 
 * Before running:
 * 1. Start Kafka in Docker: docker-compose up -d
 * 2. Ensure topic 'demo_topic_example' exists with 3 partitions
 * 3. Run this producer
 * 
 * Expected behavior:
 * - Each unique key goes to a specific partition (e.g., id_1 → partition 0)
 * - Same key in second batch goes to EXACTLY same partition
 * - Different keys may go to different partitions
 */
public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {

        log.info("🚀 Starting Kafka Producer with Keys");
        log.info("📚 Demonstrating key-based partition routing");
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

        // Create the producer (using try-with-resources for auto-close)
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            // Store partition mapping to show consistency
            Map<String, Integer> keyPartitionMap = new HashMap<>();

            // Send 2 batches of 10 messages each (total 20 messages)
            for (int batch = 1; batch <= 2; batch++) {
                log.info("📦 BATCH {} of 2", batch);
                
                for (int i = 1; i <= 10; i++) {
                    // Create key and value
                    String topic = "demo_topic_example";
                    String key = "id_" + i;
                    String value = "Hello from " + key + " - batch " + batch;

                    // Create producer record with key
                    ProducerRecord<String, String> producerRecord = 
                        new ProducerRecord<>(topic, key, value);

                    // Send data asynchronously with callback
                    final String finalKey = key;
                    final int currentBatch = batch;
                    
                    producer.send(producerRecord, (metadata, exception) -> {
                        if (exception == null) {
                            // Success: log partition information
                            int partition = metadata.partition();
                            
                            // Store first seen partition for this key
                            keyPartitionMap.putIfAbsent(finalKey, partition);
                            int expectedPartition = keyPartitionMap.get(finalKey);
                            
                            // Check if key continues to go to same partition
                            boolean consistent = (partition == expectedPartition);
                            String consistencyMark = consistent ? "✅" : "❌";
                            
                            log.info("   {} Key: {} → Partition: {} | Offset: {} | Batch: {}",
                                consistencyMark, finalKey, partition, metadata.offset(), currentBatch);
                            
                            if (!consistent) {
                                log.error("   ⚠️ KEY MISMATCH! {} went to partition {} but was expected to go to {}",
                                    finalKey, partition, expectedPartition);
                            }
                        } else {
                            log.error("❌ Failed to send message with key: {}", finalKey, exception);
                        }
                    });
                    
                    // Small delay to see logs clearly
                    Thread.sleep(50);
                }
                
                log.info("   ✅ Batch {} completed", batch);
                
                // Separate batches with a pause
                if (batch < 2) {
                    Thread.sleep(1000);
                    log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
                }
            }

            // Flush and close (try-with-resources handles close automatically)
            producer.flush();
            Thread.sleep(1000); // Wait for final callbacks to complete
            
        } catch (InterruptedException e) {
            log.error("Thread interrupted", e);
            Thread.currentThread().interrupt();
        }

        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("🏁 Producer finished");
        log.info("🎯 Key insight: Same key → Same partition across all batches!");
    }
}