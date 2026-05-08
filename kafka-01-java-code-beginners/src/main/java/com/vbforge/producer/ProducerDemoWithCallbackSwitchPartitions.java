package com.vbforge.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Producer demonstrating Sticky Partitioning behavior.
 * 
 * What is Sticky Partitioning?
 * - Kafka batches messages to the SAME partition until the batch is full
 * - Then "sticks" to a NEW partition for subsequent batches
 * - More efficient than round-robin (fewer metadata requests)
 * 
 * Key Features:
 * - Sends 300 messages total (10 batches × 30 messages)
 * - Batch size set to 400 bytes (smaller than default 16KB)
 * - Shows when partition switches occur
 * - 500ms delay between batches for visibility
 * 
 * Expected behavior:
 * - First ~30 messages → Partition X (fills first batch)
 * - Next ~30 messages → Partition Y (may switch)
 * - Partition switches when batch size limit is reached
 * 
 * Before running:
 * 1. Start Kafka in Docker: docker-compose up -d
 * 2. Ensure topic 'demo_topic_example' exists with 3 partitions
 * 3. Run this producer
 * 
 * Why Sticky Partitioning matters:
 * - Reduces network overhead (fewer partition metadata requests)
 * - Improves throughput for high-volume producers
 * - Default behavior in modern Kafka (3.0+)
 */
public class ProducerDemoWithCallbackSwitchPartitions {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallbackSwitchPartitions.class.getSimpleName());

    public static void main(String[] args) {

        log.info("🚀 Starting Kafka Producer with Sticky Partitioning (Docker version)");
        log.info("📦 Demonstrating how Kafka batches messages to partitions");
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("⚙️  Configuration: batch.size = 400 bytes (small for demo)");
        log.info("📊 Sending 10 batches × 30 messages = 300 total messages");
        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        // Create Producer Properties
        Properties properties = new Properties();

        // Connect to Docker Kafka on localhost:9092
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        
        // CRITICAL: Small batch size to see partition switching
        // Default is 16384 bytes (16KB). With 400 bytes, batch fills quickly!
        properties.setProperty("batch.size", "400");
        
        // Optional: Add reliability settings
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "3");
        properties.setProperty("enable.idempotence", "true");
        
        // Linger.ms = 0 (send immediately when batch is full or queue is empty)
        properties.setProperty("linger.ms", "0");

        // Track partition switches for demonstration
        int[] currentPartition = {-1};
        int batchCount = 0;
        java.util.Map<Integer, Integer> partitionMessageCount = new java.util.HashMap<>();

        // Create the producer (using try-with-resources for auto-close)
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            // 10 batches of 30 messages = 300 total
            for (int batch = 1; batch <= 10; batch++) {
                log.info("");
                log.info("📦 BATCH {} of 10 starting...", batch);
                
                for (int i = 1; i <= 30; i++) {
                    int messageNumber = (batch - 1) * 30 + i;
                    String message = "Message #" + messageNumber + " (batch:" + batch + ", item:" + i + ")";
                    
                    // Create producer record (no key for sticky partitioning demo)
                    ProducerRecord<String, String> producerRecord = 
                        new ProducerRecord<>("demo_topic_example", message);

                    final int finalBatch = batch;
                    final int finalMsgNum = messageNumber;
                    
                    // Send data with callback to track partition assignments
                    int finalI = i;
                    producer.send(producerRecord, (metadata, exception) -> {
                        if (exception == null) {
                            int partition = metadata.partition();
                            
                            // Track partition distribution
                            partitionMessageCount.merge(partition, 1, Integer::sum);
                            
                            // Detect partition switch
                            if (currentPartition[0] != -1 && currentPartition[0] != partition) {
                                log.info("   🔄 PARTITION SWITCH: {} → {} at message #{}", 
                                    currentPartition[0], partition, finalMsgNum);
                            }
                            currentPartition[0] = partition;
                            
                            // Log first few messages of each batch
                            if (finalI <= 3 || finalI >= 28) {
                                log.info("   📝 Msg #{} | Partition: {} | Offset: {} | Batch: {}",
                                    finalMsgNum, partition, metadata.offset(), finalBatch);
                            } else if (finalI == 15) {
                                log.info("   ... (messages 4-27 omitted for brevity) ...");
                            }
                        } else {
                            log.error("❌ Failed to send message #{}", finalMsgNum, exception);
                        }
                    });
                }
                
                log.info("   ✅ Batch {} sent, waiting 500ms...", batch);
                
                // Wait between batches to clearly see partition switching
                Thread.sleep(500);
            }

            // Flush any remaining messages and wait for completion
            log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            log.info("🔄 Flushing producer...");
            producer.flush();
            
            // Give callbacks time to complete
            Thread.sleep(2000);
            
        } catch (InterruptedException e) {
            log.error("Thread interrupted", e);
            Thread.currentThread().interrupt();
        }

        log.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        log.info("🏁 Producer finished");
        log.info("📊 Sticky Partitioning Summary:");
        log.info("   Messages were batched to the SAME partition until batch filled");
        log.info("   When batch.size limit reached, partitioner 'sticks' to a NEW partition");
        log.info("🎯 Key insight: Batching improves throughput by reducing metadata requests!");
    }
}