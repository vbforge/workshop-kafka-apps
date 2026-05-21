package com.vbforge.scenario_04_topic_keyed;

import com.vbforge.config.KafkaConfig;
import com.vbforge.config.Utility;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import static com.vbforge.config.Constants.*;
 
/**
 * KeyedProducer — sends user events with user IDs as message keys.
 *
 * Demonstrates:
 *  - Key-based partition routing: hash(key) % numPartitions → partition index
 *  - Same key always produces the same hash → same partition → same consumer
 *  - Different keys may land on different partitions (depends on hash distribution)
 *
 * With 3 users and 3 partitions, each user will typically land on a distinct
 * partition — making the routing visible in consumer output.
 *
 * Async callbacks are used (same as scenario_02_simple) so sending is non-blocking.
 * Producer exits automatically after all messages are sent.
 */
public class KeyedProducer {
 
    private static final Logger logger = LoggerFactory.getLogger(KeyedProducer.class);
 
    // Keys that drive partition routing in this scenario
    private static final String[] USERS = {"user-123", "user-456", "user-789"};
 
    private static final String[] ACTIONS = {
            "Login", "View Product", "Add to Cart",
            "View Profile", "Search", "Checkout", "Logout"
    };
 
    public static void main(String[] args) {
 
        logger.info("=== Keyed Producer ===");
        logger.info("Sending user events — user ID is the message key");
        logger.info("Key routing: hash(key) % numPartitions → partition index");
        Utility.verifyConfiguration();
 
        java.util.Random random = new java.util.Random();
 
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.createProducerConfig())) {
 
            for (int i = 1; i <= 30; i++) {
                String userId = USERS[random.nextInt(USERS.length)];
                String action = ACTIONS[random.nextInt(ACTIONS.length)];
                String value  = String.format("Event-%d: %s", i, action);
 
                // KEY is the user ID — this is what determines the partition
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC_KEYED, userId, value);
 
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("Sent | key: {} | value: {} | partition: {} | offset: {}",
                                userId, value, metadata.partition(), metadata.offset());
                    } else {
                        logger.error("Failed to send event for {}: {}", userId, exception.getMessage());
                    }
                });
 
                Thread.sleep(DEFAULT_DELAY_MS);
            }
 
            producer.flush(); // ensure all async sends complete before close
            logger.info("All 30 user events sent.");
            logger.info("Observe: each user ID always landed on the same partition.");
 
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Producer interrupted");
        } catch (Exception e) {
            logger.error("Producer failed: {}", e.getMessage(), e);
        }
    }
}