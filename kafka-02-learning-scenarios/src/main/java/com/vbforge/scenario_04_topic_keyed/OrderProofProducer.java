package com.vbforge.scenario_04_topic_keyed;

import com.vbforge.config.KafkaConfig;
import com.vbforge.config.Utility;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import java.util.concurrent.Future;
 
import static com.vbforge.config.Constants.*;
 
/**
 * OrderProofProducer — proves that per-key ordering is guaranteed.
 *
 * Sends a fixed, sequential workflow for each user (Login → Browse → ... → Logout).
 * Uses synchronous send (.get()) deliberately — this ensures messages are written
 * to Kafka in the exact order they are sent, with no possibility of reordering
 * between send() calls.
 *
 * WHY SYNCHRONOUS HERE:
 *  - Async send is faster but the callback fires after acknowledgement,
 *    meaning a second send() can be in-flight before the first is confirmed.
 *  - With idempotent producer (enable.idempotence=true) async sends are still
 *    safe from duplicates, but for a demo where "see the exact order" matters,
 *    synchronous send makes the sequence unambiguous.
 *  - In production you would use async + idempotence for throughput; here
 *    we sacrifice throughput for observability.
 *
 * Run AFTER starting KeyedConsumer instances.
 * Observe: each user's steps arrive at the consumer in exact order (1→2→3→4→5).
 * They may interleave across users (user-123 step 1, user-456 step 1, user-123 step 2...)
 * but within a single user the sequence is always preserved.
 */
public class OrderProofProducer {
 
    private static final Logger logger = LoggerFactory.getLogger(OrderProofProducer.class);
 
    private static final String[] USERS = {"user-123", "user-456", "user-789"};
 
    // A realistic user journey — order matters
    private static final String[] WORKFLOW = {
            "Step 1: Login",
            "Step 2: Browse",
            "Step 3: Add to Cart",
            "Step 4: Checkout",
            "Step 5: Logout"
    };
 
    public static void main(String[] args) {
 
        logger.info("=== Order Proof Producer ===");
        logger.info("Sending fixed sequential workflow per user");
        logger.info("Synchronous send used — guarantees exact write order per key");
        Utility.verifyConfiguration();
 
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.createProducerConfig())) {
 
            for (String user : USERS) {
                logger.info("--- Sending workflow for: {} ---", user);
 
                for (String step : WORKFLOW) {
                    ProducerRecord<String, String> record =
                            new ProducerRecord<>(TOPIC_KEYED, user, step);
 
                    // Synchronous send: .get() blocks until broker confirms this message
                    // before moving to the next step — order is guaranteed
                    Future<RecordMetadata> future = producer.send(record);
                    RecordMetadata metadata = future.get();
 
                    logger.info("  Sent | key: {} | value: {} | partition: {} | offset: {}",
                            user, step, metadata.partition(), metadata.offset());
                }
 
                logger.info("  Workflow complete for {}", user);
                Thread.sleep(DEFAULT_DELAY_MS);
            }
 
            logger.info("All workflows sent.");
            logger.info("Verify: each user's steps appear at the consumer in order 1→2→3→4→5.");
 
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Producer interrupted");
        } catch (Exception e) {
            logger.error("Producer failed: {}", e.getMessage(), e);
        }
    }
}