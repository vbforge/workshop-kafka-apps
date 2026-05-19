package com.vbforge.scenario_02_demo_app;

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
 * MultiMessageProducer — sends multiple messages, one by one.
 *
 * Demonstrates:
 *  - Sending a batch of messages in a loop
 *  - Synchronous confirmation per message (easy to follow for demo purposes)
 *  - Observing round-robin partition distribution when key=null
 *
 * With 3 partitions on topic-demo and no key, Kafka distributes
 * messages across partitions — watch the partition numbers in the output.
 */
public class MultiMessageProducer {
 
    private static final Logger logger = LoggerFactory.getLogger(MultiMessageProducer.class);
 
    public static void main(String[] args) {
 
        logger.info("=== Multi-Message Producer ===");
        Utility.verifyConfiguration();
 
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.createProducerConfig())) {
 
            for (int i = 1; i <= DEFAULT_MESSAGE_COUNT; i++) {
                String message = "Message #" + i + " from MultiMessageProducer";
 
                // key=null → round-robin across partitions
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_DEMO, null, message);
 
                Future<RecordMetadata> future = producer.send(record);
                RecordMetadata metadata = future.get(); // synchronous — wait for ack
 
                logger.info("Sent: {} → partition: {}, offset: {}",
                        message, metadata.partition(), metadata.offset());
 
                Thread.sleep(DEFAULT_DELAY_MS);
            }
 
            logger.info("All {} messages sent successfully!", DEFAULT_MESSAGE_COUNT);
 
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Producer interrupted");
        } catch (Exception e) {
            logger.error("Producer failed: {}", e.getMessage(), e);
        }
    }
}