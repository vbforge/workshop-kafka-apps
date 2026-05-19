package com.vbforge.scenario_02_demo_app;

import com.vbforge.config.KafkaConfig;
import com.vbforge.config.Utility;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import java.util.concurrent.Future;
 
import static com.vbforge.config.Constants.TOPIC_DEMO;
 
/**
 * MyProducer — simplest possible producer.
 *
 * Demonstrates:
 *  - Minimum viable producer setup via KafkaConfig
 *  - Synchronous send with .get() — wait for broker confirmation before continuing
 *  - Reading RecordMetadata (topic, partition, offset, timestamp)
 *
 * Synchronous send is used here intentionally for clarity:
 * you can read the result immediately on the next line.
 * See SimpleProducer (scenario_02) for the async callback pattern.
 */
public class MyProducer {
 
    private static final Logger logger = LoggerFactory.getLogger(MyProducer.class);
 
    public static void main(String[] args) {
 
        logger.info("=== Quick Demo App Producer ===");
        Utility.verifyConfiguration();
 
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.createProducerConfig())) {
 
            // key=null → Kafka assigns partition via round-robin
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC_DEMO, null, "Hello from Quick Demo App!");
 
            logger.info("Sending message to topic: {}", TOPIC_DEMO);
 
            // Synchronous send: blocks until broker acknowledges
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();
 
            logger.info("Message sent successfully!");
            logger.info("  Topic:     {}", metadata.topic());
            logger.info("  Partition: {}", metadata.partition());
            logger.info("  Offset:    {}", metadata.offset());
            logger.info("  Timestamp: {}", metadata.timestamp());
 
        } catch (Exception e) {
            logger.error("Failed to send message: {}", e.getMessage(), e);
        }

    }
}