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
 * KeyedMessageProducer — demonstrates message key routing.
 *
 * Demonstrates:
 *  - Messages with the same key always go to the same partition
 *  - This is Kafka's ordering guarantee: per-key ordering is preserved
 *  - Different keys may land on different partitions (determined by hash of key)
 *
 * Watch the output: all "user-A" messages share one partition,
 * all "user-B" share another, etc.
 * This is the foundation for scenario_04 (keyed messages deep-dive).
 */
public class KeyedMessageProducer {
 
    private static final Logger logger = LoggerFactory.getLogger(KeyedMessageProducer.class);
 
    public static void main(String[] args) {
 
        logger.info("=== Keyed Message Producer ===");
        logger.info("Messages with the same key always go to the same partition");
        Utility.verifyConfiguration();
 
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.createProducerConfig())) {
 
            String[] keys = {"user-A", "user-B", "user-D", "user-A", "user-C", "user-F", "user-D", "user-B", "user-A", "user-T"};
 
            for (int i = 0; i < keys.length; i++) {
                String key   = keys[i];
                String value = "Action #" + (i + 1) + " from " + key;
 
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_DEMO, key, value);
 
                Future<RecordMetadata> future = producer.send(record);
                RecordMetadata metadata = future.get();
 
                logger.info("Key: {} | Value: {} | Partition: {}",
                        key, value, metadata.partition());
 
                Thread.sleep(DEFAULT_DELAY_MS);
            }
 
            logger.info("Done — notice all messages with the same key landed on the same partition");
 
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Producer interrupted");
        } catch (Exception e) {
            logger.error("Producer failed: {}", e.getMessage(), e);
        }
    }
}