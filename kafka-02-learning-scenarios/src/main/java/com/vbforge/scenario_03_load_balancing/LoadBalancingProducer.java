package com.vbforge.scenario_03_load_balancing;

import com.vbforge.config.KafkaConfig;
import com.vbforge.config.Utility;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vbforge.config.Constants.*;

/**
 * LoadBalancingProducer — sends messages with no key (null) to a 3-partition topic.
 *
 * Demonstrates:
 *  - Round-robin partition distribution when key=null
 *  - Kafka spreads messages across available partitions automatically
 *  - With 30 messages and 3 partitions you expect ~10 messages per partition
 *    (not exactly equal — Kafka batches and may not perfectly round-robin at
 *    low throughput, but distribution will be visible)
 *
 * Run AFTER starting the consumers so you can watch the distribution live.
 * Producer exits automatically after sending all messages.
 */
public class LoadBalancingProducer {

    private static final Logger logger = LoggerFactory.getLogger(LoadBalancingProducer.class);

    public static void main(String[] args) {

        logger.info("=== Load Balancing Producer ===");
        logger.info("Sending {} messages to topic: {}", LOAD_BALANCE_MESSAGE_COUNT, TOPIC_LOAD_BALANCE);
        Utility.verifyConfiguration();

        try(KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.createProducerConfig())){

            for (int i = 1; i <= LOAD_BALANCE_MESSAGE_COUNT; i++) {

                String value = "Task - " + i;

                // key=null → Kafka distributes across partitions (round-robin within a batch)
                // This is intentional: we want to observe partition spread, not key routing
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_LOAD_BALANCE, null, value);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("Sent: {} → partition: {}, offset: {}", value, metadata.partition(), metadata.offset());
                    } else {
                        logger.error("Failed to send {}: {}", value, exception.getMessage());
                    }
                });

                Thread.sleep(DEFAULT_DELAY_MS);

            }

            producer.flush(); // ensure all async sends complete before closing
            logger.info("All {} messages sent.", LOAD_BALANCE_MESSAGE_COUNT);

        }catch (InterruptedException e){
            Thread.currentThread().interrupt();
            logger.warn("Producer failed: {}", e.getMessage(), e);

        }catch (Exception e){
            logger.error("Producer failed: {}", e.getMessage(), e);

        }


    }


}






