package com.vbforge.scenario_05_manual_offset_management;

import com.vbforge.config.KafkaConfig;
import com.vbforge.config.Utility;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vbforge.config.Constants.*;

/**
 * OrderProducer — sends 20 simulated orders to topic-orders.
 *
 * Each message uses the order ID as the key — this ensures all events
 * for the same order always go to the same partition (ordering per order).
 *
 * Message value is a minimal JSON string to simulate a real order payload.
 * In scenario_06 this will be a proper serialized object.
 *
 * Producer exits automatically after all orders are sent.
 */
public class OrderProducer {

    private static final Logger logger = LoggerFactory.getLogger(OrderProducer.class);

    private static final int ORDER_COUNT = 20;

    public static void main(String[] args) {

        logger.info("=== Order Producer ===");
        logger.info("Sending {} orders to topic: {}", ORDER_COUNT, TOPIC_MANUAL_OFFSET);
        Utility.verifyConfiguration();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.createProducerConfig())) {

            for (int i = 1; i <= ORDER_COUNT; i++) {
                String orderId   = String.format("ORDER-%03d", i);
                String orderData = String.format(
                        "{\"orderId\":\"%s\", \"amount\":%.2f}", orderId, 100.0 + (i * 10));

                // Key = orderId: all events for the same order land on the same partition
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(TOPIC_MANUAL_OFFSET, orderId, orderData);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("Sent | key: {} | partition: {} | offset: {}",
                                orderId, metadata.partition(), metadata.offset());
                    } else {
                        logger.error("Failed to send {}: {}", orderId, exception.getMessage());
                    }
                });

                Thread.sleep(DEFAULT_DELAY_MS);
            }

            producer.flush(); // wait for all async sends before closing
            logger.info("All {} orders sent.", ORDER_COUNT);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Producer interrupted");
        } catch (Exception e) {
            logger.error("Producer failed: {}", e.getMessage(), e);
        }
    }
}
