package com.vbforge.scenario_06_e_commerce_orders_app;

import com.vbforge.config.KafkaConfig;
import com.vbforge.config.Utility;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static com.vbforge.config.Constants.*;

/**
 * OrderService — produces new orders to topic-ecommerce-orders.
 *
 * Each order is serialized to JSON using the shared Utility.getObjectMapper()
 * (which has JavaTimeModule registered — handles LocalDateTime correctly).
 *
 * Message key = userId: guarantees all orders from the same user land on
 * the same partition → per-user ordering is preserved across all consumer services.
 *
 * Producer exits automatically after all orders are sent.
 * Start all consumer services BEFORE running this.
 */
public class OrderService {

    private static final Logger logger = LoggerFactory.getLogger(OrderService.class);

    private static final Random   RANDOM   = new Random();

    public static void main(String[] args) {

        logger.info("=== Order Service (Producer) ===");
        logger.info("Sending {} orders to topic: {}", DEFAULT_ORDER_COUNT, TOPIC_ECOMMERCE_ORDERS);
        Utility.verifyConfiguration();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.createProducerConfig())) {

            for (int i = 1; i <= DEFAULT_ORDER_COUNT; i++) {
                Order order = createRandomOrder(i);

                // Serialize to JSON — Utility.getObjectMapper() handles LocalDateTime via JavaTimeModule
                String orderJson = Utility.getObjectMapper().writeValueAsString(order);

                // Key = userId: same user → same partition → ordering per user guaranteed
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_ECOMMERCE_ORDERS, order.getUserId(), orderJson);

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("Order placed | id: {} | user: {} | partition: {} | offset: {}",
                                order.getOrderId(), order.getUserId(),
                                metadata.partition(), metadata.offset());
                    } else {
                        logger.error("Failed to place order {}: {}", order.getOrderId(), exception.getMessage());
                    }
                });

                Thread.sleep(ORDER_PRODUCER_DELAY_MS); // simulate real-world ordering rate (1 order/sec)
            }

            producer.flush(); // ensure all async sends complete before closing
            logger.info("All {} orders submitted.", DEFAULT_ORDER_COUNT);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("OrderService interrupted");
        } catch (Exception e) {
            logger.error("OrderService failed: {}", e.getMessage(), e);
        }
    }

    private static Order createRandomOrder(int orderNum) {
        String orderId   = String.format("ORD-%05d", orderNum);
        String userId    = USERS[RANDOM.nextInt(USERS.length)];
        String productId = PRODUCTS[RANDOM.nextInt(PRODUCTS.length)];
        int    quantity  = RANDOM.nextInt(3) + 1;
        double unitPrice = 100 + RANDOM.nextDouble() * 900;
        double total     = unitPrice * quantity;
        return new Order(orderId, userId, productId, quantity, total, "PENDING");
    }
}