package com.vbforge.scenario_06_e_commerce_orders_app;


import com.vbforge.config.KafkaConfig;
import com.vbforge.config.Utility;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.vbforge.config.Constants.*;

/**
 * InventoryService — updates stock levels for incoming orders.
 *
 * Consumer group: inventory-service-group (independent of other services).
 * Each service has its own group → each receives ALL orders independently (broadcast pattern).
 *
 * Commit strategy: manual commitSync() after each successful batch (at-least-once).
 * If inventory update fails (insufficient stock), the batch is NOT committed —
 * the order will be redelivered on next poll.
 *
 * STOP: Ctrl+C in terminal only.
 */
public class InventoryService {

    private static final Logger logger = LoggerFactory.getLogger(InventoryService.class);

    private static final Map<String, Integer> inventory = new HashMap<>();

    static {
        // Initialize inventory
        inventory.put(LAPTOP, LAPTOP_STOCK);
        inventory.put(PHONE, PHONE_STOCK);
        inventory.put(TABLET, TABLET_STOCK);
        inventory.put(HEADPHONES, HEADPHONES_STOCK);
        inventory.put(MONITOR, MONITOR_STOCK);
    }

    private KafkaConsumer<String, String> consumer;
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);
    private long startTime;

    public static void main(String[] args) {
        new InventoryService().run();
    }

    private void run() {
        logger.info("======== Inventory Service ========");
        Utility.verifyConfiguration();
        printInventory();

        consumer = new KafkaConsumer<>(KafkaConfig.createManualCommitConsumerConfig(CONSUMER_GROUP_ECOMMERCE_INVENTORY));

        Thread mainThread = Thread.currentThread();
        registerShutdownHook(mainThread);

        startTime = System.currentTimeMillis();

        try {
            consumer.subscribe(Collections.singletonList(TOPIC_ECOMMERCE_ORDERS));
            logger.info("Subscribed to: {} | group: {}", TOPIC_ECOMMERCE_ORDERS, CONSUMER_GROUP_ECOMMERCE_INVENTORY);
            logger.info("Ctrl+C to stop");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(DEFAULT_POLL_TIMEOUT_MS));

                if (records.isEmpty()) {
                    continue;
                }

                boolean batchSucceeded = true;

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Order order = Utility.getObjectMapper().readValue(record.value(), Order.class);

                        // Update inventory
                        updateInventory(order);

                        processedCount.incrementAndGet();
                        logger.info("Inventory updated | order: {} | product: {} | qty: -{} | remaining: {}",
                                order.getOrderId(), order.getProductId(), order.getQuantity(),
                                inventory.get(order.getProductId()));

                    } catch (InsufficientStockException e) {
                        failedCount.incrementAndGet();
                        logger.error("Inventory update FAILED | key: {} | reason: {} — batch not committed",
                                record.key(), e.getMessage());
                        batchSucceeded = false;
                        break;
                    } catch (Exception e) {
                        logger.error("Unexpected error for key {}: {}", record.key(), e.getMessage(), e);
                        batchSucceeded = false;
                        break;
                    }
                }

                if (batchSucceeded && !records.isEmpty()) {
                    consumer.commitSync();
                    printInventory();
                }
            }

        } catch (WakeupException e) {
            logger.info("WakeupException — shutting down");
        } catch (Exception e) {
            logger.error("Unexpected error: {}", e.getMessage(), e);
        } finally {
            consumer.close();
            logger.info("Consumer closed");
            printFinalStats();
        }
    }

    /**
     * Updates inventory for an order. Throws InsufficientStockException if stock is too low.
     */
    private void updateInventory(Order order) throws InsufficientStockException, InterruptedException {
        String product = order.getProductId();
        int currentStock = inventory.getOrDefault(product, 0);

        if (currentStock < order.getQuantity()) {
            throw new InsufficientStockException("Insufficient stock for " + product +
                    " (requested: " + order.getQuantity() + ", available: " + currentStock + ")");
        }

        inventory.put(product, currentStock - order.getQuantity());
        Thread.sleep(INVENTORY_UPDATE_DELAY_MS); // simulate inventory update latency
    }

    private void printInventory() {
        logger.info("\n--- Current Inventory ---");
        inventory.forEach((product, qty) ->
                logger.info("{}: {} units", product, qty));
        logger.info("");
    }

    private void registerShutdownHook(Thread mainThread) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received — calling consumer.wakeup()");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "inventory-service-shutdown-hook"));
    }

    private void printFinalStats() {
        long runtime = System.currentTimeMillis() - startTime;
        logger.info("==========================================");
        logger.info("INVENTORY SERVICE — FINAL STATISTICS:");
        logger.info("   Inventory updates: {}", processedCount.get());
        logger.info("   Updates failed:    {}", failedCount.get());
        logger.info("   Total runtime:     {} ms", runtime);
        logger.info("==========================================");
    }

    /**
     * Custom exception for inventory stock issues.
     */
    private static class InsufficientStockException extends Exception {
        public InsufficientStockException(String message) {
            super(message);
        }
    }
}
