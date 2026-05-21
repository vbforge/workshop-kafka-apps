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
import java.util.stream.Collectors;

import static com.vbforge.config.Constants.*;

/**
 * AnalyticsService — tracks metrics and generates real-time reports.
 *
 * Consumer group: analytics-service-group (independent of other services).
 * Each service has its own group → each receives ALL orders independently (broadcast pattern).
 *
 * Uses auto-commit for simplicity since analytics processing is idempotent.
 *
 * STOP: Ctrl+C in terminal only.
 */
public class AnalyticsService {

    private static final Logger logger = LoggerFactory.getLogger(AnalyticsService.class);

    private final AtomicLong totalOrders = new AtomicLong(0);
    private final AtomicLong totalRevenue = new AtomicLong(0);
    private final Map<String, Long> productSales = new HashMap<>();
    private final Map<String, Long> customerOrders = new HashMap<>();

    private KafkaConsumer<String, String> consumer;
    private long startTime;

    public static void main(String[] args) {
        new AnalyticsService().run();
    }

    private void run() {
        logger.info("======== Analytics Service ========");
        Utility.verifyConfiguration();

        consumer = new KafkaConsumer<>(KafkaConfig.createConsumerConfig(CONSUMER_GROUP_ECOMMERCE_ANALYTICS));

        Thread mainThread = Thread.currentThread();
        registerShutdownHook(mainThread);

        startTime = System.currentTimeMillis();

        try {
            consumer.subscribe(Collections.singletonList(TOPIC_ECOMMERCE_ORDERS));
            logger.info("Subscribed to: {} | group: {}", TOPIC_ECOMMERCE_ORDERS, CONSUMER_GROUP_ECOMMERCE_ANALYTICS);
            logger.info("Ctrl+C to stop");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(DEFAULT_POLL_TIMEOUT_MS));

                if (records.isEmpty()) {
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Order order = Utility.getObjectMapper().readValue(record.value(), Order.class);
                        updateAnalytics(order);

                        long currentOrders = totalOrders.incrementAndGet();
                        long currentRevenue = totalRevenue.addAndGet((long) (order.getTotalAmount() * 100));

                        logger.info("Analytics updated | order: {} | user: {} | amount: ${}",
                                order.getOrderId(), order.getUserId(),
                                String.format("%.2f", order.getTotalAmount()));

                    } catch (Exception e) {
                        logger.error("Analytics update failed for key {}: {}", record.key(), e.getMessage(), e);
                    }
                }

                if (!records.isEmpty()) {
                    printAnalytics();
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
     * Updates all analytics counters atomically.
     */
    private void updateAnalytics(Order order) {
        // Product sales: track quantity per product
        productSales.merge(order.getProductId(), (long) order.getQuantity(), Long::sum);

        // Customer orders: track count per customer
        customerOrders.merge(order.getUserId(), 1L, Long::sum);
    }

    /**
     * Prints the current analytics dashboard.
     */
    private void printAnalytics() {
        long orderCount = totalOrders.get();
        double revenueCents = totalRevenue.get();
        double revenue = revenueCents / 100.0;

        logger.info("=======================================");
        logger.info("=========ANALYTICS DASHBOARD===========");
        logger.info("=======================================");
        logger.info("===Total Orders: {}", orderCount);
        logger.info("===Total Revenue: ${}", String.format("%.2f", revenue));
        logger.info("===Average Order Value: ${}",
                orderCount > 0 ? String.format("%.2f", revenue / orderCount) : "0.00");

        logger.info("\nTop Products:");
        productSales.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(3)
                .forEach(e -> logger.info("  - {}: {} units", e.getKey(), e.getValue()));

        logger.info("\nTop Customers:");
        customerOrders.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(3)
                .forEach(e -> logger.info("  - {}: {} orders", e.getKey(), e.getValue()));

        logger.info("==========================================\n");
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
        }, "analytics-service-shutdown-hook"));
    }

    private void printFinalStats() {
        long runtime = System.currentTimeMillis() - startTime;
        long orderCount = totalOrders.get();
        double revenue = totalRevenue.get() / 100.0;

        logger.info("==========================================");
        logger.info("ANALYTICS SERVICE — FINAL STATISTICS:");
        logger.info("   Total orders processed: {}", orderCount);
        logger.info("   Total revenue:          ${}", String.format("%.2f", revenue));
        logger.info("   Average order value:    ${}",
                orderCount > 0 ? String.format("%.2f", revenue / orderCount) : "0.00");
        logger.info("   Total runtime:          {} ms", runtime);
        logger.info("==========================================");
    }
}


