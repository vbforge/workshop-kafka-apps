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
import java.util.concurrent.atomic.AtomicLong;

import static com.vbforge.config.Constants.*;

/**
 * NotificationService — sends order confirmations to users.
 *
 * Consumer group: CONSUMER_GROUP_ECOMMERCE_NOTIFICATIONS (independent of other services).
 * Each service has its own group → each receives ALL orders independently (broadcast pattern).
 *
 * Uses auto-commit for simplicity since notification failures are non-critical.
 *
 * STOP: Ctrl+C in terminal only.
 */
public class NotificationService {

    private static final Logger logger = LoggerFactory.getLogger(NotificationService.class);

    private KafkaConsumer<String, String> consumer;
    private final AtomicLong notificationCount = new AtomicLong(0);
    private long startTime;

    public static void main(String[] args) {
        new NotificationService().run();
    }

    private void run() {
        logger.info("======== Notification Service ========");
        Utility.verifyConfiguration();

        consumer = new KafkaConsumer<>(KafkaConfig.createConsumerConfig(CONSUMER_GROUP_ECOMMERCE_NOTIFICATIONS));

        Thread mainThread = Thread.currentThread();
        registerShutdownHook(mainThread);

        startTime = System.currentTimeMillis();

        try {
            consumer.subscribe(Collections.singletonList(TOPIC_ECOMMERCE_ORDERS));
            logger.info("Subscribed to: {} | group: {}", TOPIC_ECOMMERCE_ORDERS, CONSUMER_GROUP_ECOMMERCE_NOTIFICATIONS);
            logger.info("Ctrl+C to stop");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(DEFAULT_POLL_TIMEOUT_MS));

                if (records.isEmpty()) {
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        Order order = Utility.getObjectMapper().readValue(record.value(), Order.class);
                        sendNotification(order);
                        notificationCount.incrementAndGet();
                        logger.info("Notification sent | user: {} | order: {} | product: {} | amount: ${}",
                                order.getUserId(), order.getOrderId(), order.getProductId(),
                                String.format("%.2f", order.getTotalAmount()));

                    } catch (Exception e) {
                        logger.error("Notification failed for key {}: {}", record.key(), e.getMessage(), e);
                    }
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
     * Simulates sending a notification to the user.
     */
    private void sendNotification(Order order) throws InterruptedException {
        Thread.sleep(NOTIFICATION_DELAY_MS); // simulate notification delivery
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
        }, "notification-service-shutdown-hook"));
    }

    private void printFinalStats() {
        long runtime = System.currentTimeMillis() - startTime;
        logger.info("==========================================");
        logger.info("NOTIFICATION SERVICE — FINAL STATISTICS:");
        logger.info("   Notifications sent: {}", notificationCount.get());
        logger.info("   Total runtime:      {} ms", runtime);
        logger.info("==========================================");
    }
}
