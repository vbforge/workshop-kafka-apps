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
 * PaymentService — processes payments for incoming orders.
 *
 * Consumer group: CONSUMER_GROUP_ECOMMERCE_PAYMENTS (independent of other services).
 * Each service has its own group → each receives ALL orders independently.
 * This is the broadcast pattern: one topic, N microservices, each with full visibility.
 *
 * Commit strategy: manual commitSync() after each successful batch (at-least-once).
 * A payment failure does NOT commit — the order will be redelivered.
 * ~5% of payments simulate a gateway timeout to demonstrate error handling.
 *
 * STOP: Ctrl+C in terminal only.
 */
public class PaymentService {


    private static final Logger logger = LoggerFactory.getLogger(PaymentService.class);
    public static final double FAIL_SIMULATION = 0.05; //adjustable (possible to get down: 0.01 --> ~1%)

    private KafkaConsumer<String, String> consumer;
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong failedCount    = new AtomicLong(0);
    private long startTime;

    public static void main(String[] args) {
        new PaymentService().run();
    }

    private void run() {

        logger.info("======== Payment Service ========");
        Utility.verifyConfiguration();

        consumer = new KafkaConsumer<>(KafkaConfig.createManualCommitConsumerConfig(CONSUMER_GROUP_ECOMMERCE_PAYMENTS));

        Thread mainThread = Thread.currentThread();
        registerShutdownHook(mainThread);

        startTime = System.currentTimeMillis();

        try {
            consumer.subscribe(Collections.singletonList(TOPIC_ECOMMERCE_ORDERS));
            logger.info("Subscribed to: {} | group: {}", TOPIC_ECOMMERCE_ORDERS, CONSUMER_GROUP_ECOMMERCE_PAYMENTS);
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
                        processPayment(order);
                        processedCount.incrementAndGet();
                        logger.info("Payment OK | id: {} | amount: ${} | user: {} | total processed: {}",
                                order.getOrderId(), String.format("%.2f", order.getTotalAmount()),
                                order.getUserId(), processedCount.get());

                    } catch (PaymentException e) {
                        failedCount.incrementAndGet();
                        logger.error("Payment FAILED | key: {} | reason: {} — batch not committed",
                                record.key(), e.getMessage());
                        batchSucceeded = false;
                        break;
                    } catch (Exception e) {
                        logger.error("Unexpected deserialization error for key {}: {}", record.key(), e.getMessage(), e);
                        batchSucceeded = false;
                        break;
                    }
                }

                if (batchSucceeded) {
                    consumer.commitSync();
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
     * Simulates payment processing with ~5% gateway timeout rate.
     */
    private void processPayment(Order order) throws PaymentException {
        try {
            Thread.sleep(PAYMENT_PROCESSING_DELAY_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (Math.random() < FAIL_SIMULATION) { //simulation ~5% fail
            throw new PaymentException("Gateway timeout for order: " + order.getOrderId());
        }
        order.setStatus("PAID");
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
        }, "payment-service-shutdown-hook"));
    }

    private void printFinalStats() {
        long runtime = System.currentTimeMillis() - startTime;
        logger.info("==========================================");
        logger.info("PAYMENT SERVICE — FINAL STATISTICS:");
        logger.info("   Payments processed: {}", processedCount.get());
        logger.info("   Payments failed:    {}", failedCount.get());
        logger.info("   Total runtime:      {} ms", runtime);
        logger.info("==========================================");
    }
}
