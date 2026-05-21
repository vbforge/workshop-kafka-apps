package com.vbforge.scenario_05_manual_offset_management;

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
 * ManualCommitConsumer — demonstrates at-least-once delivery with commitSync().
 *
 * Flow per poll batch:
 *  1. poll() returns records — no commit yet
 *  2. processOrder() is called for each record
 *  3. If processing succeeds → commitSync() is called AFTER the full batch
 *  4. If processing fails → we log the error and do NOT commit
 *     → on the next poll() Kafka re-delivers the failed record (and everything after it)
 *
 * THE GUARANTEE — at-least-once delivery:
 *  Messages are never lost — but they may be processed more than once
 *  if a crash happens after processing but before commitSync() returns.
 *
 * THE TRADEOFF:
 *  commitSync() blocks until the broker confirms the commit — this adds latency.
 *  For high-throughput scenarios use BatchCommitConsumer (commits every N records)
 *  or commitAsync() (non-blocking, but needs careful error handling).
 *
 * IMPORTANT: because we commit the whole batch offset after processing all records,
 *  a failure on record 3 of 10 means records 1 and 2 (already processed) will be
 *  redelivered on restart. Your processing logic must be idempotent to handle this.
 *
 * STOP: Ctrl+C in terminal only.
 */
public class ManualCommitConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ManualCommitConsumer.class);

    private KafkaConsumer<String, String> consumer;
    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong failedCount    = new AtomicLong(0);
    private long startTime;

    public static void main(String[] args) {
        new ManualCommitConsumer().run();
    }

    private void run() {

        logger.info("=== Manual Commit Consumer ===");
        logger.info("Offsets committed only after successful processing — at-least-once guarantee");
        Utility.verifyConfiguration();

        consumer = new KafkaConsumer<>(KafkaConfig.createManualCommitConsumerConfig(CONSUMER_GROUP_MANUAL_OFFSET));

        Thread mainThread = Thread.currentThread();
        registerShutdownHook(mainThread);

        startTime = System.currentTimeMillis();

        try {
            consumer.subscribe(Collections.singletonList(TOPIC_MANUAL_OFFSET));
            logger.info("Subscribed to: {} | group: {}", TOPIC_MANUAL_OFFSET, CONSUMER_GROUP_MANUAL_OFFSET);
            logger.info("Ctrl+C to stop");

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(DEFAULT_POLL_TIMEOUT_MS));

                if (records.isEmpty()) {
                    continue;
                }

                boolean batchSucceeded = true;

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        processOrder(record);
                        processedCount.incrementAndGet();
                        logger.info("OK | key: {} | partition: {} | offset: {} | total: {}",
                                record.key(), record.partition(), record.offset(), processedCount.get());

                    } catch (OrderProcessingException e) {
                        failedCount.incrementAndGet();
                        logger.error("FAILED | key: {} | reason: {} — batch will NOT be committed",
                                record.key(), e.getMessage());
                        batchSucceeded = false;
                        // Stop processing remaining records in this batch.
                        // On the next poll(), Kafka re-delivers from the last committed offset.
                        break;
                    }
                }

                if (batchSucceeded) {
                    // Commit only after ALL records in the batch are processed successfully.
                    // commitSync() blocks until the broker confirms — safe but adds latency.
                    consumer.commitSync();
                    logger.info("Batch committed ({} records)", records.count());
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
     * Simulates order processing with an occasional random failure (~10% chance).
     * Throws a domain-specific exception so the caller can distinguish
     * a business failure from an unexpected runtime error.
     */
    private void processOrder(ConsumerRecord<String, String> record) throws OrderProcessingException {
        try {
            Thread.sleep(500); // simulate DB write or API call
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (Math.random() < 0.1) {
            throw new OrderProcessingException("Simulated failure for order: " + record.key());
        }

        logger.debug("Order processed: {} | value: {}", record.key(), record.value());
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
        }, "manual-commit-consumer-shutdown-hook"));
    }

    private void printFinalStats() {
        long runtime = System.currentTimeMillis() - startTime;
        logger.info("==============================================");
        logger.info("FINAL STATISTICS:");
        logger.info("   Successfully processed: {}", processedCount.get());
        logger.info("   Failed (not committed):  {}", failedCount.get());
        logger.info("   Total runtime:           {} ms", runtime);
        logger.info("==============================================");
    }
}
