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
 * BatchCommitConsumer — commits offsets every N messages for balanced throughput and safety.
 *
 * The problem with ManualCommitConsumer:
 *  commitSync() after every single batch is safe but slow —
 *  each commit is a round-trip to the broker and blocks the poll loop.
 *
 * The problem with AutoCommitConsumer:
 *  commits happen on a timer, independent of processing — messages can be lost.
 *
 * BatchCommitConsumer is the middle ground:
 *  - Process BATCH_COMMIT_SIZE records, then commit once
 *  - Fewer broker round-trips → better throughput
 *  - Still at-least-once: worst case is BATCH_COMMIT_SIZE records reprocessed on crash
 *
 * CHOOSING BATCH_COMMIT_SIZE:
 *  - Larger batches = higher throughput, larger reprocessing window on crash
 *  - Smaller batches = more commits, closer to record-by-record safety
 *  - Typical production values: 50–500 depending on processing latency
 *
 * STOP: Ctrl+C in terminal only.
 */
public class BatchCommitConsumer {

    private static final Logger logger = LoggerFactory.getLogger(BatchCommitConsumer.class);

    // How many records to process before committing
    private static final int BATCH_COMMIT_SIZE = 5;

    private KafkaConsumer<String, String> consumer;
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private long startTime;

    public static void main(String[] args) {
        new BatchCommitConsumer().run();
    }

    private void run() {

        logger.info("======== Batch Commit Consumer ========");
        logger.info("Commits every {} messages", BATCH_COMMIT_SIZE);
        Utility.verifyConfiguration();

        consumer = new KafkaConsumer<>(
                KafkaConfig.createManualCommitConsumerConfig(CONSUMER_GROUP_MANUAL_OFFSET + "-batch"));

        Thread mainThread = Thread.currentThread();
        registerShutdownHook(mainThread);

        startTime = System.currentTimeMillis();

        try {
            consumer.subscribe(Collections.singletonList(TOPIC_MANUAL_OFFSET));
            logger.info("Subscribed to: {} | group: {}", TOPIC_MANUAL_OFFSET, CONSUMER_GROUP_MANUAL_OFFSET + "-batch");
            logger.info("Ctrl+C to stop");

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(DEFAULT_POLL_TIMEOUT_MS));

                for (ConsumerRecord<String, String> record : records) {
                    processOrder(record);
                    long count = totalProcessed.incrementAndGet();

                    logger.info("Processed #{} | key: {} | partition: {} | offset: {}",
                            count, record.key(), record.partition(), record.offset());

                    // Commit after every BATCH_COMMIT_SIZE records
                    if (count % BATCH_COMMIT_SIZE == 0) {
                        consumer.commitSync();
                        logger.info("--- Batch commit at record #{} ---", count);
                    }
                }
            }

        } catch (WakeupException e) {
            logger.info("WakeupException — shutting down");
        } catch (Exception e) {
            logger.error("Unexpected error: {}", e.getMessage(), e);
        } finally {
            // Commit any records processed since the last batch commit.
            // This runs on the main thread (same thread that owns the consumer) — correct.
            // Without this, up to BATCH_COMMIT_SIZE-1 records would be reprocessed on restart.
            try {
                consumer.commitSync();
                logger.info("Final commit on shutdown — {} records total", totalProcessed.get());
            } catch (Exception e) {
                logger.warn("Final commit failed: {}", e.getMessage());
            }
            consumer.close();
            logger.info("Consumer closed");
            printFinalStats();
        }
    }

    private void processOrder(ConsumerRecord<String, String> record) {
        try {
            Thread.sleep(100); // simulate processing
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void registerShutdownHook(Thread mainThread) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received — calling consumer.wakeup()");
            consumer.wakeup();
            try {
                mainThread.join(); // wait for finally block: final commit + close + stats
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "batch-commit-consumer-shutdown-hook"));
    }

    private void printFinalStats() {
        long runtime = System.currentTimeMillis() - startTime;
        long processed = totalProcessed.get();
        logger.info("==========================================");
        logger.info("FINAL STATISTICS:");
        logger.info("   Messages processed: {}", processed);
        logger.info("   Total runtime:      {} ms", runtime);
        logger.info("==========================================");
    }
}
