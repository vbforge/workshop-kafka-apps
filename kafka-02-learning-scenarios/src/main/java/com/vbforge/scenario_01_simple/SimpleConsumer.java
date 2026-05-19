package com.vbforge.scenario_01_simple;


import com.vbforge.config.Constants;
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
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SimpleConsumer - demonstrates:
 *  - Basic message consumption from a Kafka topic
 *  - Graceful shutdown via ShutdownHook + consumer.wakeup()
 *  - WakeupException as the correct interrupt mechanism for poll()
 *  - Metrics tracking (message count, throughput)
 *
 * SHUTDOWN HOOK PATTERN EXPLAINED:
 *  - Ctrl+C sends SIGINT → JVM triggers all registered shutdown hooks
 *  - The hook calls consumer.wakeup() - thread-safe, interrupts poll() immediately
 *  - poll() throws WakeupException → caught in main loop → clean exit
 *  - This is the ONLY correct way; setting a boolean flag alone doesn't interrupt
 *    a blocking poll() call that may be waiting up to DEFAULT_POLL_TIMEOUT_MS
 *
 * IMPORTANT: Run via terminal (not IDE Stop button).
 *  - Ctrl+C  → SIGINT → shutdown hook fires → clean stats printed
 *  - IDE Stop → SIGKILL → JVM killed instantly → hook never runs
 */
public class SimpleConsumer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);

    // Instance field — accessible from both run() and the shutdown hook thread
    private KafkaConsumer<String, String> consumer;

    // Metrics
    private final AtomicLong totalMessagesReceived = new AtomicLong(0);
    private long startTime;

    // ============================================================
    // ENTRY POINT
    // ============================================================

    public static void main(String[] args) {
        new SimpleConsumer().run();
    }

    // ============================================================
    // MAIN CONSUMER LOOP
    // ============================================================

    private void run() {

        logger.info("Starting SimpleConsumer");
        Utility.verifyConfiguration();

        Properties props = KafkaConfig.createConsumerConfig(Constants.CONSUMER_GROUP_SIMPLE);
        consumer = new KafkaConsumer<>(props);

        // Register BEFORE starting the loop — hook needs consumer reference
        registerShutdownHook();

        startTime = System.currentTimeMillis();

        try {
            consumer.subscribe(Collections.singletonList(Constants.TOPIC_SIMPLE));
            logger.info("Subscribed to topic: {}", Constants.TOPIC_SIMPLE);
            logger.info("Waiting for messages... (Press Ctrl+C to stop)");

            while (true) {
                // poll() blocks up to DEFAULT_POLL_TIMEOUT_MS
                // wakeup() from shutdown hook causes it to throw WakeupException immediately
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(Constants.DEFAULT_POLL_TIMEOUT_MS));

                if (records.isEmpty()) {
                    logger.debug("No messages in this poll cycle");
                    continue;
                }

                processRecords(records);
            }

        } catch (WakeupException e) {
            // Expected on shutdown — not an error
            // wakeup() was called by the shutdown hook; this is the intended exit path
            logger.info("WakeupException received - consumer is shutting down");

        } catch (Exception e) {
            logger.error("Unexpected error: {}", e.getMessage(), e);

        } finally {
            // Commits pending offsets and releases partition assignments cleanly
            consumer.close();
            logger.info("Consumer closed");
            printFinalStats();
        }
    }

    // ============================================================
    // SHUTDOWN HOOK
    // ============================================================

    /**
     * Registers a JVM shutdown hook triggered by Ctrl+C (SIGINT).
     *
     * consumer.wakeup() is the ONLY thread-safe method on KafkaConsumer.
     * It causes the next (or current) poll() call to throw WakeupException,
     * which unblocks the main loop immediately regardless of poll timeout.
     *
     * No sleep needed — wakeup() is instant and non-blocking.
     */
    private void registerShutdownHook() {

        Thread mainThread = Thread.currentThread(); // capture BEFORE the hook runs

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received - calling consumer.wakeup()");
            consumer.wakeup();

            try {
                // Wait for the main thread to finish its finally block
                // (consumer.close() + printFinalStats()) before JVM exits
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Note: we do NOT call consumer.close() here.
            // close() must be called from the same thread that called poll().
            // The finally block in run() handles it on the main thread.
        }, "consumer-shutdown-hook"));
    }

    // ============================================================
    // RECORD PROCESSING
    // ============================================================

    private void processRecords(ConsumerRecords<String, String> records) {
        long batchStart = System.currentTimeMillis();
        int batchSize = records.count();

        logger.info("Received batch of {} message(s)", batchSize);

        for (ConsumerRecord<String, String> record : records) {
            processSingleRecord(record);
        }

        long elapsed = System.currentTimeMillis() - batchStart;
        logger.info("Batch processed in {}ms (avg {}ms/msg)",
                elapsed, String.format("%.2f", (double) elapsed / batchSize));
    }

    private void processSingleRecord(ConsumerRecord<String, String> record) {
        long count = totalMessagesReceived.incrementAndGet();
        logger.info("┌─────────────────────────────────────────────");
        logger.info("│ Message #{}", count);
        logger.info("│    Value:     {}", record.value());
        logger.info("│    Key:       {}", record.key() != null ? record.key() : "null");
        logger.info("│    Topic:     {}", record.topic());
        logger.info("│    Partition: {}", record.partition());
        logger.info("│    Offset:    {}", record.offset());
        logger.info("│    Timestamp: {}", record.timestamp());
        logger.info("└─────────────────────────────────────────────");
    }

    // ============================================================
    // STATS
    // ============================================================

    private void printFinalStats() {
        long runtime = System.currentTimeMillis() - startTime;
        long processed = totalMessagesReceived.get();
        double throughput = runtime > 0 ? processed / (runtime / 1000.0) : 0;

        logger.info("===========================================");
        logger.info("FINAL STATISTICS:");
        logger.info("   Messages processed: {}", processed);
        logger.info("   Total runtime:      {} ms", runtime);
        logger.info("   Avg throughput:     {} msgs/sec", String.format("%.2f", throughput));
        logger.info("===========================================");
        logger.info("SimpleConsumer finished!");
    }


}




























