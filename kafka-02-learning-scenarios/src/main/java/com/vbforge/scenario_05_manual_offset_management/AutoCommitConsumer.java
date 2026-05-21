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
 * AutoCommitConsumer — demonstrates the DEFAULT Kafka consumer behavior.
 *
 * With enable.auto.commit=true (set in KafkaConfig.createConsumerConfig),
 * Kafka commits offsets automatically every AUTO_COMMIT_INTERVAL_MS (1 second).
 * This happens in the background, independent of your processing logic.
 *
 * THE RISK — at-most-once delivery:
 *  1. poll() returns records — Kafka internally schedules a commit
 *  2. Auto-commit fires — offsets are committed to broker
 *  3. Consumer crashes before finishing processOrder()
 *  4. On restart, consumer reads from the committed offset → those records are SKIPPED
 *
 * This is the "at-most-once" delivery guarantee:
 *  messages are processed at most once — they can be lost, never duplicated.
 *
 * When is auto-commit acceptable?
 *  - Processing is idempotent and losing a message is tolerable
 *  - Logging, metrics, analytics — where an occasional gap is fine
 *
 * When is it NOT acceptable?
 *  - Financial transactions, inventory updates, anything where loss = business impact
 *  → Use ManualCommitConsumer or BatchCommitConsumer instead
 *
 * STOP: Ctrl+C in terminal only.
 */
public class AutoCommitConsumer {

    private static final Logger logger = LoggerFactory.getLogger(AutoCommitConsumer.class);

    private KafkaConsumer<String, String> consumer;
    private final AtomicLong processedCount = new AtomicLong(0);
    private long startTime;

    public static void main(String[] args) {
        new AutoCommitConsumer().run();
    }

    private void run() {

        logger.info("======== Auto-Commit Consumer ========");
        logger.warn("Offsets committed automatically — message loss possible on crash");
        Utility.verifyConfiguration();

        // createConsumerConfig has auto-commit enabled by default
        consumer = new KafkaConsumer<>(KafkaConfig.createConsumerConfig(CONSUMER_GROUP_MANUAL_OFFSET + "-auto"));

        Thread mainThread = Thread.currentThread();
        registerShutdownHook(mainThread);

        startTime = System.currentTimeMillis();

        try {
            consumer.subscribe(Collections.singletonList(TOPIC_MANUAL_OFFSET));
            logger.info("Subscribed to: {} | group: {}", TOPIC_MANUAL_OFFSET, CONSUMER_GROUP_MANUAL_OFFSET + "-auto");
            logger.info("Ctrl+C to stop");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(DEFAULT_POLL_TIMEOUT_MS));

                for (ConsumerRecord<String, String> record : records) {
                    processOrder(record);
                    // NOTE: offset for this record may already be committed by auto-commit
                    // before processOrder() finishes — that's the risk being demonstrated
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

    private void processOrder(ConsumerRecord<String, String> record) {
        long count = processedCount.incrementAndGet();
        logger.info("Processing #{} | key: {} | value: {} | partition: {} | offset: {}",
                count, record.key(), record.value(), record.partition(), record.offset());
        try {
            Thread.sleep(100); // simulate work
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // If a crash happened here, and auto-commit already fired,
        // this record would be skipped on restart
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
        }, "auto-commit-consumer-shutdown-hook"));
    }

    private void printFinalStats() {
        long runtime = System.currentTimeMillis() - startTime;
        long processed = processedCount.get();
        logger.info("===========================================");
        logger.info("FINAL STATISTICS:");
        logger.info("   Messages processed: {}", processed);
        logger.info("   Total runtime:      {} ms", runtime);
        logger.info("===========================================");
    }
}