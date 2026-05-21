package com.vbforge.scenario_05_manual_offset_management;

import com.vbforge.config.KafkaConfig;
import com.vbforge.config.Utility;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.vbforge.config.Constants.*;

/**
 * PerPartitionCommitConsumer — fine-grained offset control per partition.
 *
 * All previous consumers commit offsets across the entire consumer in one call.
 * This consumer commits each partition's offset independently after processing
 * all records from that partition in the current poll batch.
 *
 * WHY THIS MATTERS:
 *  Imagine a poll() returns records from partitions 0 and 1.
 *  Processing partition 0 succeeds, partition 1 fails mid-way.
 *  With commitSync() (no map): you can't commit partition 0's progress without
 *  also committing partition 1's partially-processed offset.
 *  With per-partition commit: commit partition 0's offset immediately after
 *  it's done — partition 1's failure only causes partition 1 to be re-delivered.
 *
 * THE PATTERN:
 *  - After processing all records from a partition, build a Map<TopicPartition, OffsetAndMetadata>
 *  - Offset to commit = last processed record's offset + 1
 *    (Kafka stores "next record to read", not "last record read")
 *  - Call commitSync(map) with that single-partition map
 *
 * This is the most precise commit strategy — use it when partitions process
 * independently and partial failures in one partition shouldn't block others.
 *
 * STOP: Ctrl+C in terminal only.
 */
public class PerPartitionCommitConsumer {

    private static final Logger logger = LoggerFactory.getLogger(PerPartitionCommitConsumer.class);

    private KafkaConsumer<String, String> consumer;
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private long startTime;

    public static void main(String[] args) {
        new PerPartitionCommitConsumer().run();
    }

    private void run() {

        logger.info("======== Per-Partition Commit Consumer ========");
        logger.info("Commits offsets independently per partition after each poll batch");
        Utility.verifyConfiguration();

        consumer = new KafkaConsumer<>(
                KafkaConfig.createManualCommitConsumerConfig(CONSUMER_GROUP_MANUAL_OFFSET + "-per-partition"));

        Thread mainThread = Thread.currentThread();
        registerShutdownHook(mainThread);

        startTime = System.currentTimeMillis();

        try {
            consumer.subscribe(Collections.singletonList(TOPIC_MANUAL_OFFSET));
            logger.info("Subscribed to: {} | group: {}", TOPIC_MANUAL_OFFSET, CONSUMER_GROUP_MANUAL_OFFSET + "-per-partition");
            logger.info("Ctrl+C to stop");

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(DEFAULT_POLL_TIMEOUT_MS));

                if (records.isEmpty()) {
                    continue;
                }

                // Process one partition at a time — commit each independently
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);

                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        processOrder(record);
                        long count = totalProcessed.incrementAndGet();
                        logger.info("[Partition {}] Processed #{} | key: {} | offset: {}",
                                partition.partition(), count, record.key(), record.offset());
                    }

                    // Commit this partition only.
                    // offset + 1 because Kafka stores the NEXT offset to read, not the last read.
                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();

                    Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
                    commitMap.put(partition, new OffsetAndMetadata(lastOffset + 1));

                    consumer.commitSync(commitMap);
                    logger.info("[Partition {}] Committed up to offset {}",
                            partition.partition(), lastOffset);
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
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "per-partition-consumer-shutdown-hook"));
    }

    private void printFinalStats() {
        long runtime = System.currentTimeMillis() - startTime;
        logger.info("===========================================");
        logger.info("FINAL STATISTICS:");
        logger.info("   Messages processed: {}", totalProcessed.get());
        logger.info("   Total runtime:      {} ms", runtime);
        logger.info("===========================================");
    }
}

