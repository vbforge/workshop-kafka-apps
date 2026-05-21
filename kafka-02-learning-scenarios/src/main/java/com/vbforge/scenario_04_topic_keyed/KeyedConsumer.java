package com.vbforge.scenario_04_topic_keyed;

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
 * KeyedConsumer — one class, run it 2–3 times to form a consumer group.
 *
 * Demonstrates:
 *  - Messages with the same key are always routed to the same partition
 *  - Because partitions are assigned to specific consumers, all messages
 *    for a given key are always processed by the same consumer instance
 *  - This is Kafka's per-key ordering guarantee in practice
 *
 * Per-key statistics are tracked locally to make the routing visible:
 * after stopping, each consumer prints which keys it processed and how many
 * events it saw per key — you'll see that keys never "jump" between consumers.
 *
 * HOW TO RUN:
 *   Terminal 1:
 *     mvn exec:java -Dexec.mainClass="com.vbforge.scenario_04_topic_keyed.KeyedConsumer" -Dexec.args="1"
 *   Terminal 2:
 *     mvn exec:java -Dexec.mainClass="com.vbforge.scenario_04_topic_keyed.KeyedConsumer" -Dexec.args="2"
 *
 * STOP: Ctrl+C in each terminal.
 */
public class KeyedConsumer {
 
    private static final Logger logger = LoggerFactory.getLogger(KeyedConsumer.class);
 
    private KafkaConsumer<String, String> consumer;
    private final String consumerId;
 
    // Per-key event counter — shows which keys this consumer instance owned
    private final Map<String, Integer> keyEventCounts = new HashMap<>();
    private final AtomicLong totalMessages = new AtomicLong(0);
    private long startTime;
 
    public KeyedConsumer(String consumerId) {
        this.consumerId = "Consumer-" + consumerId;
    }
 
    // =========================================================================
    // ENTRY POINT
    // =========================================================================
 
    public static void main(String[] args) {
        String id = (args.length > 0) ? args[0] : String.valueOf(System.currentTimeMillis());
        new KeyedConsumer(id).run();
    }
 
    // =========================================================================
    // MAIN CONSUMER LOOP
    // =========================================================================
 
    private void run() {
 
        logger.info("=== Starting {} ===", consumerId);
        logger.info("Group: {} | Topic: {}", CONSUMER_GROUP_KEYED, TOPIC_KEYED);
        Utility.verifyConfiguration();
 
        consumer = new KafkaConsumer<>(KafkaConfig.createConsumerConfig(CONSUMER_GROUP_KEYED));
 
        Thread mainThread = Thread.currentThread();
        registerShutdownHook(mainThread);
 
        startTime = System.currentTimeMillis();
 
        try {
            consumer.subscribe(Collections.singletonList(TOPIC_KEYED));
            logger.info("[{}] Subscribed — waiting for partition assignment...", consumerId);
            logger.info("[{}] Ctrl+C to stop", consumerId);
 
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(DEFAULT_POLL_TIMEOUT_MS));
 
                if (records.isEmpty()) {
                    continue;
                }
 
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                }
 
                // Print per-key summary after each non-empty batch
                // Makes it easy to observe key-to-consumer affinity in real time
                printKeySummary();
            }
 
        } catch (WakeupException e) {
            logger.info("[{}] WakeupException — shutting down", consumerId);
 
        } catch (Exception e) {
            logger.error("[{}] Unexpected error: {}", consumerId, e.getMessage(), e);
 
        } finally {
            consumer.close();
            logger.info("[{}] Consumer closed", consumerId);
            printFinalStats();
        }
    }
 
    // =========================================================================
    // RECORD PROCESSING
    // =========================================================================
 
    private void processRecord(ConsumerRecord<String, String> record) {
        String key = record.key();
        long count = totalMessages.incrementAndGet();
 
        // Accumulate per-key count
        keyEventCounts.merge(key, 1, Integer::sum);
 
        logger.info("[{}] #{} | key: {} | value: {} | partition: {} | offset: {}",
                consumerId, count, key, record.value(), record.partition(), record.offset());
    }
 
    // =========================================================================
    // SUMMARIES
    // =========================================================================
 
    /**
     * Prints per-key event counts after each batch.
     * The key observation: each consumer only ever sees a fixed subset of keys.
     * A key that appears in Consumer-1's summary will never appear in Consumer-2's.
     */
    private void printKeySummary() {
        logger.info("[{}] --- Key distribution so far ---", consumerId);
        keyEventCounts.forEach((key, count) ->
                logger.info("[{}]   {}: {} event(s)", consumerId, key, count));
    }
 
    private void printFinalStats() {
        long runtime = System.currentTimeMillis() - startTime;
        long processed = totalMessages.get();
        double throughput = runtime > 0 ? processed / (runtime / 1000.0) : 0;
 
        logger.info("═══════════════════════════════════════════");
        logger.info("[{}] FINAL STATISTICS:", consumerId);
        logger.info("   Total messages processed: {}", processed);
        logger.info("   Keys handled by this consumer:");
        keyEventCounts.forEach((key, count) ->
                logger.info("     {}: {} event(s)", key, count));
        logger.info("   Total runtime:  {} ms", runtime);
        logger.info("   Avg throughput: {} msgs/sec", String.format("%.2f", throughput));
        logger.info("═══════════════════════════════════════════");
        logger.info("[{}] finished.", consumerId);
    }
 
    // =========================================================================
    // SHUTDOWN HOOK
    // =========================================================================
 
    private void registerShutdownHook(Thread mainThread) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("[{}] Shutdown signal received — calling consumer.wakeup()", consumerId);
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, consumerId + "-shutdown-hook"));
    }
}