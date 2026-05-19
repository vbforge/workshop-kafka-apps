package com.vbforge.scenario_03_load_balancing;

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
import java.util.concurrent.atomic.AtomicLong;

import static com.vbforge.config.Constants.*;

/**
 * LoadBalancingConsumer — one class, run it N times to form a consumer group.
 * <p>
 * Demonstrates:
 * - Multiple consumers sharing partitions within the same group
 * - Automatic partition assignment by Kafka (you don't choose who gets what)
 * - Rebalancing: when a consumer joins or leaves, Kafka redistributes partitions
 * - The ceiling rule: consumers > partitions → excess consumers sit idle
 * <p>
 * HOW TO RUN:
 * Start 3 separate terminals, each with a different consumer number:
 * <p>
 * mvn exec:java -Dexec.mainClass="com.vbforge.scenario_03_load_balancing.LoadBalancingConsumer" -Dexec.args="1"
 * mvn exec:java -Dexec.mainClass="com.vbforge.scenario_03_load_balancing.LoadBalancingConsumer" -Dexec.args="2"
 * mvn exec:java -Dexec.mainClass="com.vbforge.scenario_03_load_balancing.LoadBalancingConsumer" -Dexec.args="3"
 * <p>
 * The number is just a label for your terminal output — it has no effect on
 * which partitions this consumer is assigned. Kafka decides that.
 * <p>
 * STOP: Ctrl+C in each terminal (not the IDE Stop button).
 */
public class LoadBalancingConsumer {

    private static final Logger logger = LoggerFactory.getLogger(LoadBalancingConsumer.class);
    private KafkaConsumer<String, String> consumer;
    private final String consumerId;

    //metrics
    private final AtomicLong totalMessagesProcessed = new AtomicLong(0);
    private long startTime;

    public LoadBalancingConsumer(String consumerId) {
        this.consumerId = "Consumer - " + consumerId;
    }

    //entry point
    public static void main(String[] args) {
        //accept consumer number from command line: -Dexec.args="1"
        //falls back to timestamp if no args provided
        String id = (args.length > 0) ? args[0] : String.valueOf(System.currentTimeMillis());
        new LoadBalancingConsumer(id).run();
    }


    //main consumer loop
    private void run() {
        logger.info("=============== Load balancing consumer started ===============");
        logger.info("Group: {} | Topic: {}", CONSUMER_GROUP_LOAD_BALANCE, TOPIC_LOAD_BALANCE);
        Utility.verifyConfiguration();

        consumer = new KafkaConsumer<>(KafkaConfig.createConsumerConfig(CONSUMER_GROUP_LOAD_BALANCE));

        Thread mainThread = Thread.currentThread();
        registerShutdownHook(mainThread);

        startTime = System.currentTimeMillis();

        try{

            //consumer subscribe to topic
            consumer.subscribe(Collections.singletonList(TOPIC_LOAD_BALANCE));
            logger.info("[{}] Subscribed — waiting for partition assignment...", consumerId);
            logger.info("[{}] Ctrl + C to stop", consumerId);

            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(DEFAULT_POLL_TIMEOUT_MS));

                if(records.isEmpty()){
                    continue;
                }

                for(ConsumerRecord<String, String> record : records){
                    processRecord(record);
                }
            }

        }catch(WakeupException e){
            logger.info("[{}] WakeupException — shutting down", consumerId);

        }catch (Exception e){
            logger.error("[{}] Unexpected error: {}", consumerId, e.getMessage(), e);

        }finally {
            consumer.close();
            logger.info("[{}] Load balancing consumer stopped", consumerId);
            printFinalStats();
        }

    }

    //record processing
    /**
     * Process one record.
     *
     * Thread.sleep(200) simulates real work (a DB write, an API call, etc.).
     * This matters for the load balancing experiment: if processing is instant,
     * you won't observe the distribution clearly. The delay makes each consumer's
     * workload visible in the terminal output.
     */
    private void processRecord(ConsumerRecord<String, String> record) {
        long count = totalMessagesProcessed.incrementAndGet();
        logger.info("[{}] Processing #{} | value: {} | partition: {} | offset: {}",
                consumerId, count, record.value(), record.partition(), record.offset());
        try{
            Thread.sleep(200); //simulation of processing work time
        }catch (InterruptedException e){
            Thread.currentThread().interrupt();
            logger.warn("[{}] Processing interrupted", consumerId);
        }

    }

    //shutdown hook
    /**
     * Shutdown hook: Ctrl+C → wakeup() → WakeupException in poll() → finally block runs.
     * mainThread.join() keeps the hook thread alive until the main thread finishes
     * consumer.close() and printFinalStats() — without it the JVM exits too early
     * and you never see the final stats.
     */
    private void registerShutdownHook(Thread mainThread) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("[{}] Shutdown signal received — calling consumer.wakeup()", consumerId);
            consumer.wakeup();
            try {
                mainThread.join(); //wait for finally block to complete before JVM exits
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, consumerId + "shutdown-hook"));
    }

    //stats
    private void printFinalStats() {
        long runtime = System.currentTimeMillis() - startTime;
        long processed = totalMessagesProcessed.get();
        double throughput = runtime > 0 ? processed / (runtime / 1000.0) : 0;

        logger.info("=========================================");
        logger.info("[{}] FINAL STATISTICS:", consumerId);
        logger.info("   Messages processed: {}", processed);
        logger.info("   Total runtime:      {} ms", runtime);
        logger.info("   Avg throughput:     {} msgs/sec", String.format("%.2f", throughput));
        logger.info("=========================================");
        logger.info("[{}] finished.", consumerId);
    }
}

















