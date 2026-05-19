package com.vbforge.scenario_01_simple;

import com.vbforge.config.KafkaConfig;
import com.vbforge.config.Utility;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static com.vbforge.config.Constants.*;

/**
 * SimpleProducer - Production-ready message producer
 *  Implemented:
 *   - Configurable message count and delay
 *   - Async sends with proper callbacks
 *   - Metrics tracking (success/failure counts, send times)
 *   - Idempotent producer configuration (prevents duplicates)
 *   - Batch processing optimization
 *   - Proper resource cleanup
 * Use Case: Simple message queue with guaranteed order and delivery
 */
public class SimpleProducer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);

    //metrics tracking
    private static final AtomicInteger successCount = new AtomicInteger(0);
    private static final AtomicInteger failCount = new AtomicInteger(0);
    private static long startTime;

    public static void main(String[] args) {

        //method to start producing messages
        simpleProducerStarter();

    }

    private static void simpleProducerStarter() {

        logger.info("Starting SimpleProducer (Docker Kafka)");
        logger.info("Configuration: {} messages, {}ms delay", DEFAULT_MESSAGE_COUNT, DEFAULT_DELAY_MS);

        Utility.verifyConfiguration();

        startTime = System.currentTimeMillis();

        //create properties for producer
        Properties properties = KafkaConfig.createProducerConfig();

        //try-with-resources and helpers used here (send messages, printing result and specified custom callback record)
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            logger.info("SimpleProducer created");
            logger.info("Sending {} messages to topic: {}", DEFAULT_MESSAGE_COUNT, TOPIC_SIMPLE);

            //send messages in batch
            for(int i = 1; i <= DEFAULT_MESSAGE_COUNT; i++){
                sendMessage(producer, i);
            }

            //ensure all messages sent before closing
            logger.info("Flushing remaining messages...");
            producer.flush();

            //printing statistics into console
            printFinalStats();

        } catch (Exception e) {
            logger.error("SimpleProducer failed: {}", e.getMessage(), e);
        }

    }


    //helpers

    //send message async callback
    private static void sendMessage(KafkaProducer<String, String> producer, int sequence) {

        String message = String.format("Message #%d from Docker Kafka - Timestamp: %d", sequence,  System.currentTimeMillis());

        // Create record with key for partition affinity (optional)
        // Using null key for round-robin distribution across partitions
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_SIMPLE, null, message);

        long startTime = System.currentTimeMillis();

        //send asynchronously with callback
        producer.send(record, new CustomCallback(message, sequence, startTime));

        //respect configured delay between sends (but don't block unnecessarily)
        if(DEFAULT_DELAY_MS > 0 && sequence < DEFAULT_MESSAGE_COUNT) {
            try {
                Thread.sleep(DEFAULT_DELAY_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("SimpleProducer Interrupted during delay");
            }
        }
    }

    //send message synchronously (when need immediate confirm)
    private static void sendMessageSync(KafkaProducer<String, String> producer, int sequence) throws InterruptedException, TimeoutException, ExecutionException {

        String message = String.format("Sync Message #%d ", sequence);

        //ProducerRecord (to our topic this message)
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_SIMPLE, message);

        //ProducerMetadata: producer.send(record)
        RecordMetadata metadata = producer.send(record).get(SEND_TIMEOUT_SEC, TimeUnit.SECONDS);

        logger.info("Sync message sent successfully");
        logger.info("   Message: {}", message);
        logger.info("   Partition: {}", metadata.partition());
        logger.info("   Offset: {}", metadata.offset());

    }

    //print final statistics
    private static void printFinalStats(){
        long totalTime = System.currentTimeMillis() - startTime;
        long successTotal =  successCount.get();
        long failTotal =  failCount.get();

        logger.info("===========================================");
        logger.info("FINAL STATISTICS:");
        logger.info("   Expected messages: {}", DEFAULT_MESSAGE_COUNT);
        logger.info("   Successfully sent: {}", successTotal);
        logger.info("   Failed: {}", failTotal);
        logger.info("   Total time: {} ms", totalTime);
        logger.info("   Throughput: {} msgs/sec", (successTotal * 1000.0 / totalTime));
        //check if all messages were sent
        if(successTotal == DEFAULT_MESSAGE_COUNT){
            logger.info("ALL MESSAGES SENT SUCCESSFULLY!");
        } else {
            logger.warn("Some messages failed to send. Check Kafka connectivity.");
        }
        logger.info("===========================================");
    }

    //static class of custom callback implementation for handling send results
    private record CustomCallback(String message, int sequence, long sendStartTime) implements Callback {

        @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {

                long duration = System.currentTimeMillis() - sendStartTime;

                if (e == null) {
                    //success case
                    successCount.incrementAndGet();
                    logger.info("Message #{} sent successfully ({}ms)", sequence, duration);
                    logger.info("   Value: {}", message);
                    logger.info("   Topic: {}", metadata.topic());
                    logger.info("   Partition: {}", metadata.partition());
                    logger.info("   Offset: {}", metadata.offset());
                    logger.info("   Timestamp: {}", metadata.timestamp());

                } else {
                    //failure case
                    failCount.incrementAndGet();
                    logger.error("Message #{} failed to send ({}ms)", sequence, duration);
                    logger.error("   Message: {}", message);
                    logger.error("   Error: {}", e.getMessage(), e);

                    //retry logic if needed, additional handling could be here (such as send to DLQ) - Dead Letter Queue
                }

            }
        }



}














