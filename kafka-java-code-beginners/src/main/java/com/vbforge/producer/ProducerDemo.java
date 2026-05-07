package com.vbforge.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Simple example of Kafka Producer connecting to Docker Kafka.
 *
 * Before running:
 * 1. Start Kafka in Docker: docker-compose up -d
 * 2. Wait for Kafka to be healthy (30 seconds)
 * 3. Run this producer
 *
 * To verify messages are sent:
 * - Use Conduktor UI at http://localhost:8085
 * - Or run console consumer in Docker:
 *   docker exec -it kafka-java-broker kafka-console-consumer \
 *     --bootstrap-server localhost:9092 \
 *     --topic demo_topic_example \
 *     --from-beginning
 */

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());


    public static void main(String[] args) {

        log.info("Starting Kafka Producer (Docker version)");

        // 1) create Producer properties
        Properties props = new Properties();

        //connect to Docker Kafka on localhost:9092
        props.setProperty("bootstrap.servers", "localhost:9092");

        //serializers (convert Java Object to bytes)
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        //optional: added for better reliability during Docker testing
        props.setProperty("acks", "all");                   // wait for all replicas
        props.setProperty("retries", "3");                  // retry on failure
        props.setProperty("enable.idempotence", "true");    // no duplicates

        // 2) create Producer (autocloseable)
        try(KafkaProducer<String, String> producer = new KafkaProducer<>(props)){

            // 3) create a Producer Record
            ProducerRecord<String, String> record = new ProducerRecord<>("demo_topic_example", "Hello World from Docker Kafka Producer!");

            //4) send data asynchronously with callback
            producer.send(record, (RecordMetadata metadata, Exception exception) -> {

                //if no exception, so all is good:
                if(exception == null){
                    log.info("✅ Message sent successfully!");
                    log.info("   Topic: {}", metadata.topic());
                    log.info("   Partition: {}", metadata.partition());
                    log.info("   Offset: {}", metadata.offset());
                    log.info("   Timestamp: {}", metadata.timestamp());
                }else {
                    //in case exception occur we are in this else block:
                    log.error("❌ Failed to send message", exception);
                }
            });

            // 5) Flush and close (try-with-resources handles close automatically)
            producer.flush();

            log.info("Message produced, waiting for acknowledgment...");

            // Give a moment for the callback to execute
            Thread.sleep(1000);

        }catch (InterruptedException e){
            log.error("Thread interrupted!", e);
            Thread.currentThread().interrupt();
        }

        log.info("Producer finished!");


    }

}




















