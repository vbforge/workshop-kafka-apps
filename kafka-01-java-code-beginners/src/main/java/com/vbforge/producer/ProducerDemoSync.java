package com.vbforge.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Alternative to ProducerDemo: Synchronous Send (if prefer blocking)
 * If we want to wait for send to complete before continuing:
 * */

public class ProducerDemoSync {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoSync.class.getSimpleName());

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        log.info("Starting Kafka Producer (Synchronous)");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "3");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            
            ProducerRecord<String, String> producerRecord = 
                new ProducerRecord<>("demo_topic_example", "Hello World from Docker Kafka Producer (Synchronous)!");
            
            // Synchronous send: wait for result
            RecordMetadata metadata = producer.send(producerRecord).get();  // .get() blocks
            
            log.info("✅ Message sent successfully!");
            log.info("   Topic: {}", metadata.topic());
            log.info("   Partition: {}", metadata.partition());
            log.info("   Offset: {}", metadata.offset());
            
            producer.flush();
        }
        
        log.info("Producer (Synchronous) finished");
    }
}