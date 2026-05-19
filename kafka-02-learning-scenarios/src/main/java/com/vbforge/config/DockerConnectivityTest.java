package com.vbforge.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.vbforge.config.Constants.*;

public class DockerConnectivityTest {

    private static final Logger log = LoggerFactory.getLogger(DockerConnectivityTest.class);

    public static void main(String[] args) {

        log.info("Test Kafka Configuration Connectivity Started...");

        Utility.verifyConfiguration();
        
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfig.createProducerConfig())) {
            
            producer.send(new ProducerRecord<>(TOPIC_TEST_CONNECTIVITY, TEST_CONNECTIVITY_KEY, TEST_CONNECTIVITY_VALUE));
            producer.flush();
            log.info("Successfully connected to Docker Kafka!");

        } catch (Exception e) {
            log.error("Failed to connect: {}", e.getMessage());
            e.printStackTrace();
        }

        log.info("Test Kafka Configuration Connectivity Completed!");

    }
}