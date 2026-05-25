package com.vbforge.kafka06;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for kafka-06-producer-consumer-masterclass.
 *
 * <p>Prerequisites before running:
 * <pre>
 *   docker compose up -d        ← starts Kafka (KRaft, no ZooKeeper)
 *   mvn spring-boot:run         ← starts the Spring Boot app on :8080
 * </pre>
 *
 * <p>Topics are created automatically on startup via {@link com.vbforge.kafka06.config.KafkaTopicConfig}.
 */
@SpringBootApplication
public class Kafka06Application {

	public static void main(String[] args) {
		SpringApplication.run(Kafka06Application.class, args);
	}

}
