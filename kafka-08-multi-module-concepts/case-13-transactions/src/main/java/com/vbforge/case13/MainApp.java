package com.vbforge.case13;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

// JUNIOR NOTE: @EnableKafka is always required — activates @KafkaListener scanning.
// @SpringBootApplication provides @EnableTransactionManagement implicitly,
// which is needed for @Transactional in TransactionalProducerService to work.
// No need to add @EnableTransactionManagement explicitly.

@EnableKafka
@SpringBootApplication
public class MainApp {

    public static void main(String[] args) {
        SpringApplication.run(MainApp.class, args);
    }

}