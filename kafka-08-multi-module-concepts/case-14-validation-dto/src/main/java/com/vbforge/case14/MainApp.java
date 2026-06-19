package com.vbforge.case14;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

// JUNIOR NOTE: @EnableKafka is required to activate @KafkaListener scanning.
// Without it Spring Boot will not register any listener containers even though
// the annotations are there. This is one of those "convention over silence" traps —
// nothing breaks at startup, messages just never arrive at your listener.
@EnableKafka
@SpringBootApplication
public class MainApp {
    public static void main(String[] args) {
        SpringApplication.run(MainApp.class, args);
    }
}
