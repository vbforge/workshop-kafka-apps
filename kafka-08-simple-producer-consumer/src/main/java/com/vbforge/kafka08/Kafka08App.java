package com.vbforge.kafka08;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class Kafka08App {

    public static void main(String[] args) {
        SpringApplication.run(Kafka08App.class, args);
    }

}
