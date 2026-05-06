package com.vbforge.kafka.dashboard;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @EnableScheduling activates the @Scheduled broadcaster in DashboardBroadcaster.
 * Without this annotation, @Scheduled methods are silently ignored.
 */
@SpringBootApplication
@EnableScheduling
public class KafkaDashboardApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDashboardApplication.class, args);
    }
}
