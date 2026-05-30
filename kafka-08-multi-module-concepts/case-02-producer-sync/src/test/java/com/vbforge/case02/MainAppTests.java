package com.vbforge.case02;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

// JUNIOR NOTE: @EmbeddedKafka spins up a real in-process Kafka broker for the test.
// No Docker needed — Spring starts a lightweight Kafka inside the JVM.
// We override bootstrap-servers via the annotation so the app connects to the embedded broker.
// This verifies that the Spring context loads correctly — all beans wire up, no missing config.

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"case-02-topic-sync"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class MainAppTests {

    @Test
    void contextLoads() {
        // If the context starts without exceptions, all Kafka beans wired correctly.
        // That's the contract of this test.
    }

}
