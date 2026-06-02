package com.vbforge.case04;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

// JUNIOR NOTE: @EmbeddedKafka starts a real in-process Kafka broker for the test.
// partitions = 3 mirrors the production intent — the custom partitioner routes to
// partitions 0, 1, 2, so we need at least 3 for all paths to be exercisable.

@SpringBootTest
@EmbeddedKafka(
        partitions = 3,
        topics = {"case-04-topic-keyed"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class MainAppTests {

    @Test
    void contextLoads() {
        // If the context starts without exceptions, both KafkaTemplate beans wired correctly.
        // That's the contract of this test.

    }

}
