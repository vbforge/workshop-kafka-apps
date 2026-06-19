package com.vbforge.case16;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {
                "case-16-request-topic",
                "case-16-reply-topic",
                "case-16-routed-priority-topic",
                "case-16-routed-standard-topic"
        },
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class MainAppTests {

    @Test
    void contextLoads() {
        // Verifies all listeners start, ReplyingKafkaTemplate wires correctly,
        // and the routing service beans are created.
    }

}
