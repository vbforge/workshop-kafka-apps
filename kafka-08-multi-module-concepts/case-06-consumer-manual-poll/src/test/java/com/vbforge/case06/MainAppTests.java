package com.vbforge.case06;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

// JUNIOR NOTE: The manual poll consumer starts its poll loop in @PostConstruct.
// EmbeddedKafka must be up before the context loads so the consumer can subscribe.
// bootstrapServersProperty overrides spring.kafka.bootstrap-servers with the
// embedded broker's address — same pattern as all previous cases.

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"case-06-topic"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class MainAppTests {

    @Test
    void contextLoads() {
        // Verifies the manual poll consumer starts cleanly and the poll loop
        // thread launches without exceptions during context initialization.
    }

}
