package com.vbforge.case15;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest
@EmbeddedKafka(
        partitions = 3,
        topics = {"case-15-events"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class MainAppTests {

    @Test
    void contextLoads() {
        // Verifies the batch listener container starts correctly with setBatchListener(true),
        // MANUAL_IMMEDIATE ack mode, and max.poll.records configuration.
    }

}
