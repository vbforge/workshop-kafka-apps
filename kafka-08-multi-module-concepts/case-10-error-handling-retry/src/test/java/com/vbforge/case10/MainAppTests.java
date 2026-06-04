package com.vbforge.case10;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"case-10-topic"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class MainAppTests {

    @Test
    void contextLoads() {
        // Verifies ExponentialBackOff + DefaultErrorHandler wire up correctly.
    }

}
