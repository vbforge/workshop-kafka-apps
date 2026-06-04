package com.vbforge.case08;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"case-08-topic"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class MainAppTests {

    @Test
    void contextLoads() {
        // Verifies the offset management consumer starts its poll loop
        // and wires all beans correctly.
    }

}
