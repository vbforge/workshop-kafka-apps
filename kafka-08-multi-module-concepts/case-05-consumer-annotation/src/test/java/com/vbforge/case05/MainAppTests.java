package com.vbforge.case05;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

// JUNIOR NOTE: partitions=3 mirrors the production intent — concurrency=3 requires
// 3 partitions for each thread to own exactly one. With fewer partitions some
// threads sit idle; with more partitions some threads own multiple.

@SpringBootTest
@EmbeddedKafka(
        partitions = 3,
        topics = {"case-05-topic"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"

)
class MainAppTests {

    @Test
    void contextLoads() {
        // Verifies all beans wire up correctly including the concurrent listener factory.
    }

}

