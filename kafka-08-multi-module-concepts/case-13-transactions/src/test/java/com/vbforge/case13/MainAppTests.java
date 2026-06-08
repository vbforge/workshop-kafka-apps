package com.vbforge.case13;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

// JUNIOR NOTE: All three topics must be declared for EmbeddedKafka.
// The transactional producer needs the broker to have transaction support enabled,
// which EmbeddedKafka provides by default.
// Without declaring the topics up front, the transactional producer may
// fail to initialise if auto-creation races with the transaction coordinator setup.

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {
                "case-13-orders-topic",
                "case-13-processed-topic",
                "case-13-probe-topic"
        },
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class MainAppTests {

    @Test
    void contextLoads() {
        // Verifies transactional ProducerFactory, KafkaTransactionManager,
        // KafkaTemplate, and all @KafkaListener beans wire up correctly.
    }

}
