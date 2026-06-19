package com.vbforge.case14;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"case-14-events", "case-14-rejected"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class MainAppTests {

    @Test
    void contextLoads() {
        // Verifies the full Spring context starts: KafkaConfig, Validator bean,
        // ValidationConsumerService, ValidationController, ProducerService.
    }

}
