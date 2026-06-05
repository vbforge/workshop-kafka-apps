package com.vbforge.case11;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

// JUNIOR NOTE: Both topics must be declared here — the main topic AND the DLT topic.
// EmbeddedKafka creates them before the Spring context loads.
// If the DLT topic doesn't exist when DeadLetterPublishingRecoverer tries to publish,
// the recoverer will fail (or auto-create depending on broker config).
// Explicit declaration avoids that race condition in tests.

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"case-11-topic", "case-11-topic.DLT"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class MainAppTests {

    @Test
    void contextLoads() {
        // Verifies DeadLetterPublishingRecoverer, both consumer factories,
        // and both @KafkaListener beans wire up correctly.
    }

}
