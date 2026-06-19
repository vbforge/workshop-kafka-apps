package com.vbforge.case17;

import com.vbforge.case17.model.EventMessage;
import com.vbforge.case17.service.EventConsumerService;
import com.vbforge.case17.service.EventProducerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

// JUNIOR NOTE: End-to-end test — exercises the full pipeline:
//   KafkaTemplate → [case-17-events-topic] → @KafkaListener → receivedEvents list
//
// This is closer to a real-world integration test: we send via the Spring
// KafkaTemplate (not through a service method), verify what the consumer received,
// and assert on message content integrity (no corruption in transit).
//
// Also shows how to use KafkaTemplate directly in tests when you need lower-level
// control (e.g., send to a specific partition, set custom headers).

@SpringBootTest
class EndToEndIntegrationTest extends AbstractKafkaIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private EventConsumerService consumerService;

    @Autowired
    private EventProducerService producerService;

    @Value("${kafka.topics.events}")
    private String eventsTopic;

    @BeforeEach
    void setUp() {
        consumerService.resetLatch(0);
    }

    @Test
    @DisplayName("E2E: message produced via KafkaTemplate is consumed with identical content")
    void e2e_messageContentIntegrity() throws Exception {
        // GIVEN
        consumerService.resetLatch(1);
        String id = UUID.randomUUID().toString();
        String payload = "e2e-payload-" + id;

        EventMessage event = EventMessage.builder()
                .id(id)
                .type("INVENTORY_UPDATED")
                .payload(payload)
                .sequenceNumber(42)
                .createdAt(LocalDateTime.of(2025, 6, 1, 12, 0, 0))
                .build();

        // WHEN — send directly via KafkaTemplate (bypasses service layer)
        kafkaTemplate.send(eventsTopic, id, event).get();
        boolean received = consumerService.getLatch().await(15, TimeUnit.SECONDS);

        // THEN
        assertThat(received).isTrue();
        EventMessage consumed = consumerService.getReceivedEvents().get(0);

        assertThat(consumed.getId()).isEqualTo(id);
        assertThat(consumed.getType()).isEqualTo("INVENTORY_UPDATED");
        assertThat(consumed.getPayload()).isEqualTo(payload);
        assertThat(consumed.getSequenceNumber()).isEqualTo(42);
        // LocalDateTime survives JSON serialization/deserialization round-trip
        assertThat(consumed.getCreatedAt()).isEqualTo(LocalDateTime.of(2025, 6, 1, 12, 0, 0));
    }

    @Test
    @DisplayName("E2E: 10 messages sent in rapid succession are all received")
    void e2e_rapidFireMessages_allReceived() throws Exception {
        // GIVEN
        int count = 10;
        consumerService.resetLatch(count);

        // WHEN — fire all 10 without waiting for acks (async)
        // JUNIOR NOTE: We call send() without .get() here — fire-and-forget style.
        // All 10 sends go to the broker's buffer in rapid succession.
        // The latch.await() below is what ensures we verify receipt.
        // This tests that the consumer keeps up under a small burst.
        for (int i = 0; i < count; i++) {
            EventMessage event = EventMessage.builder()
                    .id(UUID.randomUUID().toString())
                    .type("RAPID_EVENT")
                    .payload("rapid-" + i)
                    .sequenceNumber(i)
                    .createdAt(LocalDateTime.now())
                    .build();
            kafkaTemplate.send(eventsTopic, event.getId(), event);
        }

        boolean allReceived = consumerService.getLatch().await(20, TimeUnit.SECONDS);

        // THEN
        assertThat(allReceived).as("All 10 messages should be received within 20s").isTrue();
        assertThat(consumerService.getReceivedEvents()).hasSize(count);

        List<String> types = consumerService.getReceivedEvents().stream()
                .map(EventMessage::getType).distinct().toList();
        assertThat(types).containsOnly("RAPID_EVENT");
    }

    @Test
    @DisplayName("E2E: null payload field survives serialization round-trip")
    void e2e_nullFieldSurvivesRoundTrip() throws Exception {
        // JUNIOR NOTE: This is a useful edge case test — JSON serialization of
        // nulls in Jackson. By default Jackson includes null fields as "field":null.
        // If the consumer's deserializer is strict about required fields, this breaks.
        // Verifying explicitly that null fields round-trip correctly prevents
        // hard-to-debug production bugs.
        consumerService.resetLatch(1);

        EventMessage event = EventMessage.builder()
                .id(UUID.randomUUID().toString())
                .type(null)           // intentionally null
                .payload("has-payload")
                .sequenceNumber(0)
                .createdAt(null)      // intentionally null
                .build();

        kafkaTemplate.send(eventsTopic, event.getId(), event).get();
        consumerService.getLatch().await(15, TimeUnit.SECONDS);

        EventMessage consumed = consumerService.getReceivedEvents().get(0);
        assertThat(consumed.getType()).isNull();
        assertThat(consumed.getCreatedAt()).isNull();
        assertThat(consumed.getPayload()).isEqualTo("has-payload");
    }

}
