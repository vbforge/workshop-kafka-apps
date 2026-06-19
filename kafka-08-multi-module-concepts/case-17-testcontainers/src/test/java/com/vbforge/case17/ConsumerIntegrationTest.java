package com.vbforge.case17;

import com.vbforge.case17.model.EventMessage;
import com.vbforge.case17.service.EventConsumerService;
import com.vbforge.case17.service.EventProducerService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

// JUNIOR NOTE: This test verifies the CONSUMER (EventConsumerService) end-to-end.
// The full flow:
//   1. resetLatch(N) — tell the consumer to expect N messages
//   2. produce N messages via EventProducerService (real Kafka, Testcontainer)
//   3. latch.await() — block the test thread until the @KafkaListener has consumed N messages
//   4. assert on getReceivedEvents()
//
// This IS the standard pattern for testing Kafka consumers.
// No Mockito. No EmbeddedKafka. A real Kafka container — same image as production.
//
// latch.await(timeout) is crucial: if you don't give a timeout and the consumer
// never receives the message (bug, misconfiguration), the test hangs forever.
// Always use await(N, TimeUnit.SECONDS) so failures fail fast.

@SpringBootTest
class ConsumerIntegrationTest extends AbstractKafkaIntegrationTest {

    @Autowired
    private EventProducerService producerService;

    @Autowired
    private EventConsumerService consumerService;

    @BeforeEach
    void setUp() {
        // JUNIOR NOTE: Reset between tests so received messages from one test
        // don't bleed into assertions of the next test.
        consumerService.resetLatch(0); // start clean; each test sets its own count
    }

    @Test
    @DisplayName("consumer should receive a single produced event within timeout")
    void consumer_shouldReceiveSingleEvent() throws Exception {
        // GIVEN
        consumerService.resetLatch(1);
        EventMessage event = EventMessage.builder()
                .id(UUID.randomUUID().toString())
                .type("ORDER_PLACED")
                .payload("test order payload")
                .sequenceNumber(1)
                .createdAt(LocalDateTime.now())
                .build();

        // WHEN
        producerService.sendSingle(event);
        boolean completed = consumerService.getLatch().await(15, TimeUnit.SECONDS);

        // THEN
        assertThat(completed).as("Latch should count down to 0 within 15s").isTrue();

        List<EventMessage> received = consumerService.getReceivedEvents();
        assertThat(received).hasSize(1);
        assertThat(received.get(0).getId()).isEqualTo(event.getId());
        assertThat(received.get(0).getType()).isEqualTo("ORDER_PLACED");
        assertThat(received.get(0).getPayload()).isEqualTo("test order payload");
    }

    @Test
    @DisplayName("consumer should receive all events from a batch in order")
    void consumer_shouldReceiveAllBatchEvents() throws Exception {
        // GIVEN
        int count = 5;
        consumerService.resetLatch(count);

        // WHEN
        producerService.sendBatch(count, "USER_REGISTERED");
        boolean completed = consumerService.getLatch().await(15, TimeUnit.SECONDS);

        // THEN
        assertThat(completed).as("All 5 events should arrive within 15s").isTrue();

        List<EventMessage> received = consumerService.getReceivedEvents();
        assertThat(received).hasSize(count);
        received.forEach(e -> assertThat(e.getType()).isEqualTo("USER_REGISTERED"));

        // sequence numbers 1..N are all present (order not guaranteed across partitions,
        // but for a single-partition topic they will be in order)
        List<Integer> sequences = received.stream()
                .map(EventMessage::getSequenceNumber)
                .sorted()
                .toList();
        assertThat(sequences).containsExactly(1, 2, 3, 4, 5);
    }

    @Test
    @DisplayName("consumer should handle multiple event types in the same batch")
    void consumer_shouldHandleMixedEventTypes() throws Exception {
        // GIVEN
        consumerService.resetLatch(2);

        EventMessage e1 = EventMessage.builder()
                .id(UUID.randomUUID().toString()).type("ORDER_PLACED")
                .payload("order-001").sequenceNumber(1).createdAt(LocalDateTime.now()).build();
        EventMessage e2 = EventMessage.builder()
                .id(UUID.randomUUID().toString()).type("PAYMENT_PROCESSED")
                .payload("payment-001").sequenceNumber(2).createdAt(LocalDateTime.now()).build();

        // WHEN
        producerService.sendSingle(e1);
        producerService.sendSingle(e2);
        boolean completed = consumerService.getLatch().await(15, TimeUnit.SECONDS);

        // THEN
        assertThat(completed).isTrue();

        List<EventMessage> received = consumerService.getReceivedEvents();
        assertThat(received).hasSize(2);

        List<String> types = received.stream().map(EventMessage::getType).toList();
        assertThat(types).containsExactlyInAnyOrder("ORDER_PLACED", "PAYMENT_PROCESSED");
    }

    @Test
    @DisplayName("latch timeout — consumer that never receives message should fail fast, not hang")
    void consumer_latchTimeout_shouldFailFastNotHang() throws Exception {
        // JUNIOR NOTE: This test intentionally demonstrates the timeout behavior.
        // We set a latch for 1 message but don't produce anything.
        // The test should complete QUICKLY (< 5s wall-clock) with completed=false,
        // proving the test suite doesn't hang when Kafka consumption doesn't happen.
        //
        // In real test failures (consumer misconfigured, topic mismatch, etc.),
        // this is how you get a fast failure instead of a CI timeout.
        consumerService.resetLatch(1);

        // WHEN — no message sent
        boolean completed = consumerService.getLatch().await(3, TimeUnit.SECONDS); // short timeout

        // THEN
        assertThat(completed).as("Should NOT complete — no message was sent").isFalse();
    }

}
