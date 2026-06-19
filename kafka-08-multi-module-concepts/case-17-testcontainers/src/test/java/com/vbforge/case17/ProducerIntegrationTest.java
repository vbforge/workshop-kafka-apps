package com.vbforge.case17;

import com.vbforge.case17.model.EventMessage;
import com.vbforge.case17.model.ProducerResponse;
import com.vbforge.case17.service.EventProducerService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

// JUNIOR NOTE: @SpringBootTest starts the FULL application context.
// This means all @Bean, @Service, @KafkaListener, etc. are created —
// exactly as they would be in production. The only difference is that
// @DynamicPropertySource (from the base class) has pointed bootstrap-servers
// to the Testcontainers Kafka instead of localhost:9092.
//
// This test verifies the PRODUCER in isolation:
//   1. Send N messages using EventProducerService
//   2. Create a raw KafkaConsumer manually (not via Spring) to read what was produced
//   3. Assert the records on the topic match what we sent
//
// A raw KafkaConsumer in the test gives you full control over which group,
// offset, and topic to read — no Spring listener interference.

@SpringBootTest
class ProducerIntegrationTest extends AbstractKafkaIntegrationTest {

    @Autowired
    private EventProducerService producerService;

    @Test
    @DisplayName("sendBatch: produced messages should appear on the topic with correct content")
    void sendBatch_shouldProduceMessagesToKafka() throws Exception {
        // GIVEN
        int count = 3;
        String type = "ORDER_PLACED";

        long offsetBefore = getEndOffset("case-17-events-topic");

        // WHEN
        ProducerResponse response = producerService.sendBatch(count, type);

        // THEN — response assertions
        assertThat(response.getMessagesSent()).isEqualTo(count);
        assertThat(response.getMessages()).hasSize(count);
        assertThat(response.getSentAt()).isNotNull();
        response.getMessages().forEach(m -> {
            assertThat(m.getMessageId()).isNotBlank();
            assertThat(m.getOffset()).isGreaterThanOrEqualTo(0);
        });

        // THEN — verify records actually landed on the topic
        List<ConsumerRecord<String, EventMessage>> consumed = consumeFromOffset(
                "case-17-events-topic", count, "producer-test-group-" + UUID.randomUUID(), offsetBefore
        );

        assertThat(consumed).hasSize(count);
        consumed.forEach(record -> {
            assertThat(record.value().getType()).isEqualTo(type);
            assertThat(record.value().getPayload()).isNotBlank();
            assertThat(record.value().getCreatedAt()).isNotNull();
        });
    }

    @Test
    @DisplayName("sendSingle: a single EventMessage should appear on the topic exactly once")
    void sendSingle_shouldProduceOneRecord() throws Exception {
        // GIVEN
        String uniqueId = UUID.randomUUID().toString();
        EventMessage event = EventMessage.builder()
                .id(uniqueId)
                .type("USER_REGISTERED")
                .payload("test-payload-" + uniqueId)
                .sequenceNumber(1)
                .createdAt(LocalDateTime.now())
                .build();

        // WHEN — record the offset BEFORE sending, then consume only what this test produces
        // JUNIOR NOTE: We snapshot the end offset before sending so the raw consumer
        // seeks past any messages left by previous tests. This is the correct isolation
        // pattern when multiple tests share one topic and auto.offset.reset=earliest
        // would otherwise replay all prior messages.
        long offsetBefore = getEndOffset("case-17-events-topic");
        producerService.sendSingle(event);

        // THEN
        List<ConsumerRecord<String, EventMessage>> consumed = consumeFromOffset(
                "case-17-events-topic", 1, "single-test-group-" + UUID.randomUUID(), offsetBefore
        );

        assertThat(consumed).hasSize(1);
        EventMessage received = consumed.get(0).value();
        assertThat(received.getId()).isEqualTo(uniqueId);
        assertThat(received.getType()).isEqualTo("USER_REGISTERED");
        assertThat(received.getPayload()).isEqualTo("test-payload-" + uniqueId);
    }


    // ── Helpers ──

    // JUNIOR NOTE: Snapshot the current end offset of partition 0 before sending.
// Used to seek the raw consumer past any messages from prior tests.
    private long getEndOffset(String topic) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "offset-probe-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        );
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            var tp = new org.apache.kafka.common.TopicPartition(topic, 0);
            consumer.assign(List.of(tp));
            consumer.seekToEnd(List.of(tp));
            return consumer.position(tp);
        }
    }

    // JUNIOR NOTE: Reads exactly expectedCount records starting from startOffset.
// This isolates each test to only the messages it produced, regardless of
// what prior tests left on the topic.
    private List<ConsumerRecord<String, EventMessage>> consumeFromOffset(
            String topic, int expectedCount, String groupId, long startOffset) {

        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class,
                JacksonJsonDeserializer.TRUSTED_PACKAGES, "*",
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, EventMessage.class.getName()
        );

        List<ConsumerRecord<String, EventMessage>> result = new ArrayList<>();
        try (KafkaConsumer<String, EventMessage> consumer = new KafkaConsumer<>(props)) {
            var tp = new org.apache.kafka.common.TopicPartition(topic, 0);
            consumer.assign(List.of(tp));
            consumer.seek(tp, startOffset);  // skip past all prior messages
            long deadline = System.currentTimeMillis() + 15_000;
            while (result.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, EventMessage> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(result::add);
            }
        }
        return result;
    }



    // ── Helper ──

    // JUNIOR NOTE: This raw consumer is created with a UNIQUE group ID each test.
    // Using a unique group = auto.offset.reset=earliest fetches ALL records from
    // the topic start. This makes the test deterministic — we always get the exact
    // records this test produced, not records from a previous test run.
    //
    // We poll in a loop because poll() may not return all records in one call —
    // especially for the first poll when metadata needs to be fetched.
    // Stop when we've collected enough records or we've been polling too long.
    private List<ConsumerRecord<String, EventMessage>> consumeFromTopic(
            String topic, int expectedCount, String groupId) {

        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class,
                JacksonJsonDeserializer.TRUSTED_PACKAGES, "*",
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, EventMessage.class.getName()
        );

        List<ConsumerRecord<String, EventMessage>> result = new ArrayList<>();
        try (KafkaConsumer<String, EventMessage> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 15_000; // 15s max
            while (result.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, EventMessage> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(result::add);
            }
        }
        return result;
    }

}
