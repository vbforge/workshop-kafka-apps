package com.vbforge.case16.service;

import com.vbforge.case16.model.RequestMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

// JUNIOR NOTE: This service demonstrates TWO related patterns:
//
// 1. HEADER-BASED ROUTING (producer side):
//    Instead of letting the consumer decide what to do with a message,
//    the PRODUCER sets a header ("priority") and routes to different topics
//    based on that header value. This is the "smart producer, dumb consumer" pattern.
//    Each consumer only subscribes to the topic relevant to its priority class.
//
// 2. MESSAGE FILTERING (@KafkaFilter via RecordFilterStrategy in KafkaConfig):
//    When a single consumer subscribes to a topic that receives mixed traffic,
//    you can attach a RecordFilterStrategy to the container factory to drop
//    records before they reach your listener. In this service we demonstrate
//    both the routing (two separate topics) AND inline header inspection to
//    show you can read headers directly in the listener method.
//
// Header reading in @KafkaListener:
//    Spring Kafka maps Kafka headers to method parameters via @Header.
//    The header value is a byte[] — you convert it to String with new String(value).
//    Missing headers (null) need a default value: @Header(required=false, defaultValue="STANDARD")

@Service
@Slf4j
public class HeaderRoutingService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.routed-priority}")
    private String priorityTopic;

    @Value("${kafka.topics.routed-standard}")
    private String standardTopic;

    private final AtomicLong priorityProcessed = new AtomicLong(0);
    private final AtomicLong standardProcessed = new AtomicLong(0);

    public HeaderRoutingService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // ===================================================================
    // PRODUCER — routes to different topics based on priority header
    // ===================================================================

    public void routeMessage(String payload, String priority) {
        String requestId = UUID.randomUUID().toString();
        RequestMessage message = RequestMessage.builder()
                .requestId(requestId)
                .payload(payload)
                .priority(priority)
                .sentAt(LocalDateTime.now())
                .build();

        // JUNIOR NOTE: Choose the destination topic based on the priority field.
        // In this pattern, routing logic lives in the producer — it decides which
        // topic to publish to. Consumers are specialized: the priority consumer
        // only subscribes to the priority topic, the standard consumer to its own.
        //
        // Alternative pattern: publish ALL messages to one topic, set a header,
        // and use RecordFilterStrategy on the consumer side to filter. Both
        // approaches are valid — topic-per-priority is better for throughput
        // isolation; header-filter is simpler when you have many priority levels.
        String targetTopic = "HIGH".equalsIgnoreCase(priority) ? priorityTopic : standardTopic;

        ProducerRecord<String, Object> record = new ProducerRecord<>(targetTopic, requestId, message);
        record.headers().add(new RecordHeader("priority", priority.getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("routed-by", "HeaderRoutingService".getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(record);
        log.info(">>> [ROUTING] Routed id={} priority={} → topic={}", requestId, priority, targetTopic);
    }


    // ===================================================================
    // CONSUMERS — separate listeners per priority class
    // ===================================================================

    // JUNIOR NOTE: @Header("priority") reads the Kafka message header directly.
    // The value is a byte[] from the wire — Spring Kafka auto-converts it to String
    // when the parameter type is String. If the header is missing, required=false
    // means the parameter is null (don't throw). defaultValue gives a fallback.
    //
    // containerFactory = "routedListenerContainerFactory" — this factory has
    // RecordFilterStrategy attached (see KafkaConfig). In our setup the filter
    // passes everything, but this is where you'd add: return !header.equals("HIGH").
    @KafkaListener(
            topics = "${kafka.topics.routed-priority}",
            groupId = "${kafka.consumer.groupID}-priority",
            containerFactory = "routedListenerContainerFactory"
    )
    public void handlePriorityMessage(
            RequestMessage message,
            @Header(name = "priority", required = false, defaultValue = "UNKNOWN") String priority,
            @Header(name = "routed-by", required = false, defaultValue = "unknown") String routedBy
    ) {
        priorityProcessed.incrementAndGet();
        log.info(">>> [PRIORITY-CONSUMER] #{} id={} priority-header={} routed-by={} payload='{}'",
                priorityProcessed.get(), message.getRequestId(), priority, routedBy, message.getPayload());
    }

    @KafkaListener(
            topics = "${kafka.topics.routed-standard}",
            groupId = "${kafka.consumer.groupID}-standard",
            containerFactory = "routedListenerContainerFactory"
    )
    public void handleStandardMessage(
            RequestMessage message,
            @Header(name = "priority", required = false, defaultValue = "UNKNOWN") String priority,
            @Header(name = "routed-by", required = false, defaultValue = "unknown") String routedBy
    ) {
        standardProcessed.incrementAndGet();
        log.info(">>> [STANDARD-CONSUMER] #{} id={} priority-header={} routed-by={} payload='{}'",
                standardProcessed.get(), message.getRequestId(), priority, routedBy, message.getPayload());
    }

    public long getPriorityProcessed() { return priorityProcessed.get(); }
    public long getStandardProcessed() { return standardProcessed.get(); }

}
