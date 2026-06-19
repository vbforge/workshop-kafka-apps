package com.vbforge.case14.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vbforge.case14.model.OrderEventDto;
import com.vbforge.case14.model.ValidationResult;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

// JUNIOR NOTE: The Validator bean is provided by Spring's validation auto-configuration
// (triggered by spring-boot-starter-validation on the classpath). It's the Jakarta Validator —
// the programmatic API that backs @Valid in controllers.
// Injecting it here lets us call validator.validate(dto) explicitly, which returns
// Set<ConstraintViolation<T>> — one entry per failed constraint.
//
// Why validate in the consumer and not rely on @Valid on the @KafkaListener method?
// @Valid on @KafkaListener parameters works, but it throws MethodArgumentNotValidException
// and sends the message straight to the error handler — no context about which orderId failed,
// no chance to publish to a rejected topic. Manual validation gives full control.

@Service
@Slf4j
public class ValidationConsumerService {

    private final Validator validator;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${kafka.topic.rejected}")
    private String rejectedTopic;

    // ── Counters ──
    private final AtomicLong totalReceived = new AtomicLong(0);
    private final AtomicLong totalAccepted = new AtomicLong(0);
    private final AtomicLong totalRejected = new AtomicLong(0);


    public ValidationConsumerService(Validator validator,
                                     KafkaTemplate<String, Object> kafkaTemplate,
                                     ObjectMapper objectMapper) {
        this.validator = validator;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "${kafka.topic.events}",
            groupId = "${kafka.consumer.groupID}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            ConsumerRecord<String, Object> record,
            Acknowledgment ack
    ) {
        totalReceived.incrementAndGet();
        log.info(">>> [CONSUMER] Received message | partition={} offset={} key={}",
                record.partition(), record.offset(), record.key());

        // JUNIOR NOTE: The consumer receives Object (deserialized from JSON as a LinkedHashMap).
        // We use ObjectMapper to convert it to the strongly-typed DTO.
        // This is the "late binding" approach — you keep the Kafka config generic and do the
        // type conversion in the service. An alternative is to configure JsonDeserializer with
        // the exact target type (see KafkaConfig comments).
        OrderEventDto dto;
        try {
            dto = objectMapper.convertValue(record.value(), OrderEventDto.class);
        } catch (IllegalArgumentException e) {
            log.error(">>> [CONSUMER] Cannot deserialize message to OrderEventDto: {}", e.getMessage());
            totalRejected.incrementAndGet();
            ack.acknowledge();
            return;
        }

        // ── Validate ──
        Set<ConstraintViolation<OrderEventDto>> violations = validator.validate(dto);

        if (violations.isEmpty()) {
            processValid(dto);
            totalAccepted.incrementAndGet();
        } else {
            processInvalid(dto, violations);
            totalRejected.incrementAndGet();
        }

        // JUNIOR NOTE: We commit in BOTH branches. A rejected message is not a Kafka error —
        // it's a business-logic decision. We've handled it (published to rejected topic),
        // so we advance the offset. NOT committing here would cause infinite redelivery
        // because the message would never pass validation.
        ack.acknowledge();
    }

    private void processValid(OrderEventDto dto) {
        log.info(">>> [VALID] orderId={} type={} amount={} items={}",
                dto.getOrderId(),
                dto.getOrderType(),
                dto.getTotalAmount(),
                dto.getItems() != null ? dto.getItems().size() : 0);
        // In production: forward to downstream service, persist to DB, etc.
    }

    private void processInvalid(OrderEventDto dto, Set<ConstraintViolation<OrderEventDto>> violations) {
        // JUNIOR NOTE: ConstraintViolation.getPropertyPath() gives you the field name
        // (including nested path like "deliveryAddress.postalCode").
        // getMessage() gives the human-readable message from the annotation.
        List<String> messages = new ArrayList<>();
        for (ConstraintViolation<OrderEventDto> v : violations) {
            String msg = v.getPropertyPath() + ": " + v.getMessage();
            messages.add(msg);
            log.warn("  ├─ VIOLATION: {}", msg);
        }

        log.warn(">>> [REJECTED] orderId={} — {} violation(s)",
                dto.getOrderId() != null ? dto.getOrderId() : "<null>",
                violations.size());

        ValidationResult result = ValidationResult.builder()
                .valid(false)
                .orderId(dto.getOrderId())
                .violations(messages)
                .processedAt(LocalDateTime.now())
                .build();

        // Publish to rejected topic for observability / alerting
        kafkaTemplate.send(rejectedTopic, dto.getOrderId(), result);
        log.info(">>> [REJECTED] Published to topic={}", rejectedTopic);
    }

    public ConsumerStats getStats() {
        return ConsumerStats.builder()
                .totalReceived(totalReceived.get())
                .totalAccepted(totalAccepted.get())
                .totalRejected(totalRejected.get())
                .build();
    }

    @lombok.Builder
    @lombok.Data
    public static class ConsumerStats {
        private long totalReceived;
        private long totalAccepted;
        private long totalRejected;
    }

}
