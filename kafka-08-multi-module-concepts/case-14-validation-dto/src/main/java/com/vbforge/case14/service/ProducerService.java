package com.vbforge.case14.service;

import com.vbforge.case14.model.OrderEventDto;
import com.vbforge.case14.model.ProducerResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topic.events}")
    private String eventsTopic;

    // JUNIOR NOTE: We send the DTO as-is to Kafka — validation happens on the consumer side.
    // This simulates a real-world scenario where the producer is a different service
    // (or an external system) that may not enforce the same rules.
    // The consumer is responsible for validating what it receives, not trusting the producer.
    public ProducerResponse send(OrderEventDto dto) {
        log.info(">>> [PRODUCER] Sending order event: orderId={}", dto.getOrderId());

        try {
            SendResult<String, Object> result = kafkaTemplate
                    .send(eventsTopic, dto.getOrderId(), dto)
                    .get(5, TimeUnit.SECONDS);

            RecordMetadata meta = result.getRecordMetadata();
            log.info(">>> [PRODUCER] Sent to partition={} offset={}", meta.partition(), meta.offset());

            return ProducerResponse.builder()
                    .orderId(dto.getOrderId())
                    .topicSentTo(eventsTopic)
                    .partition(meta.partition())
                    .offset(meta.offset())
                    .sentAt(LocalDateTime.now())
                    .build();

        } catch (TimeoutException e) {
            throw new RuntimeException("Send timed out for orderId=" + dto.getOrderId(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Send interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Broker error: " + e.getCause().getMessage(), e);
        }
    }

}
