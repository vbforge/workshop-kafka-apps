package com.vbforge.case09.service;

import com.vbforge.case09.model.ProducerResponse;
import com.vbforge.case09.model.TaskMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topic.name}")
    private String topic;

    public ProducerResponse send(String content, String failureMode) {
        if (failureMode == null || failureMode.isBlank()) failureMode = "none";

        TaskMessage message = TaskMessage.builder()
                .id(UUID.randomUUID().toString())
                .content(content != null ? content : "Task message")
                .failureMode(failureMode)
                .timestamp(LocalDateTime.now())
                .build();

        log.info(">>> [PRODUCER] Sending id={} failureMode={}", message.getId(), failureMode);

        try {
            SendResult<String, Object> result = kafkaTemplate
                    .send(topic, message.getId(), message)
                    .get(5, TimeUnit.SECONDS);

            RecordMetadata meta = result.getRecordMetadata();
            return ProducerResponse.builder()
                    .messageId(message.getId())
                    .failureMode(failureMode)
                    .partition(meta.partition())
                    .offset(meta.offset())
                    .sentAt(LocalDateTime.now())
                    .build();

        } catch (TimeoutException e) {
            throw new RuntimeException("Send timed out", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Send interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Broker error: " + e.getCause().getMessage(), e);
        }
    }

}
