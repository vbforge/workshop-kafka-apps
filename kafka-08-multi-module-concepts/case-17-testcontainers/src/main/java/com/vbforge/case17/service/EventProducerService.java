package com.vbforge.case17.service;

import com.vbforge.case17.model.EventMessage;
import com.vbforge.case17.model.ProducerResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@Slf4j
@RequiredArgsConstructor
public class EventProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.events}")
    private String eventsTopic;

    public ProducerResponse sendBatch(int count, String type) {
        List<ProducerResponse.MessageSummary> summaries = new ArrayList<>();
        log.info(">>> [PRODUCER] Sending {} events of type={}", count, type);

        for (int i = 1; i <= count; i++) {
            EventMessage event = EventMessage.builder()
                    .id(UUID.randomUUID().toString())
                    .type(type)
                    .payload("Payload for event #" + i)
                    .sequenceNumber(i)
                    .createdAt(LocalDateTime.now())
                    .build();

            try {
                SendResult<String, Object> result = kafkaTemplate
                        .send(eventsTopic, event.getId(), event)
                        .get(5, TimeUnit.SECONDS);

                RecordMetadata meta = result.getRecordMetadata();
                summaries.add(ProducerResponse.MessageSummary.builder()
                        .messageId(event.getId())
                        .sequenceNumber(i)
                        .partition(meta.partition())
                        .offset(meta.offset())
                        .build());

            } catch (TimeoutException e) {
                throw new RuntimeException("Send timed out at event #" + i, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Send interrupted", e);
            } catch (ExecutionException e) {
                throw new RuntimeException("Broker error: " + e.getCause().getMessage(), e);
            }
        }

        log.info(">>> [PRODUCER] Sent {} events", summaries.size());
        return ProducerResponse.builder()
                .messagesSent(summaries.size())
                .messages(summaries)
                .sentAt(LocalDateTime.now())
                .build();
    }

    // JUNIOR NOTE: Package-accessible send for integration tests — avoids
    // test code needing to go through the HTTP layer just to produce a message.
    // Real test code calls this directly via the autowired service bean.
    public void sendSingle(EventMessage event) {
        try {
            kafkaTemplate.send(eventsTopic, event.getId(), event).get(5, TimeUnit.SECONDS);
            log.debug(">>> [PRODUCER] Sent single event id={}", event.getId());
        } catch (Exception e) {
            throw new RuntimeException("Failed to send event: " + e.getMessage(), e);
        }
    }

}
