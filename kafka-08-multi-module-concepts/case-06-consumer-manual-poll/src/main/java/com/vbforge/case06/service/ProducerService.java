package com.vbforge.case06.service;

import com.vbforge.case06.model.ProducerResponse;
import com.vbforge.case06.model.WorkMessage;
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
public class ProducerService {

    public static final int TIMEOUT = 5;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topic.name}")
    private String topic;

    public ProducerResponse sendBulk(int count) {

        List<ProducerResponse.MessageSummary> summaries = new ArrayList<>();
        log.info(">>> [PRODUCER] Sending {} messages", count);

        for (int i = 1; i <= count; i++) {

            WorkMessage message = WorkMessage.builder()
                    .id(UUID.randomUUID().toString())
                    .content("Message #" + i + " - sent at " + LocalDateTime.now())
                    .timestamp(LocalDateTime.now())
                    .build();

            try {
                SendResult<String, Object> result = kafkaTemplate
                        .send(topic, message)
                        .get(TIMEOUT, TimeUnit.SECONDS);

                RecordMetadata meta = result.getRecordMetadata();
                log.info(">>> [PRODUCER] Sent msg #{} → partition={} offset={}", i, meta.partition(), meta.offset());

                summaries.add(ProducerResponse.MessageSummary.builder()
                        .messageId(message.getId())
                        .offset(meta.offset())
                        .partition(meta.partition())
                        .build());

            } catch (TimeoutException e) {
                throw new RuntimeException("Send timed out at message #" + i, e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Send interrupted", e);
            } catch (ExecutionException e) {
                throw new RuntimeException("Broker error: " + e.getCause().getMessage(), e);
            }

        }

        return ProducerResponse.builder()
                .messagesSent(summaries.size())
                .messages(summaries)
                .sentAt(LocalDateTime.now())
                .build();

    }


}
