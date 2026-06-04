package com.vbforge.case07.service;

import com.vbforge.case07.model.OrderEvent;
import com.vbforge.case07.model.ProducerResponse;
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

    private static final List<String> REGIONS   = List.of("EU", "US", "ASIA");
    private static final List<String> STATUSES  = List.of("PLACED", "PAID", "SHIPPED");

    public ProducerResponse sendBatch(int count){

        List<ProducerResponse.MessageSummary> summaries = new ArrayList<>();
        log.info(">>> [PRODUCER] Sending {} order events", count);

        for (int i = 1; i <= count; i++) {
            String region   = REGIONS.get((i - 1) % REGIONS.size());
            String status   = STATUSES.get((i - 1) % STATUSES.size());

            OrderEvent event = OrderEvent.builder()
                    .orderId("ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase())
                    .customerId("CUST-" + (1000 + i))
                    .region(region)
                    .amount(Math.round((10.0 + i * 7.5) * 100.0) / 100.0)
                    .status(status)
                    .timestamp(LocalDateTime.now())
                    .build();

            try{
                SendResult<String, Object> result = kafkaTemplate
                        .send(topic, region, event)
                        .get(TIMEOUT, TimeUnit.SECONDS);

                RecordMetadata meta = result.getRecordMetadata();
                log.info(">>> [PRODUCER] orderId={} region={} status={} -> partition={} offset={}",
                        event.getOrderId(), region, status, meta.partition(), meta.offset());

                summaries.add(ProducerResponse.MessageSummary.builder()
                        .orderId(event.getOrderId())
                        .region(region)
                        .partition(meta.partition())
                        .offset(meta.offset())
                        .build());

            } catch (TimeoutException e) {
                throw new RuntimeException("Send timed out", e);
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
