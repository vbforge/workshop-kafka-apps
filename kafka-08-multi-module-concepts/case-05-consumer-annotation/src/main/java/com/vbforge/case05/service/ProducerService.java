package com.vbforge.case05.service;

import com.vbforge.case05.model.ProducerResponse;
import com.vbforge.case05.model.WorkMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

// JUNIOR NOTE: The producer in case-05 is intentionally minimal — its only job is
// to generate enough messages to make concurrent consumption visible.
//
// The bulk-send endpoint sends N messages to each of the provided keys.
// Since keys route deterministically to partitions, sending to 3 different keys
// (one per partition) ensures all 3 consumer threads get work simultaneously.
//
// We use sync send (.get()) here so the HTTP response includes confirmed partition
// assignments — this lets you verify "yes, messages went to all 3 partitions"
// before checking the consumer logs.


@Configuration
@Slf4j
@RequiredArgsConstructor
public class ProducerService {

    public static final int TIMEOUT = 5;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topic.name}")
    private String topic;

    // Default keys chosen to spread across all 3 partitions via murmur2 hash.
    // "alpha", "beta", "gamma" tested to land on different partitions on a 3-partition topic.
    // You can override them via the API if you want to experiment with other keys.
    private static final List<String> DEFAULT_KEYS = List.of("alpha", "beta", "gamma"); //assume we have 3 keys for 3 partition by design


    // BULK SEND — send messagesPerKey messages to each key
    public ProducerResponse sendBulk(List<String> keys, int messagesPerKey) {
        if (keys == null || keys.isEmpty()) {
            keys = DEFAULT_KEYS;
        }

        List<ProducerResponse.MessageSummary> summaries = new ArrayList<>();
        log.info(">>> [BULK] Sending {} messages x {} keys = {} total", messagesPerKey, keys.size(), messagesPerKey * keys.size());

        for (String key : keys) {
            for (int i = 1; i <= messagesPerKey; i++) {
                WorkMessage workMessage = WorkMessage.builder()
                        .id(UUID.randomUUID().toString())
                        .key(key)
                        .content("Message " + i + " for key = " + key)
                        .timestamp(LocalDateTime.now())
                        .build();

                try {
                    //SendResult
                    SendResult<String, Object> result = kafkaTemplate.send(topic, key, workMessage).get(TIMEOUT, TimeUnit.SECONDS);

                    //RecordMetadata
                    RecordMetadata meta = result.getRecordMetadata();
                    log.info(">>> [BULK] Sent key='{}' msg={} → partition={} offset={}", key, i, meta.partition(), meta.offset());

                    //summaries
                    summaries.add(ProducerResponse.MessageSummary.builder()
                            .messageId(workMessage.getId())
                            .key(key)
                            .partition(meta.partition())
                            .offset(meta.offset())
                            .build());

                } catch (TimeoutException e) {
                    throw new RuntimeException("Send timed out for key=" + key, e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Send interrupted", e);
                } catch (ExecutionException e) {
                    throw new RuntimeException("Broker error: " + e.getCause().getMessage(), e);

                }

            }
        }

        return ProducerResponse.builder()
                .messagesSent(summaries.size())
                .messages(summaries)
                .sentAt(LocalDateTime.now())
                .build();

    }


    //SINGLE SEND — send one message with a specific key
    public ProducerResponse.MessageSummary sendSingle(String key, String content){

        WorkMessage workMessage = WorkMessage.builder()
                .id(UUID.randomUUID().toString())
                .key(key)
                .content(content != null ? content : "Single message with key=" + key)
                .timestamp(LocalDateTime.now())
                .build();

        try{
            SendResult<String , Object> result = kafkaTemplate.send(topic, key, workMessage).get(TIMEOUT, TimeUnit.SECONDS);

            RecordMetadata meta = result.getRecordMetadata();
            log.info(">>> [SINGLE] key='{}' → partition={} offset={}", key, meta.partition(), meta.offset());

            return ProducerResponse.MessageSummary.builder()
                    .messageId(workMessage.getId())
                    .key(key)
                    .partition(meta.partition())
                    .offset(meta.offset())
                    .build();

        }catch (TimeoutException e) {
            throw new RuntimeException("Send timed out", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Send interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Broker error: " + e.getCause().getMessage(), e);
        }


    }


}























