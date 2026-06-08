package com.vbforge.case12.service;

import com.vbforge.case12.model.GenericMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
 
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
 
// JUNIOR NOTE: This is the global DLT consumer — the key new concept of case-12.
//
// In case-11, the DLT consumer only needed to handle one source topic.
// Here, three topics all route to ONE shared DLT.
// The DLT consumer uses the kafka_dlt-original-topic header to determine
// which topic the failed record came from — and can then apply topic-specific
// handling logic within the same consumer.
//
// This is the advantage of a shared DLT over per-topic DLTs:
//   - One consumer to monitor instead of three
//   - One alert threshold to configure
//   - Cross-topic failure correlation ("orders AND payments failing → common infra issue?")
//   - Simpler operational surface area
//
// Trade-off: if topics have wildly different failure volumes or urgency,
// separate DLTs allow independent alerting thresholds and consumer scaling.
 
@Service
@Slf4j
public class GlobalDltConsumer {
 
    // Track counts per source topic for the status endpoint
    private final ConcurrentHashMap<String, AtomicInteger> countBySourceTopic =
            new ConcurrentHashMap<>();
 
    @KafkaListener(
            topics = "${kafka.topic.dlt}",
            groupId = "${kafka.consumer.dltGroupID}",
            containerFactory = "dltContainerFactory"
    )
    public void consumeFromDlt(ConsumerRecord<String, GenericMessage> record) {
        // JUNIOR NOTE: Read the kafka_dlt-original-topic header to know which
        // source topic this record came from. This header is always set by
        // DeadLetterPublishingRecoverer — it's guaranteed to be present.
        String sourceTopic = extractHeader(record, "kafka_dlt-original-topic");
        String exceptionFqcn = extractHeader(record, "kafka_dlt-exception-fqcn");
        String exceptionMsg  = extractHeader(record, "kafka_dlt-exception-message");
        String originalOffset = extractHeader(record, "kafka_dlt-original-offset");
 
        countBySourceTopic.computeIfAbsent(sourceTopic, k -> new AtomicInteger(0))
                .incrementAndGet();
 
        GenericMessage msg = record.value();
 
        log.error("╔══════════════════════════════════════════");
        log.error("║  GLOBAL DLT — source: {}", sourceTopic);
        log.error("║  messageId:     {}", msg.getId());
        log.error("║  originalOffset: {}", originalOffset);
        log.error("║  exception:     {}: {}", exceptionFqcn, exceptionMsg);
        log.error("╠── Routing by source topic ──────────────");
 
        // JUNIOR NOTE: Branch on source topic to apply topic-specific DLT logic.
        // In production each branch might:
        //   - Alert a different Slack channel
        //   - Write to a different incident table
        //   - Apply a different replay strategy
        if (sourceTopic.contains("orders")) {
            log.error("║  [ORDERS-DLT] → alert order team, write to orders_failures table");
        } else if (sourceTopic.contains("payments")) {
            log.error("║  [PAYMENTS-DLT] → URGENT — alert finance team, page on-call");
        } else if (sourceTopic.contains("notifications")) {
            log.error("║  [NOTIFICATIONS-DLT] → log to notifications_failures, low priority");
        } else {
            log.error("║  [UNKNOWN-DLT] → unknown source topic: {}", sourceTopic);
        }
 
        log.error("╚══════════════════════════════════════════");
    }
 
    private String extractHeader(ConsumerRecord<?, ?> record, String headerKey) {
        var header = record.headers().lastHeader(headerKey);
        if (header == null) return "N/A";
        return new String(header.value(), StandardCharsets.UTF_8);
    }
 
    public ConcurrentHashMap<String, AtomicInteger> getCountBySourceTopic() {
        return countBySourceTopic;
    }
 
}