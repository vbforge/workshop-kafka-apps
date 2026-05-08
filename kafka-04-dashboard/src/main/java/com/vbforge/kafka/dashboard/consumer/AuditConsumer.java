package com.vbforge.kafka.dashboard.consumer;

import com.vbforge.kafka.dashboard.model.AuditEntry;
import com.vbforge.kafka.dashboard.model.DashboardEvent;
import com.vbforge.kafka.dashboard.service.AuditLogService;
import com.vbforge.kafka.dashboard.service.DashboardStatsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
 
import java.time.LocalDateTime;
 
/**
 * Consumer Group 2 — dashboard-audit-group
 *
 * Purpose: write a raw audit log of every event with full partition metadata.
 * This is what the UI shows in the "Live Event Feed" panel.
 *
 * AckMode: MANUAL_IMMEDIATE
 * The method signature includes an Acknowledgment parameter.
 * Spring will NOT commit the offset until ack.acknowledge() is called explicitly.
 *
 * LESSON — Why manual ack matters here:
 * If auditLogService.add(entry) throws an exception before we call ack,
 * the offset is NOT committed. On restart, this message will be redelivered.
 * With BATCH mode, the offset would already be committed and the message lost.
 *
 * LESSON — Two groups, same topic:
 * Both AggregatorConsumer and AuditConsumer listen to the same topic.
 * Each group has its own independent offset pointer — they do not interfere.
 * Kafka delivers every message to every consumer group subscribed to the topic.
 * This is the core pub-sub model: one producer, multiple independent consumers.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class AuditConsumer {
 
    private final AuditLogService auditLogService;
    private final DashboardStatsService statsService;
 
    @KafkaListener(
            topics = "${app.kafka.topic}",
            groupId = "${app.kafka.audit-group}",
            containerFactory = "auditListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, DashboardEvent> record, Acknowledgment ack) {
        DashboardEvent event = record.value();
 
        if (event == null) {
            log.warn("Audit: null event on partition {} offset {} — acking and skipping",
                    record.partition(), record.offset());
            ack.acknowledge();
            return;
        }
 
        log.debug("Audit received | partition={} | offset={} | sender={}",
                record.partition(), record.offset(), event.getSender());
 
        // Build the audit entry — this is what the UI displays in the event feed
        AuditEntry entry = AuditEntry.builder()
                .sender(event.getSender())
                .content(event.getContent())
                .category(event.getCategory())
                .partition(record.partition())
                .offset(record.offset())
                .consumedAt(LocalDateTime.now())
                .build();
 
        // Store the entry — only commit the offset AFTER this succeeds
        auditLogService.add(entry);
 
        // Track this group's offset per partition
        statsService.recordOffset("audit", record.partition(), record.offset());
 
        // Explicitly commit the offset — this is the manual ack
        ack.acknowledge();
 
        log.debug("Audit ack committed | partition={} | offset={}", record.partition(), record.offset());
    }
}