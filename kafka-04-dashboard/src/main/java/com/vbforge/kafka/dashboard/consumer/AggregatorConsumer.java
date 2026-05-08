package com.vbforge.kafka.dashboard.consumer;

import com.vbforge.kafka.dashboard.model.DashboardEvent;
import com.vbforge.kafka.dashboard.service.DashboardStatsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
 
/**
 * Consumer Group 1 — dashboard-aggregator-group
 *
 * Purpose: aggregate stats for the live dashboard (total count, per-partition,
 * per-category, top senders, rolling rate).
 *
 * AckMode: BATCH — Spring commits offsets automatically after each poll batch.
 * This is appropriate here because losing a few stats on a crash is acceptable.
 * We are not persisting anything critical — just updating in-memory counters.
 *
 * LESSON — ConsumerRecord<K, V>:
 * Instead of receiving just the value (DashboardEvent), we receive the full
 * ConsumerRecord which gives us access to:
 *   - record.partition()  → which partition this message came from
 *   - record.offset()     → position within that partition
 *   - record.key()        → the partition routing key (sender name)
 *   - record.value()      → the deserialized DashboardEvent payload
 *
 * This is the standard way to get partition metadata in a @KafkaListener.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class AggregatorConsumer {
 
    private final DashboardStatsService statsService;
 
    @KafkaListener(
            topics = "${app.kafka.topic}",
            groupId = "${app.kafka.aggregator-group}",
            containerFactory = "aggregatorListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, DashboardEvent> record) {
        DashboardEvent event = record.value();
 
        if (event == null) {
            log.warn("Received null event on partition {} offset {} — skipping",
                    record.partition(), record.offset());
            return;
        }
 
        log.debug("Aggregator received | partition={} | offset={} | sender={} | category={}",
                record.partition(), record.offset(), event.getSender(), event.getCategory());
 
        // Feed the in-memory aggregation store
        statsService.record(event.getSender(), event.getCategory(), record.partition());
 
        // Track this group's consumed offset per partition
        statsService.recordOffset("aggregator", record.partition(), record.offset());
    }
}