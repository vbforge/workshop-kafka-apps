package com.vbforge.kafka.dashboard.model;

import lombok.Builder;
import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * A point-in-time snapshot of aggregated dashboard statistics.
 * This is serialized to JSON and pushed to WebSocket clients every second.
 *
 * Kept as a plain value object — no Kafka logic here.
 */
@Data
@Builder
public class DashboardStats {

    /** Total events received since the application started. */
    private long totalEvents;

    /**
     * Events received per partition key (e.g. "partition-0", "partition-1", "partition-2").
     * This is the core "partition awareness" lesson — same topic, different partitions.
     */
    private Map<String, Long> eventsPerPartition;

    /**
     * Events received per category (INFO / WARN / ERROR).
     * Simple in-memory aggregation — no state store, no Kafka Streams.
     */
    private Map<String, Long> eventsPerCategory;

    /**
     * Top 5 senders by message count.
     */
    private Map<String, Long> topSenders;

    /**
     * Rolling message rate — events in the last 60 seconds.
     */
    private long eventsLastMinute;

    /**
     * Consumer group lag snapshot — how far behind each group is.
     * Populated by the aggregator listener.
     */
    private Map<String, Long> consumerGroupOffsets;

}
