package com.vbforge.kafka.dashboard.service;

import com.vbforge.kafka.dashboard.model.DashboardStats;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Central in-memory aggregation store for the dashboard.
 *
 * All fields are thread-safe — multiple consumer threads and the WebSocket
 * broadcaster thread can access this concurrently without synchronization issues.
 *
 * LESSON — Why thread safety matters here:
 * The two consumer groups and the @Scheduled broadcaster all run on separate threads.
 * Using ConcurrentHashMap and AtomicLong prevents race conditions without the overhead
 * of synchronized blocks on every read.
 *
 * This is an in-memory store — data resets on restart. That is intentional for this
 * demo. Project 3 introduces persisting offsets and state to a database.
 */
@Service
public class DashboardStatsService {

    // ── Counters ─────────────────────────────────────────────────────────

    private final AtomicLong totalEvents = new AtomicLong(0);

    /** partition index → event count */
    private final ConcurrentHashMap<String, AtomicLong> eventsPerPartition = new ConcurrentHashMap<>();

    /** category (INFO/WARN/ERROR) → event count */
    private final ConcurrentHashMap<String, AtomicLong> eventsPerCategory = new ConcurrentHashMap<>();

    /** sender name → event count */
    private final ConcurrentHashMap<String, AtomicLong> senderCounts = new ConcurrentHashMap<>();

    /**
     * Rolling window: timestamps (epoch millis) of events in the last 60 seconds.
     */
    private final LinkedList<Long> rollingWindow = new LinkedList<>();
    private final Object rollingWindowLock = new Object();

    /** Last known offsets per consumer group+partition */
    private final ConcurrentHashMap<String, AtomicLong> consumerGroupOffsets = new ConcurrentHashMap<>();

    // ── Record an event ──────────────────────────────────────────────────

    public void record(String sender, String category, int partition) {
        totalEvents.incrementAndGet();

        String partitionKey = "partition-" + partition;
        eventsPerPartition
                .computeIfAbsent(partitionKey, k -> new AtomicLong(0))
                .incrementAndGet();

        String cat = (category != null && !category.isBlank()) ? category : "UNKNOWN";
        eventsPerCategory
                .computeIfAbsent(cat, k -> new AtomicLong(0))
                .incrementAndGet();

        String s = (sender != null && !sender.isBlank()) ? sender : "anonymous";
        senderCounts
                .computeIfAbsent(s, k -> new AtomicLong(0))
                .incrementAndGet();

        synchronized (rollingWindowLock) {
            rollingWindow.addLast(Instant.now().toEpochMilli());
        }
    }

    public void recordOffset(String groupId, int partition, long offset) {
        String key = groupId + ":partition-" + partition;
        consumerGroupOffsets
                .computeIfAbsent(key, k -> new AtomicLong(0))
                .set(offset);
    }

    // ── Build snapshot ───────────────────────────────────────────────────

    public DashboardStats buildSnapshot() {

        long cutoff = Instant.now().toEpochMilli() - 60_000;
        long eventsLastMinute;

        synchronized (rollingWindowLock) {
            while (!rollingWindow.isEmpty() && rollingWindow.getFirst() < cutoff) {
                rollingWindow.removeFirst();
            }
            eventsLastMinute = rollingWindow.size();
        }

        // Partition snapshot
        Map<String, Long> partitionSnapshot = eventsPerPartition.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().get()
                ));

        for (int i = 0; i < 3; i++) {
            partitionSnapshot.putIfAbsent("partition-" + i, 0L);
        }

        // Category snapshot
        Map<String, Long> categorySnapshot = eventsPerCategory.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().get()
                ));

        // ✅ FIXED: Top 5 senders
        Map<String, Long> topSenders = senderCounts.entrySet().stream()
                .sorted(Comparator.comparingLong(
                        (Map.Entry<String, AtomicLong> e) -> e.getValue().get()
                ).reversed())
                .limit(5)
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().get(),
                        (a, b) -> a,
                        LinkedHashMap::new
                ));

        // Offsets snapshot
        Map<String, Long> offsetSnapshot = consumerGroupOffsets.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().get()
                ));

        return DashboardStats.builder()
                .totalEvents(totalEvents.get())
                .eventsPerPartition(partitionSnapshot)
                .eventsPerCategory(categorySnapshot)
                .topSenders(topSenders)
                .eventsLastMinute(eventsLastMinute)
                .consumerGroupOffsets(offsetSnapshot)
                .build();
    }

    public long getTotalEvents() {
        return totalEvents.get();
    }
}