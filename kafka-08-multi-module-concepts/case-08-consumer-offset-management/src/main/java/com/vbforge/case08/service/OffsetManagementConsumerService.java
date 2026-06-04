package com.vbforge.case08.service;

import com.vbforge.case08.model.EventMessage;
import com.vbforge.case08.model.OffsetStatus;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

// JUNIOR NOTE: This is THE central lesson of case-08.
//
// case-06 introduced manual poll and showed you own the loop.
// case-08 goes deeper — it's entirely about OFFSET OPERATIONS:
//
//   seekToBeginning()   → rewind to offset 0 on ALL assigned partitions
//                         Use case: replay entire topic from scratch
//                                   disaster recovery, re-processing after a bug fix
//
//   seekToEnd()         → jump to the latest offset (skip unread messages)
//                         Use case: "I don't care about the backlog, start fresh"
//                                   clearing lag intentionally after an outage
//
//   seekToOffset()      → seek to a SPECIFIC offset on a SPECIFIC partition
//                         Use case: skip a poison pill (seek past the bad offset)
//                                   custom checkpoint recovery
//                                   partial replay (replay only partition 2 from offset 5)
//
//   commitSync(map)     → manually commit specific offsets per partition
//                         Use case: commit only after your downstream work succeeds
//
// Key vocabulary distinction — internalized and demonstrated here:
//
//   consumer.position(tp)              → the NEXT offset this consumer will fetch
//                                        advances automatically after every poll()
//                                        NOT persisted to Kafka
//
//   consumer.committed(partitions)     → the LAST COMMITTED offset for each partition
//                                        what the consumer will resume from after restart
//                                        persisted to Kafka's __consumer_offsets topic
//
// These are different numbers. After polling 10 records and NOT committing:
//   position = 10  (consumer fetched records 0-9, next fetch is 10)
//   committed = 0  (no commitSync called — broker still thinks we're at 0)
// Restart now → consumer starts at 0 and reprocesses records 0-9.
// This is at-least-once delivery in action.

@Service
@Slf4j
public class OffsetManagementConsumerService {

    private final Map<String, Object> consumerProperties;

    @Value("${kafka.topic.name}")
    private String topic;

    @Value("${kafka.consumer.poll-timeout-ms}")
    private long pollTimeoutMs;

    // ── Loop control ──
    private final AtomicBoolean running       = new AtomicBoolean(false);
    private final AtomicBoolean paused        = new AtomicBoolean(false);

    // ── Seek signals — set by HTTP thread, read by poll thread ──
    private enum SeekCommand { NONE, BEGINNING, END, SPECIFIC }
    private volatile SeekCommand pendingSeek      = SeekCommand.NONE;
    private volatile int  seekPartition           = 0;
    private volatile long seekOffset              = 0;

    // ── Commit-without-processing signal ──
    // JUNIOR NOTE: "skip" means: advance the committed offset WITHOUT processing.
    // Equivalent to acknowledging a poison-pill record you want to discard.
    private final AtomicBoolean skipRequested = new AtomicBoolean(false);
    private volatile int  skipPartition = 0;
    private volatile long skipOffset    = 0;

    // ── Counters ──
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong totalCommitted = new AtomicLong(0);

    private volatile KafkaConsumer<String, EventMessage> consumer;

    private final ExecutorService pollExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "offset-poll-thread");
        t.setDaemon(true);
        return t;
    });

    public OffsetManagementConsumerService(
            @Qualifier("consumerProperties") Map<String, Object> consumerProperties) {
        this.consumerProperties = consumerProperties;
    }


    // ===================================================================
    // LIFECYCLE
    // ===================================================================

    @PostConstruct
    public void start() {
        running.set(true);
        pollExecutor.submit(this::pollLoop);
        log.info(">>> [OFFSET-MGT] Consumer loop started");
    }

    @PreDestroy
    public void stop() {
        running.set(false);
        if (consumer != null) consumer.wakeup();
        pollExecutor.shutdown();
        try {
            if (!pollExecutor.awaitTermination(10, TimeUnit.SECONDS))
                pollExecutor.shutdownNow();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pollExecutor.shutdownNow();
        }
    }


    // ===================================================================
    // POLL LOOP
    // ===================================================================

    private void pollLoop() {
        consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(List.of(topic));
        log.info(">>> [OFFSET-MGT] Subscribed to topic: {}", topic);

        try {
            while (running.get()) {

                // ── Handle pause/resume ──
                Set<TopicPartition> assigned = consumer.assignment();
                if (!assigned.isEmpty()) {
                    if (paused.get()) {
                        Set<TopicPartition> notYetPaused = new HashSet<>(assigned);
                        notYetPaused.removeAll(consumer.paused());
                        if (!notYetPaused.isEmpty()) {
                            consumer.pause(notYetPaused);
                            log.info(">>> [OFFSET-MGT] Paused: {}", notYetPaused);
                        }
                    } else {
                        Set<TopicPartition> currentlyPaused = consumer.paused();
                        if (!currentlyPaused.isEmpty()) {
                            consumer.resume(currentlyPaused);
                            log.info(">>> [OFFSET-MGT] Resumed: {}", currentlyPaused);
                        }
                    }
                }

                // ── Handle seek commands ──
                // JUNIOR NOTE: We check pendingSeek BEFORE polling because seek takes
                // effect on the NEXT poll(). The sequence is:
                //   HTTP thread sets pendingSeek → poll thread executes seek() → next poll() fetches from new position
                // If we polled first and THEN seeked, we'd have already fetched records
                // from the old position that we'd need to discard.
                if (pendingSeek != SeekCommand.NONE && !assigned.isEmpty()) {
                    executeSeek(assigned);
                    pendingSeek = SeekCommand.NONE;
                }

                // ── Handle skip (commit without process) ──
                if (skipRequested.compareAndSet(true, false) && !assigned.isEmpty()) {
                    executeSkip();
                }

                ConsumerRecords<String, EventMessage> records =
                        consumer.poll(Duration.ofMillis(pollTimeoutMs));

                if (records.isEmpty()) {
                    log.debug(">>> [OFFSET-MGT] Empty poll");
                    continue;
                }

                log.info(">>> [OFFSET-MGT] Polled {} records", records.count());

                Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();

                for (ConsumerRecord<String, EventMessage> record : records) {
                    processRecord(record);
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    toCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));
                }

                consumer.commitSync(toCommit);
                totalCommitted.addAndGet(records.count());
                log.info(">>> [OFFSET-MGT] Committed {} offsets | positions={}",
                        records.count(), currentPositionMap());

            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            if (running.get()) {
                log.error(">>> [OFFSET-MGT] Unexpected wakeup", e);
            } else {
                log.info(">>> [OFFSET-MGT] Wakeup received - shutting down");
            }
        } finally {
            consumer.close();
            log.info(">>> [OFFSET-MGT] KafkaConsumer closed");
        }
    }


    // ===================================================================
    // SEEK EXECUTION — runs on poll thread
    // ===================================================================

    private void executeSeek(Set<TopicPartition> assigned) {
        switch (pendingSeek) {

            case BEGINNING -> {
                // JUNIOR NOTE: seekToBeginning() rewinds ALL assigned partitions to offset 0.
                // The next poll() will return records starting from offset 0 on every partition.
                // This is a POSITION change — committed offsets in Kafka are unchanged.
                // If you restart without committing after this seek, the consumer will again
                // start from the last committed offset (not from 0).
                // To make the rewind permanent, call commitSync() with offset 0 after seeking.
                consumer.seekToBeginning(assigned);
                log.info(">>> [SEEK] seekToBeginning - rewound {} partitions to offset 0", assigned.size());
            }

            case END -> {
                // JUNIOR NOTE: seekToEnd() jumps ALL assigned partitions to the latest offset.
                // The next poll() returns NOTHING until new messages arrive.
                // Use case: intentionally skip the backlog — "I don't care about messages
                // that built up while I was down, start processing only new ones."
                // This is destructive from a business perspective — those messages won't
                // be processed by this group. Make sure that's intentional.
                consumer.seekToEnd(assigned);
                log.info(">>> [SEEK] seekToEnd - skipped to latest offset on {} partitions", assigned.size());
            }

            case SPECIFIC -> {
                // JUNIOR NOTE: seek(tp, offset) moves a SINGLE partition to a SPECIFIC offset.
                // This is the surgical tool:
                //   - Skip poison pill at offset 7: seek to offset 8
                //   - Replay just partition 2 from offset 5: seek partition 2 to 5
                //   - Custom checkpoint recovery: read your DB checkpoint, seek to it
                TopicPartition tp = new TopicPartition(topic, seekPartition);
                if (assigned.contains(tp)) {
                    consumer.seek(tp, seekOffset);
                    log.info(">>> [SEEK] seek partition={} to offset={}", seekPartition, seekOffset);
                } else {
                    log.warn(">>> [SEEK] partition={} not assigned to this consumer - seek ignored", seekPartition);
                }
            }

            default -> {}
        }
    }


    // ===================================================================
    // SKIP EXECUTION — commit an offset without processing the record
    // ===================================================================

    private void executeSkip() {
        // JUNIOR NOTE: "Skip" = commit offset N+1 without processing record at offset N.
        // The record at offset N is effectively discarded — the consumer will never
        // process it again (assuming no manual seek back).
        // This is how you handle a poison pill without crashing the consumer:
        //   1. Detect the bad record (deserialization failure, business rule violation)
        //   2. Log/alert it
        //   3. Call skip(partition, badOffset) — commits badOffset+1
        //   4. Consumer resumes normally at badOffset+1
        // In production you'd first publish the bad record to a Dead Letter Topic (case-11)
        // before committing past it, so the record isn't silently lost.
        TopicPartition tp = new TopicPartition(topic, skipPartition);
        Map<TopicPartition, OffsetAndMetadata> skipCommit =
                Map.of(tp, new OffsetAndMetadata(skipOffset + 1));
        consumer.commitSync(skipCommit);
        log.info(">>> [SKIP] Committed partition={} offset={} (skipped record at {})",
                skipPartition, skipOffset + 1, skipOffset);
    }


    // ===================================================================
    // RECORD PROCESSING
    // ===================================================================

    private void processRecord(ConsumerRecord<String, EventMessage> record) {
        totalProcessed.incrementAndGet();
        EventMessage msg = record.value();
        log.info("  ├─ partition={} offset={} seq={} id={}",
                record.partition(), record.offset(), msg.getSequenceNumber(), msg.getId());
        log.info("  └─ content: {}", msg.getContent());
    }


    // ===================================================================
    // STATUS — reads positions + committed offsets for the status endpoint
    // ===================================================================

    private Map<Integer, Long> currentPositionMap() {
        // JUNIOR NOTE: consumer.position(tp) is safe to call on the poll thread between poll() calls.
        Map<Integer, Long> positions = new LinkedHashMap<>();
        for (TopicPartition tp : consumer.assignment()) {
            try {
                positions.put(tp.partition(), consumer.position(tp));
            } catch (Exception e) {
                positions.put(tp.partition(), -1L);
            }
        }
        return positions;
    }

    private Map<Integer, Long> committedOffsetMap() {
        Map<Integer, Long> committed = new LinkedHashMap<>();
        Set<TopicPartition> assignment = consumer.assignment();
        if (assignment.isEmpty()) return committed;

        Map<TopicPartition, OffsetAndMetadata> committedMeta = consumer.committed(assignment);
        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : committedMeta.entrySet()) {
            committed.put(e.getKey().partition(),
                    e.getValue() != null ? e.getValue().offset() : 0L);
        }
        return committed;
    }


    // ===================================================================
    // PUBLIC CONTROL API — called by HTTP thread, sets flags only
    // ===================================================================

    public void pause()  { paused.set(true);  log.info(">>> [CONTROL] Pause signal set"); }
    public void resume() { paused.set(false); log.info(">>> [CONTROL] Resume signal set"); }

    public void seekToBeginning() {
        pendingSeek = SeekCommand.BEGINNING;
        log.info(">>> [CONTROL] SeekToBeginning signal set");
    }

    public void seekToEnd() {
        pendingSeek = SeekCommand.END;
        log.info(">>> [CONTROL] SeekToEnd signal set");
    }

    public void seekToOffset(int partition, long offset) {
        seekPartition = partition;
        seekOffset    = offset;
        pendingSeek   = SeekCommand.SPECIFIC;
        log.info(">>> [CONTROL] SeekToOffset signal set - partition={} offset={}", partition, offset);
    }

    public void skipOffset(int partition, long offset) {
        skipPartition = partition;
        skipOffset    = offset;
        skipRequested.set(true);
        log.info(">>> [CONTROL] Skip signal set - partition={} offset={}", partition, offset);
    }

    public OffsetStatus getStatus() {
        String state;
        if (!running.get())   state = "STOPPED";
        else if (paused.get()) state = "PAUSED";
        else                   state = "RUNNING";

        // JUNIOR NOTE: Reading position/committed from a different thread than the poll thread
        // is technically unsafe. For a demo/status endpoint this is acceptable — worst case
        // we get a slightly stale snapshot. In production, you'd route this through the
        // same flag mechanism (signal the poll thread to capture and expose state).
        Map<Integer, Long> positions  = new LinkedHashMap<>();
        Map<Integer, Long> committed  = new LinkedHashMap<>();
        try {
            positions = currentPositionMap();
            committed = committedOffsetMap();
        } catch (Exception e) {
            log.debug("Status read race - consumer may be between assignments: {}", e.getMessage());
        }

        return OffsetStatus.builder()
                .consumerState(state)
                .totalProcessed(totalProcessed.get())
                .totalCommitted(totalCommitted.get())
                .currentPositions(positions)
                .committedOffsets(committed)
                .checkedAt(LocalDateTime.now())
                .build();
    }

}
