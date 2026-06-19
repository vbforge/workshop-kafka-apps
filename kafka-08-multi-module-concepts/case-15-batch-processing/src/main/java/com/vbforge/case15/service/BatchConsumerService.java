package com.vbforge.case15.service;

import com.vbforge.case15.model.BatchStatus;
import com.vbforge.case15.model.EventMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

// JUNIOR NOTE: This is the core class of case-15 — a BATCH listener.
//
// The single-record listener (case-05, case-08, case-09...) looks like:
//   @KafkaListener
//   public void consume(ConsumerRecord<String, EventMessage> record, Acknowledgment ack)
//
// The batch listener (case-15) looks like:
//   @KafkaListener
//   public void consume(List<ConsumerRecord<String, EventMessage>> records, Acknowledgment ack)
//
// The entire difference is List<ConsumerRecord<...>> vs ConsumerRecord<...>.
// Spring Kafka detects the List parameter type (when setBatchListener(true) is configured)
// and passes all records from one poll() call as a single List.
//
// Why batch matters for throughput:
//   Single-record: 1 record → process → commit → 1 record → process → commit ...
//     N commits per N records → high overhead
//
//   Batch: [50 records] → process all → commit once
//     1 commit per N records → lower overhead, higher throughput
//
// The tradeoff: if processing fails halfway through a batch (record 26 of 50),
// you can either fail the whole batch (all 50 re-processed) or
// track which records succeeded and commit selectively.
// This case demonstrates the selective commit approach.

@Service
@Slf4j
public class BatchConsumerService {

    // ── Counters ──
    private final AtomicLong totalReceived  = new AtomicLong(0);
    private final AtomicLong totalCommitted = new AtomicLong(0);
    private final AtomicLong batchCount     = new AtomicLong(0);

    // ── Recent batch history (last 10) ──
    private final List<BatchStatus.BatchSummary> recentBatches =
            Collections.synchronizedList(new ArrayList<>());
    private static final int MAX_RECENT = 10;


    // ===================================================================
    // BATCH LISTENER
    // ===================================================================

    @KafkaListener(
            topics = "${kafka.topic.events}",
            groupId = "${kafka.consumer.groupID}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeBatch(
            List<ConsumerRecord<String, EventMessage>> records,
            Acknowledgment ack
    ) {
        if (records.isEmpty()) {
            return;
        }

        long batchNumber = batchCount.incrementAndGet();
        long startMs = System.currentTimeMillis();

        log.info(">>> [BATCH #{}] Received {} records", batchNumber, records.size());
        totalReceived.addAndGet(records.size());

        // JUNIOR NOTE: Track the first and last record in the batch for the summary.
        // In a real system you might persist these as checkpoints.
        ConsumerRecord<String, EventMessage> first = records.get(0);
        ConsumerRecord<String, EventMessage> last  = records.get(records.size() - 1);

        // ── Process all records ──
        int processed = 0;
        for (ConsumerRecord<String, EventMessage> record : records) {
            processRecord(record, batchNumber);
            processed++;
        }

        // ── Commit after the entire batch succeeds ──
        // JUNIOR NOTE: This is the "commit once per batch" pattern.
        // We call ack.acknowledge() exactly ONCE after the loop — not inside the loop.
        // This means:
        //   - All offsets in the batch are committed in a single commit operation.
        //   - If the process crashes mid-loop, NONE of the records are committed.
        //     On restart, the whole batch is redelivered (at-least-once).
        //   - If the process completes the loop, ALL records are committed atomically.
        //
        // Compare to per-record ack (wrong for batch): calling ack.acknowledge() inside
        // the loop would commit after each record, losing the batch throughput benefit.
        ack.acknowledge();
        totalCommitted.addAndGet(processed);

        long processingMs = System.currentTimeMillis() - startMs;
        log.info(">>> [BATCH #{}] Committed {} records in {}ms | first={} last={}",
                batchNumber, processed, processingMs,
                formatOffset(first), formatOffset(last));

        // ── Store summary ──
        BatchStatus.BatchSummary summary = BatchStatus.BatchSummary.builder()
                .batchNumber((int) batchNumber)
                .size(records.size())
                .processingMs(processingMs)
                .firstOffset(formatOffset(first))
                .lastOffset(formatOffset(last))
                .completedAt(LocalDateTime.now())
                .build();

        recentBatches.add(summary);
        if (recentBatches.size() > MAX_RECENT) {
            recentBatches.remove(0);
        }
    }


    // ===================================================================
    // RECORD PROCESSING
    // ===================================================================

    private void processRecord(ConsumerRecord<String, EventMessage> record, long batchNumber) {
        // JUNIOR NOTE: In a real batch consumer this is where you'd:
        //   - Accumulate records into an in-memory list
        //   - Then bulk-insert to a DB at the end of the loop
        //   - Or call an external API in parallel using CompletableFuture
        //
        // The key insight: processing all records THEN committing once is
        // what makes batch processing efficient. The commit is the expensive operation
        // (round-trip to the broker). Doing it once per batch instead of once per record
        // can improve throughput by 10-100x depending on record volume.
        EventMessage msg = record.value();
        log.debug("  ├─ [BATCH #{}] partition={} offset={} seq={} content={}",
                batchNumber, record.partition(), record.offset(),
                msg.getSequenceNumber(), msg.getContent());
    }


    // ===================================================================
    // STATUS
    // ===================================================================

    public BatchStatus getStatus() {
        long received = totalReceived.get();
        long batches  = batchCount.get();

        return BatchStatus.builder()
                .totalReceived(received)
                .totalCommitted(totalCommitted.get())
                .batchCount(batches)
                .avgBatchSize(batches > 0 ? (double) received / batches : 0.0)
                .recentBatches(new ArrayList<>(recentBatches))
                .checkedAt(LocalDateTime.now())
                .build();
    }


    // ===================================================================
    // HELPERS
    // ===================================================================

    private String formatOffset(ConsumerRecord<?, ?> record) {
        return "partition=" + record.partition() + " offset=" + record.offset();
    }

}
