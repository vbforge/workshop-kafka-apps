package com.vbforge.case06.service;

import com.vbforge.case06.model.ConsumerStatus;
import com.vbforge.case06.model.WorkMessage;
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

// JUNIOR NOTE: This is THE central lesson of case-06.
//
// In case-05, @KafkaListener hid the poll loop inside Spring's container.
// Here, we OWN the loop. Every line you see is what Spring was doing for you automatically.
//
// The manual poll pattern gives you:
//   1. FULL CONTROL over commit timing — commit only after your downstream work succeeds
//   2. PAUSE / RESUME — stop consuming partitions without leaving the group
//      (useful when a downstream DB is overloaded — pause Kafka, let it catch up, resume)
//   3. SEEK — rewind or skip offsets at runtime
//      (useful for replaying a bad batch, or skipping a poison pill)
//   4. VISIBILITY — you see exactly what the poll loop looks like
//
// Trade-off vs @KafkaListener:
//   Manual poll = more code, more responsibility, more power
//   @KafkaListener = less code, less control, Spring handles errors/retries/metrics for you
//
// When to use manual poll in production:
//   - When your processing has external side effects that must succeed before commit
//     (write to DB, call external API, produce to another topic — then commit)
//   - When you need fine-grained pause/resume control (backpressure handling)
//   - When you need to seek to specific offsets at startup (custom checkpoint management)
//
// Thread model:
//   KafkaConsumer is NOT thread-safe. All calls (poll, commit, pause, resume, seek)
//   MUST happen on the same thread. We run the entire loop on a dedicated single thread
//   (pollExecutor). The pause/resume/seek commands from HTTP threads use AtomicBoolean
//   flags as signals — the poll thread reads them and acts. Never call KafkaConsumer
//   methods from multiple threads directly.

@Service
@Slf4j
public class ManualPollConsumerService {

    private final Map<String, Object> consumerProperties;

    @Value("${kafka.topic.name}")
    private String topic;

    @Value("${kafka.consumer.poll-timeout-ms}")
    private long pollTimeoutMs;

    // ── Loop control flags (read by poll thread, written by HTTP threads) ──
    // AtomicBoolean for visibility + atomicity across threads without synchronized.
    private final AtomicBoolean running  = new AtomicBoolean(false);
    private final AtomicBoolean paused   = new AtomicBoolean(false);
    private final AtomicBoolean seekRequested = new AtomicBoolean(false);

    // ── Counters ──
    private final AtomicLong totalProcessed = new AtomicLong(0);
    private final AtomicLong totalCommitted  = new AtomicLong(0);

    // ── Seek target (set by seekToOffset(), read by poll thread) ──
    private volatile long seekTargetOffset = 0;
    private volatile int  seekTargetPartition = 0;

    // ── Single dedicated thread for the poll loop ──
    // JUNIOR NOTE: Single-thread executor = exactly one thread, reused across tasks.
    // We submit the poll loop as one long-running task to this executor.
    // This guarantees all KafkaConsumer calls happen on the same thread.
    private final ExecutorService pollExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "manual-poll-thread");
        t.setDaemon(true); // don't block JVM shutdown
        return t;
    });

    // KafkaConsumer is created and owned exclusively by the poll thread.
    // It's volatile so the @PreDestroy shutdown can read it from the main thread
    // to call wakeup() — the ONE KafkaConsumer method that is thread-safe.
    private volatile KafkaConsumer<String, WorkMessage> consumer;

    public ManualPollConsumerService(
            @Qualifier("consumerProperties") Map<String, Object> consumerProperties) {
        this.consumerProperties = consumerProperties;
    }


    // ===================================================================
    // LIFECYCLE — start on app startup, stop on shutdown
    // ===================================================================

    @PostConstruct
    public void start() {
        running.set(true);
        pollExecutor.submit(this::pollLoop);
        log.info(">>> [MANUAL-POLL] Consumer loop submitted to poll thread");
    }

    @PreDestroy
    public void stop() {
        log.info(">>> [MANUAL-POLL] Shutdown initiated");
        running.set(false);
        // JUNIOR NOTE: wakeup() is the ONLY KafkaConsumer method that is thread-safe.
        // It causes the next poll() call to throw a WakeupException, which we catch
        // in the loop to break out cleanly. Never call close() from another thread.
        if (consumer != null) {
            consumer.wakeup();
        }
        pollExecutor.shutdown();
        try {
            if (!pollExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                pollExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            pollExecutor.shutdownNow();
        }
        log.info(">>> [MANUAL-POLL] Consumer loop stopped");
    }


    // ===================================================================
    // THE POLL LOOP — runs entirely on the poll thread
    // ===================================================================

    private void pollLoop() {
        // JUNIOR NOTE: KafkaConsumer is created here, on the poll thread.
        // It lives for the lifetime of this loop and is closed in the finally block.
        consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(List.of(topic));
        log.info(">>> [MANUAL-POLL] Subscribed to topic: {}", topic);

        try {
            while (running.get()) {

                // ── Handle pause/resume ──
                // JUNIOR NOTE: pause() and resume() act on TopicPartitions.
                // paused partitions are still assigned to this consumer — they participate
                // in heartbeats and keep the consumer in the group.
                // But poll() returns ZERO records from paused partitions.
                // This is the key difference from unsubscribing: paused = "I'm in the group
                // but not consuming right now." Unsubscribed = "I left the group."
                handlePauseResume();

                // ── Handle seek ──
                if (seekRequested.compareAndSet(true, false)) {
                    TopicPartition tp = new TopicPartition(topic, seekTargetPartition);
                    consumer.seek(tp, seekTargetOffset);
                    log.info(">>> [MANUAL-POLL] Seeked partition={} to offset={}",
                            seekTargetPartition, seekTargetOffset);
                }

                // ── Poll ──
                // JUNIOR NOTE: poll(Duration) is the core of the consumer.
                // It does several things in one call:
                //   1. Sends heartbeats to the group coordinator (keeps us in the group)
                //   2. Fetches records from assigned partitions
                //   3. Blocks up to pollTimeoutMs if no records are available
                // If the broker has records ready, it returns immediately with a batch.
                ConsumerRecords<String, WorkMessage> records =
                        consumer.poll(Duration.ofMillis(pollTimeoutMs));

                if (records.isEmpty()) {
                    log.debug(">>> [MANUAL-POLL] Poll returned empty batch — waiting for messages");
                    continue;
                }

                log.info(">>> [MANUAL-POLL] Poll returned {} records", records.count());

                // ── Process each record ──
                // JUNIOR NOTE: We build a per-partition offset map as we process.
                // We commit partition-by-partition after processing all records in that
                // partition, rather than doing one bulk commit at the end.
                // This gives us per-record control: if processing fails mid-batch,
                // we only commit the partitions we fully processed.
                Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();

                for (ConsumerRecord<String, WorkMessage> record : records) {
                    processRecord(record);

                    // JUNIOR NOTE: We commit offset N+1, not N.
                    // Kafka's committed offset means "the next record to fetch starts here."
                    // So after processing offset 5, we commit offset 6 — meaning on restart,
                    // we'll pick up from 6, not re-process 5.
                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    offsetsToCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));
                }

                // ── Manual commit ──
                // JUNIOR NOTE: commitSync() vs commitAsync():
                //   commitSync()  → blocks until broker confirms the commit. Slower but safe.
                //                   If the commit fails, it throws — you know it failed.
                //   commitAsync() → non-blocking, takes a callback for failure.
                //                   Faster but you must handle failures in the callback.
                //
                // For simplicity we use commitSync() here. In high-throughput scenarios
                // you'd use commitAsync() during the loop and commitSync() only on shutdown.
                consumer.commitSync(offsetsToCommit);
                totalCommitted.addAndGet(records.count());
                log.info(">>> [MANUAL-POLL] Committed offsets for {} records", records.count());
            }

        } catch (org.apache.kafka.common.errors.WakeupException e) {
            // JUNIOR NOTE: WakeupException is the expected signal from stop() → wakeup().
            // It's NOT an error — it's our clean shutdown mechanism.
            // Only log it as info, not error. Do NOT re-throw.
            if (running.get()) {
                // If running is still true, wakeup() was called unexpectedly — log as error.
                log.error(">>> [MANUAL-POLL] Unexpected WakeupException while loop was running", e);
            } else {
                log.info(">>> [MANUAL-POLL] WakeupException received — shutting down cleanly");
            }
        } finally {
            // JUNIOR NOTE: Always close the consumer in the finally block on the poll thread.
            // close() triggers a group rebalance (graceful leave) and flushes any pending commits.
            // Calling close() from a different thread is NOT safe — that's why we use wakeup()
            // as the signal and close() here in the finally on the poll thread.
            consumer.close();
            log.info(">>> [MANUAL-POLL] KafkaConsumer closed");
        }
    }


    // ===================================================================
    // RECORD PROCESSING
    // ===================================================================

    private void processRecord(ConsumerRecord<String, WorkMessage> record) {
        totalProcessed.incrementAndGet();
        WorkMessage msg = record.value();

        log.info("  ├─ partition={} offset={} key={}",
                record.partition(), record.offset(), record.key());
        log.info("  ├─ ID:      {}", msg.getId());
        log.info("  ├─ Content: {}", msg.getContent());
        log.info("  └─ Total processed: {}", totalProcessed.get());
    }


    // ===================================================================
    // PAUSE / RESUME
    // ===================================================================

    private void handlePauseResume() {
        Set<TopicPartition> assigned = consumer.assignment();
        if (assigned.isEmpty()) return;

        if (paused.get()) {
            // Only pause partitions that aren't already paused
            Set<TopicPartition> currentlyPaused = consumer.paused();
            Set<TopicPartition> toPause = new HashSet<>(assigned);
            toPause.removeAll(currentlyPaused);
            if (!toPause.isEmpty()) {
                consumer.pause(toPause);
                log.info(">>> [MANUAL-POLL] Paused partitions: {}", toPause);
            }
        } else {
            // Resume any paused partitions
            Set<TopicPartition> currentlyPaused = consumer.paused();
            if (!currentlyPaused.isEmpty()) {
                consumer.resume(currentlyPaused);
                log.info(">>> [MANUAL-POLL] Resumed partitions: {}", currentlyPaused);
            }
        }
    }


    // ===================================================================
    // PUBLIC CONTROL API — called by HTTP endpoints (different thread!)
    // ===================================================================

    // JUNIOR NOTE: These methods are called by the HTTP request thread.
    // They NEVER touch the KafkaConsumer directly — that would violate thread safety.
    // They only set AtomicBoolean flags. The poll thread reads those flags on the
    // next loop iteration and calls the actual KafkaConsumer methods.
    // This is the correct cross-thread signalling pattern for manual poll consumers.

    public void pause() {
        paused.set(true);
        log.info(">>> [CONTROL] Pause signal set — will take effect on next poll iteration");
    }

    public void resume() {
        paused.set(false);
        log.info(">>> [CONTROL] Resume signal set — will take effect on next poll iteration");
    }

    public void seekToOffset(int partition, long offset) {
        seekTargetPartition = partition;
        seekTargetOffset = offset;
        seekRequested.set(true);
        log.info(">>> [CONTROL] Seek signal set — partition={} offset={}", partition, offset);
    }

    public ConsumerStatus getStatus() {
        String state;
        if (!running.get()) state = "STOPPED";
        else if (paused.get()) state = "PAUSED";
        else state = "RUNNING";

        return ConsumerStatus.builder()
                .running(running.get())
                .paused(paused.get())
                .totalProcessed(totalProcessed.get())
                .totalCommitted(totalCommitted.get())
                .currentState(state)
                .checkedAt(LocalDateTime.now())
                .build();
    }

}



























