# Case 06: Q&A Document

## 🎓 Theory Section

### Core Concepts Covered

| Concept | Description |
|---------|-------------|
| **`KafkaConsumer.poll(Duration)`** | Fetches records AND sends heartbeats — must be called regularly |
| **`commitSync(offsets)`** | Blocking commit of specific offsets — you choose exactly what and when |
| **`commitAsync(offsets, callback)`** | Non-blocking commit — faster but requires failure handling in callback |
| **`pause(partitions)`** | Stop fetching from specific partitions while staying in the group |
| **`resume(partitions)`** | Re-enable fetching from paused partitions |
| **`seek(partition, offset)`** | Move the consumer's read position to any offset on any partition |
| **`WakeupException`** | Thread-safe signal to interrupt a blocking `poll()` call |
| **`MAX_POLL_INTERVAL_MS`** | Deadline between `poll()` calls — exceed it and Kafka removes you from the group |

### Manual Poll vs @KafkaListener

| Dimension | `@KafkaListener` (case-05) | Manual Poll (case-06) |
|-----------|---------------------------|----------------------|
| Poll loop | Spring owns it | You own it |
| Commit | Spring commits via AckMode | You call `commitSync()` / `commitAsync()` |
| Pause/Resume | Via container API | Via `KafkaConsumer.pause()` directly |
| Error handling | Spring intercepts, retries | You handle every exception |
| Seek | Via container listener | Via `KafkaConsumer.seek()` directly |
| Boilerplate | Minimal | Significant |
| Control | Limited | Total |

---

## 📝 Interview Q&A

### Q1: What does `KafkaConsumer.poll()` actually do beyond just fetching records?

**Answer:**

`poll()` is deceptively multi-purpose. In a single call it does:

**Heartbeats**: Sends a heartbeat to the group coordinator broker. The coordinator tracks whether consumers are alive. If `session.timeout.ms` (default 10s) passes without a heartbeat, the coordinator declares the consumer dead and triggers a rebalance. By calling `poll()` regularly, your consumer implicitly keeps its membership in the group alive.

**Partition assignment processing**: If a rebalance happened since the last `poll()`, this call processes the new assignment — calling any `ConsumerRebalanceListener` callbacks and updating the internal partition assignment state.

**Fetch execution**: Issues fetch requests to the brokers for the assigned partitions and returns up to `max.poll.records` records.

**Blocking**: If no records are available, blocks for up to the `Duration` argument before returning an empty `ConsumerRecords`. This allows the thread to sleep rather than spin.

The critical implication: if your processing between two `poll()` calls takes longer than `max.poll.interval.ms`, Kafka considers you stuck, removes you from the group, and triggers a rebalance. The other consumers will then pick up your partitions. This is the most common pitfall of manual poll — slow processing between polls.

---

### Q2: Why is `KafkaConsumer` not thread-safe, and what does that mean in practice?

**Answer:**

`KafkaConsumer` maintains a lot of internal state: the fetch buffer, the offset tracker, the assignment map, the heartbeat timer, the coordinator connection. None of this state is protected by locks — the class was explicitly designed as single-threaded for performance.

If you call `poll()` from thread A while thread B calls `commitSync()`, you can corrupt the internal offset state, cause `ConcurrentModificationException`, or produce incorrect commits — silently wrong, not just failing.

In practice this means: **create the `KafkaConsumer` on its dedicated thread and never call any of its methods from any other thread**, with exactly one exception — `wakeup()`. The Kafka client documents `wakeup()` as explicitly thread-safe because its only job is to set an atomic flag that causes the next `poll()` to throw `WakeupException`.

The pattern we follow in this case:
- Poll thread: creates consumer, runs the loop, calls `poll()`, `commitSync()`, `pause()`, `resume()`, `seek()`, `close()`
- HTTP threads: set `AtomicBoolean` flags only
- Poll thread: reads those flags on each loop iteration and acts on them

This flag-based indirection is the standard pattern for controlling a manual poll consumer from outside its thread.

---

### Q3: What is the difference between `commitSync()` and `commitAsync()`, and when do you use each?

**Answer:**

`commitSync(offsets)` blocks the calling thread until the broker confirms the commit. If the commit fails (network error, coordinator unavailability), it throws an exception. You know immediately if a commit failed, and you can decide: retry, log, or crash. Safe but adds latency — each batch pays a broker round-trip for the commit.

`commitAsync(offsets, callback)` sends the commit request and returns immediately. The callback fires later (on the poll thread, during a future `poll()` call) with either success or a `Exception`. Faster overall throughput because the commit doesn't block processing of the next batch. But: if the callback fires with a failure, you may have already processed and "forgotten" the next batch. Retrying stale commits can cause out-of-order commits (commit offset 10 after already committing offset 20).

**Production pattern**: use `commitAsync()` during normal operation for throughput, and `commitSync()` on shutdown to ensure the last batch's offsets are flushed before the consumer closes. The hybrid loop looks like:

```java
try {
    while (running) {
        records = consumer.poll(timeout);
        process(records);
        consumer.commitAsync(offsets, (o, e) -> { if (e != null) log.error(...); });
    }
} finally {
    consumer.commitSync(); // flush last offsets on shutdown
    consumer.close();
}
```

---

### Q4: What is the difference between `pause()` and unsubscribing from a partition?

**Answer:**

`pause(partitions)` temporarily stops `poll()` from returning records for those partitions, but the consumer **stays assigned to them**. It continues sending heartbeats, participates in the group, and will eventually resume processing. Other consumers in the group do NOT receive those partitions — they remain exclusively assigned to the paused consumer.

Unsubscribing (calling `unsubscribe()` or `subscribe()` with a different list) removes the consumer from the group for those topics entirely. A rebalance is triggered and other consumers absorb the partitions.

The practical difference:

`pause()` is **backpressure** — "I'm overwhelmed right now, give me a moment, I'll be back." The partition stays mine. No rebalance. No data re-routing. Used when a downstream service (DB, external API) is temporarily overloaded and you need to stop ingestion without disrupting the group topology.

Unsubscribing is **topology change** — "I'm permanently done with this partition/topic." Triggers rebalance. Other consumers pick up the work.

For backpressure handling in production, `pause()` is almost always the right tool. It's lightweight, reversible, and doesn't destabilize the group.

---

### Q5: When we commit offsets, why do we commit `record.offset() + 1` instead of `record.offset()`?

**Answer:**

Kafka's committed offset semantics define the committed value as **the next offset to fetch**, not the last processed offset.

If you processed offset 5 and commit 5, Kafka interprets that as: "the next time this consumer starts, fetch from offset 5 again." You'd reprocess offset 5 every restart.

By committing offset 6 (= `record.offset() + 1`), you tell Kafka: "I've handled everything up to and including offset 5; start me at 6 next time." Correct at-least-once semantics — no unnecessary reprocessing on normal restart.

This is a common gotcha. The raw `KafkaConsumer` API gives you full control, which means it also gives you the opportunity to commit the wrong value. The `OffsetAndMetadata` object wraps the offset + optional metadata string (which you can use to store a timestamp or processing ID alongside the offset for debugging).

---

### Q6: What is `WakeupException` and why is it the correct shutdown mechanism?

**Answer:**

`poll()` is a blocking call — it holds the poll thread for up to `pollTimeoutMs`. If you want to stop the loop from another thread (HTTP handler, Spring's `@PreDestroy`), you can't just set a flag and wait — the poll thread is blocked inside `poll()` and won't check the flag until the timeout expires (up to 3 seconds in our case).

`wakeup()` solves this. It's the **only thread-safe method on `KafkaConsumer`**. When called from any thread, it causes the **currently blocking `poll()` (or the next one)** to immediately throw `WakeupException`. The poll thread's catch block receives it, checks `running.get()` to confirm it was intentional, and exits the loop cleanly.

The shutdown sequence:
1. `@PreDestroy`: `running.set(false)` → `consumer.wakeup()`
2. Poll thread: `poll()` throws `WakeupException`
3. Poll thread: catch block sees `running == false` → logs clean shutdown
4. Poll thread: `finally` block → `consumer.close()`

Why not just call `consumer.close()` from `@PreDestroy`? Because `close()` is not thread-safe. Calling it concurrently with `poll()` would corrupt internal state. `wakeup()` → `poll()` throws → `close()` on the poll thread is the correct, documented pattern.

---

### Q7: What is `MAX_POLL_INTERVAL_MS` and what happens when you exceed it?

**Answer:**

`max.poll.interval.ms` (default 5 minutes) is the maximum time allowed between two consecutive `poll()` calls. If the gap exceeds this, Kafka concludes the consumer is "stuck" — unable to make progress — and forcibly removes it from the group, triggering a rebalance.

This exists because Kafka needs a way to detect consumers that are alive (heartbeats) but not actually consuming (e.g. stuck in a deadlock, or processing a single record for 10 minutes). Before this config existed (Kafka < 0.10.1), the only liveness signal was the heartbeat, and a consumer could hold a partition indefinitely while blocked on a slow database call.

The production gotcha with manual poll: if your processing logic between `poll()` calls is slow (database batch writes, external API calls, complex transforms), you must ensure the total time stays under `max.poll.interval.ms`. Options:
- Process records faster (optimise the logic)
- Reduce `max.poll.records` so each batch is smaller and faster to process
- Increase `max.poll.interval.ms` if your processing is legitimately slow
- Move slow work to a separate thread and `poll()` on a schedule regardless (advanced pattern — requires careful offset management)

---

### Q8: What are two production scenarios where manual poll is the right choice over `@KafkaListener`?

**Answer:**

**Scenario 1 — Transactional exactly-once processing**

You're consuming events and writing to a database. You want the guarantee: "if the write succeeds, commit the offset; if the write fails, don't commit." With `@KafkaListener` + `AckMode`, an exception prevents commit — but you can only communicate "process or don't process," not arbitrary business logic outcomes.

With manual poll, you do:
```
poll() → begin DB transaction → process records → commit DB transaction → commitSync() offsets
```
If the DB transaction fails, no Kafka commit. If Kafka commit fails after the DB commit, you get a duplicate on replay — which your DB upsert handles. This is the closest you get to exactly-once without Kafka Transactions (case-13).

**Scenario 2 — Dynamic pause/resume for backpressure**

Your consumer writes to an Elasticsearch cluster. Under normal load it keeps up. Under peak load, Elasticsearch starts returning 429s (rate limit). With `@KafkaListener` you'd be throwing exceptions and triggering Spring's retry/error handler — messy.

With manual poll, you detect the 429, call `consumer.pause(assignedPartitions)`, back off for 10 seconds, call `consumer.resume(assignedPartitions)`, and continue. The partition stays yours throughout. No rebalance. No error handler magic. Clean, explicit backpressure that you control entirely.

---

## 📊 Quick Reference Card

| Term | Definition |
|------|------------|
| `poll(Duration)` | Fetch records + send heartbeat + process rebalance events |
| `commitSync(offsets)` | Blocking offset commit — throws on failure |
| `commitAsync(offsets, cb)` | Non-blocking commit — failure via callback |
| `pause(partitions)` | Stop fetching from partitions, stay assigned in the group |
| `resume(partitions)` | Re-enable fetching from paused partitions |
| `seek(tp, offset)` | Move read position to specific offset on a partition |
| `wakeup()` | Thread-safe interrupt — causes next `poll()` to throw `WakeupException` |
| `MAX_POLL_INTERVAL_MS` | Max gap between `poll()` calls — exceed it and you're removed from the group |
| `offset + 1` | The value to commit — means "start from here next time" |

---

## ✅ Self-Assessment

After studying this Q&A, you should be able to:

- Describe everything `poll()` does beyond just fetching records
- Explain why `KafkaConsumer` is not thread-safe and how the flag pattern solves it
- Compare `commitSync()` and `commitAsync()` and describe the hybrid production pattern
- Explain `pause()` vs unsubscribe and when to use each
- Explain why you commit `offset + 1` and not `offset`
- Describe `WakeupException` and the correct shutdown sequence for a manual poll loop
- Explain `MAX_POLL_INTERVAL_MS` and three ways to avoid exceeding it
- Give two production scenarios where manual poll is the right choice over `@KafkaListener`
