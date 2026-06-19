# Case 15: Q&A Document

## 🎓 Theory Section

### Core Concepts Covered

| Concept | Description |
|---------|-------------|
| **`setBatchListener(true)`** | Spring Kafka config that makes the listener receive a `List<ConsumerRecord>` instead of a single record |
| **`max.poll.records`** | Kafka consumer config — upper bound on records returned by one `poll()` call |
| **`max.poll.interval.ms`** | Maximum time Kafka waits between `poll()` calls before removing the consumer from the group |
| **Batch commit** | `ack.acknowledge()` called once after the loop — commits offsets for all records in the batch |
| **At-least-once** | Batch not committed → entire batch redelivered on restart |
| **Throughput vs. failure window** | Larger batch = more throughput, larger re-processing window on crash |

### The Batch Processing Mental Model

```
Topic partition:
  offset:  0   1   2   3 ... 49  50  51 ... 99

poll() with max.poll.records=50:
  First call  → returns records [0..49]   (up to 50)
  Process all → ack.acknowledge()
  → committed offset = 50

  Second call → returns records [50..99]  (up to 50)
  Process all → ack.acknowledge()
  → committed offset = 100

  Third call  → empty (no more records)  → polls again
```

---

## 📝 Interview Q&A

### Q1: What is `max.poll.records` and what does it control?

**Answer:**

`max.poll.records` is a Kafka consumer configuration property that limits the **maximum number of records returned by a single `poll()` call**. It does not affect how often `poll()` is called — only the batch size ceiling.

If the topic has fewer records available than `max.poll.records`, you get fewer records. If a partition has 200 records available and `max.poll.records=50`, you'll need at least 4 `poll()` calls to consume all of them (assuming single-threaded, single-partition case).

What it controls in practice:
- **Batch size**: directly bounds how many records arrive in one listener invocation when using `setBatchListener(true)`
- **Commit frequency**: each batch = one commit (if you're committing per-batch). Larger `max.poll.records` = fewer commits = less overhead
- **Max re-processing window**: if the app crashes, you replay at most `max.poll.records` records from the last uncommitted batch

What it does NOT control:
- The rate at which `poll()` is called — that's controlled by the container's internal poll loop
- Whether records are split across partitions in one batch — a batch may contain records from multiple partitions

The interaction with `max.poll.interval.ms` is important: if processing `max.poll.records` records takes longer than `max.poll.interval.ms`, Kafka removes you from the group. Set `max.poll.interval.ms` to at least 2-3x your p99 batch processing time.

---

### Q2: What is `max.poll.interval.ms` and what happens when you exceed it?

**Answer:**

`max.poll.interval.ms` (default: 300,000ms = 5 minutes) is the maximum time Kafka allows between consecutive `poll()` calls from the same consumer. It exists because Kafka uses the `poll()` heartbeat mechanism to detect dead consumers.

If your application fails to call `poll()` within `max.poll.interval.ms` — perhaps because processing the previous batch took too long — Kafka:
1. Considers the consumer dead (stuck in processing)
2. Removes it from the consumer group
3. Triggers a rebalance — other consumers take over the abandoned partitions
4. Your application eventually calls `poll()` again and gets a `RevokedException` or receives an empty result, then re-joins the group (triggering another rebalance)

This is called "poll starvation" — the consumer is alive but processing so slowly that Kafka thinks it's dead.

In batch mode the risk is: `max.poll.records=500` × 10ms per record = 5 seconds processing. If `max.poll.interval.ms` is 3 seconds, you'll consistently trigger rebalances. The fix is either:
1. Increase `max.poll.interval.ms`
2. Reduce `max.poll.records`
3. Parallelize processing within the batch (multiple threads, but then you must track which records succeeded)

Note: `max.poll.interval.ms` is different from the heartbeat mechanism (`heartbeat.interval.ms`, `session.timeout.ms`). The heartbeat is sent on a background thread; `max.poll.interval.ms` is checked on the poll thread. A stuck processing thread can satisfy heartbeats but still trigger the poll interval violation.

---

### Q3: How does `setBatchListener(true)` change the listener method signature and behaviour?

**Answer:**

Without `setBatchListener(true)` (the default), Spring Kafka's container calls your listener method once per record:
```java
@KafkaListener(...)
public void consume(ConsumerRecord<String, MyDto> record, Acknowledgment ack) {
    // called N times for N records
}
```

With `setBatchListener(true)`, Spring Kafka detects the `List<ConsumerRecord<...>>` parameter type and calls your method once per `poll()` batch:
```java
@KafkaListener(...)
public void consumeBatch(List<ConsumerRecord<String, MyDto>> records, Acknowledgment ack) {
    // called once per poll() — receives all records from that poll
}
```

The `List` size is bounded by `max.poll.records` and the actual number of available records. Spring Kafka checks the parameter type at registration time: if it's `List<ConsumerRecord>`, it enables batch delivery; if it's `ConsumerRecord`, it enables single-record delivery.

A common mistake: defining the parameter as `List<ConsumerRecord<String, MyDto>>` but forgetting `setBatchListener(true)`. Spring Kafka then tries to pass a `List` object where a `ConsumerRecord` is expected and throws a type mismatch at runtime.

The `Acknowledgment` parameter works the same in both modes, but its semantics change: in batch mode, one `ack.acknowledge()` commits offsets for all records in the batch.

---

### Q4: Why do you call `ack.acknowledge()` after the processing loop, not inside it?

**Answer:**

Calling `ack.acknowledge()` inside the loop would commit the offset after every record — effectively reverting to single-record commit semantics and losing all the throughput benefit of batching. You'd have `N` commit operations for `N` records, identical overhead to a non-batch listener.

Calling it once after the loop is the "all-or-nothing" approach:
- Process all records → commit once → 1 round-trip to broker
- This is the batch processing contract: the batch either succeeds as a whole, or fails as a whole

The failure consequence: if the app crashes after processing record 25 of 50 (but before `ack.acknowledge()`), all 50 records are redelivered on restart. This is at-least-once delivery. Records 0-24 will be reprocessed. Your downstream system must be idempotent — processing the same record twice must not cause duplicate side effects (duplicate DB inserts, double-counting metrics, etc.).

If you need partial commit (commit record by record), use a single-record listener with `AckMode.MANUAL_IMMEDIATE`. The batch listener is specifically for high-throughput scenarios where idempotency at the destination is guaranteed.

---

### Q5: What is the throughput benefit of batch processing in concrete terms?

**Answer:**

The primary overhead in Kafka consumption is **commit operations** — each `commitSync()` is a synchronous round-trip to the broker's `__consumer_offsets` topic. In a high-latency environment (even localhost: ~1ms), single-record commit at 1000 records/second means 1000 commit round-trips per second.

With `max.poll.records=100`:
- 1000 records processed → 10 batches → 10 commits
- Commit overhead drops to 1/100th of the single-record case

For I/O-bound processing (DB inserts, HTTP calls), the bigger gain is **bulk operations**:
- Single-record: `INSERT INTO orders VALUES (?)` × 1000 = 1000 DB round-trips
- Batch: `INSERT INTO orders VALUES (?), (?), ... (?)` × 1 = 1 DB round-trip

Combining Kafka batch consumption with bulk DB inserts is the standard pattern for high-throughput ingestion pipelines (event sourcing, audit logs, analytics). Throughput improvements of 10-100x are common.

The limit: if `max.poll.records` is so large that processing exceeds `max.poll.interval.ms`, you get rebalances, which destroy throughput. The optimal value is empirically tuned: measure p99 batch processing time, set `max.poll.interval.ms = 3 × p99`, then maximize `max.poll.records` within that constraint.

---

### Q6: Can a single batch contain records from multiple partitions?

**Answer:**

Yes. `max.poll.records` limits the total number of records across all assigned partitions in one `poll()` call. If a consumer is assigned 3 partitions and there are 20 records available on each, a `poll()` with `max.poll.records=50` might return 17 + 17 + 16 records from partitions 0, 1, and 2 respectively.

The exact distribution per partition in one `poll()` is implementation-defined — Kafka's consumer client distributes the `max.poll.records` budget across partitions proportionally to available records.

This has implications for selective batch commit: if you want to commit partition 0's records immediately without waiting for partition 1, the batch listener is the wrong tool. Use `AckMode.MANUAL` combined with `commitSync(Map<TopicPartition, OffsetAndMetadata>)` (case-08 style) to commit specific partitions independently.

The more common pattern with batch listeners: treat the batch as an atomic unit regardless of which partitions its records came from. Process all, commit all. If you need partition-level granularity, step down to single-record mode or the raw `KafkaConsumer` poll loop.

---

### Q7: What is `AckMode.BATCH` vs `AckMode.MANUAL_IMMEDIATE` in Spring Kafka?

**Answer:**

These are two different ack modes for Spring Kafka batch listeners:

`AckMode.BATCH` — Spring commits offsets automatically at the end of each batch listener invocation. You don't call `ack.acknowledge()` at all (or even declare the `Acknowledgment` parameter). Spring handles it: listener method returns → Spring commits. Simple and safe for the common case.

`AckMode.MANUAL_IMMEDIATE` — Spring does NOT commit automatically. You must call `ack.acknowledge()` explicitly. The commit happens immediately when you call it, not deferred to the next poll cycle.

When to use each:
- `BATCH`: production default for batch listeners when no special commit logic is needed. Less code, less risk of forgetting to commit.
- `MANUAL_IMMEDIATE`: when you need conditional commits — e.g., skip committing if downstream processing failed, or commit after persisting to a DB (to ensure the DB write and the offset commit are sequenced correctly). Also good for learning, since you explicitly see where commits happen.

There's also `AckMode.MANUAL` (without IMMEDIATE): `ack.acknowledge()` records the intent, but the actual commit happens on the next `poll()` call. Slightly higher throughput (batches multiple `acknowledge()` calls), but more complex timing. `MANUAL_IMMEDIATE` is simpler to reason about.

Case-15 uses `MANUAL_IMMEDIATE` deliberately so the code makes the commit visible and educational.

---

### Q8: How does batch processing relate to exactly-once delivery?

**Answer:**

Standard batch processing with manual commit gives **at-least-once** delivery: if the app crashes after processing but before committing, the batch is redelivered. This is a strong guarantee — nothing is lost — but duplicates are possible.

For **exactly-once** across a Kafka-to-Kafka pipeline (read batch → transform → produce results → commit), you need Kafka transactions (case-13). The pattern:

```java
// In a Kafka Streams or transactional producer context:
beginTransaction();
for (record : batch) {
    produce(transformedRecord, outputTopic);
}
commitTransaction();  // atomically commits producer offsets AND consumer offsets
```

Kafka transactions make the produce + consumer-offset-commit atomic. Either both happen or neither does. This eliminates duplicates on the output side.

For **exactly-once at the destination** (e.g., writing to a database), Kafka transactions don't help directly — they're Kafka-internal. You need **idempotent writes** at the destination: track a unique event ID per record, check before inserting, or use database upsert semantics. The combination of at-least-once Kafka delivery + idempotent destination writes = exactly-once end-to-end effect.

In practice: most batch consumers are at-least-once with idempotent destinations. Exactly-once Kafka transactions are reserved for Kafka Streams pipelines where the full processing graph is inside Kafka.

---

## 📊 Quick Reference Card

| Concept | Key Setting / API |
|---------|-------------------|
| Enable batch mode | `factory.setBatchListener(true)` |
| Batch size ceiling | `ConsumerConfig.MAX_POLL_RECORDS_CONFIG` |
| Batch listener signature | `consume(List<ConsumerRecord<K,V>> records, Acknowledgment ack)` |
| Commit batch | `ack.acknowledge()` once after the loop |
| Auto-commit batch | `AckMode.BATCH` — no `ack.acknowledge()` needed |
| Manual commit batch | `AckMode.MANUAL_IMMEDIATE` — explicit `ack.acknowledge()` |
| Prevent rebalance | `max.poll.interval.ms` > batch processing time |

---

## ✅ Self-Assessment

After studying this Q&A, you should be able to:

- Explain what `max.poll.records` controls and what it does NOT control
- Describe what happens when `max.poll.interval.ms` is exceeded
- Explain the `setBatchListener(true)` effect on method signature and call frequency
- Articulate why `ack.acknowledge()` belongs outside the processing loop
- Quantify the throughput benefit of batch commit over single-record commit
- Explain whether a batch can contain records from multiple partitions
- Distinguish `AckMode.BATCH` from `AckMode.MANUAL_IMMEDIATE`
- Describe what it takes to achieve exactly-once delivery in a batch scenario
