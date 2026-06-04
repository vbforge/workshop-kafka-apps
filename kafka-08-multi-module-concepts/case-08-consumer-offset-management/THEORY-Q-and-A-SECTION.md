# Case 08: Q&A Document

## 🎓 Theory Section

### Core Concepts Covered

| Concept | Description |
|---------|-------------|
| **`position(tp)`** | Next offset the consumer will fetch — advances after every `poll()`, NOT persisted |
| **`committed(partitions)`** | Last committed offset — persisted to `__consumer_offsets`, survives restart |
| **`seekToBeginning(partitions)`** | Reset position to offset 0 on all given partitions |
| **`seekToEnd(partitions)`** | Advance position to latest offset on all given partitions |
| **`seek(tp, offset)`** | Move position to a specific offset on one partition |
| **`auto.offset.reset`** | Determines starting position only when NO committed offset exists |
| **Poison pill** | A message that causes consumer processing to fail every time — must be handled explicitly |
| **Skip pattern** | `commitSync(badOffset + 1)` without processing — discards the bad record |

### The Offset Mental Model

```
Partition 0 log:
  offset:  0   1   2   3   4   5   6   7   8   9
  msg:    [A] [B] [C] [D] [E] [F] [G] [H] [I] [J]

After consuming offsets 0–4 and calling commitSync(offset=5):
  position(partition=0)  = 5    ← next poll starts here
  committed(partition=0) = 5    ← restart also starts here

After seekToBeginning() — no commitSync:
  position(partition=0)  = 0    ← next poll starts at A
  committed(partition=0) = 5    ← restart STILL starts at F (committed unchanged)

After seekToBeginning() + commitSync(offset=0):
  position(partition=0)  = 0
  committed(partition=0) = 0    ← NOW restart also starts at A (permanent rewind)
```

---

## 📝 Interview Q&A

### Q1: What is the difference between `consumer.position()` and `consumer.committed()`?

**Answer:**

`consumer.position(TopicPartition)` returns the offset of the **next record the consumer will fetch** in the next `poll()` call. It's a purely local, in-memory value maintained by the `KafkaConsumer` object. It advances automatically every time `poll()` returns records — no action required. It is NOT stored anywhere on the broker.

`consumer.committed(Set<TopicPartition>)` queries the broker for the **last committed offset** for each partition — the value stored in Kafka's `__consumer_offsets` internal topic. This is the offset the consumer will resume from if it restarts, crashes, or if a rebalance moves the partition to another consumer.

The gap between them is the "unacknowledged" window — records that have been polled and potentially processed, but not yet committed. If the consumer crashes in this gap, those records will be redelivered. This is the source of at-least-once delivery semantics.

Example after processing 10 records without committing:
- `position = 10` — the consumer internally knows it's read up to record 9
- `committed = 0` — the broker has no record of any progress
- Restart → consumer starts from 0, reprocesses all 10 records

---

### Q2: Describe all three values of `auto.offset.reset` and the exact condition each applies to.

**Answer:**

`auto.offset.reset` applies in exactly one situation: a consumer group has **no committed offset** for a partition. This happens when:
1. The group is brand new and has never consumed from this topic
2. The committed offset is out of range (the log segment containing it was deleted due to retention)

`earliest` → start from the lowest available offset in the partition (offset 0 if the log hasn't been cleaned, otherwise the earliest retained segment). This means: read all available history.

`latest` → start from the current end of the log. Only messages produced after this consumer starts will be seen. Historical messages are invisible to this group.

`none` → throw `NoOffsetForPartitionException` immediately. Do not guess. This is the "fail loudly" option — use it when you can't safely decide whether to replay history or start fresh. Forces an explicit operator decision.

Critical: once the group has committed at least one offset, `auto.offset.reset` is **completely ignored**. The consumer always resumes from its committed offset regardless of this setting. It's a one-time bootstrap policy, not an ongoing behaviour.

---

### Q3: `seekToBeginning()` rewinds the consumer to offset 0. Is that change permanent?

**Answer:**

No, not by itself. `seekToBeginning()` only changes the consumer's in-memory `position` — it does not write anything to `__consumer_offsets`. The change is local to the currently running consumer instance.

If the consumer restarts, crashes, or if a rebalance moves the partition to another consumer, the new consumer will read the last **committed** offset from `__consumer_offsets` — which hasn't changed. It will start from wherever the committed offset points, not from 0.

To make a rewind permanent:
```java
consumer.seekToBeginning(assignedPartitions);
// Then immediately commit offset 0 for each partition:
Map<TopicPartition, OffsetAndMetadata> rewindCommit = new HashMap<>();
for (TopicPartition tp : assignedPartitions) {
    rewindCommit.put(tp, new OffsetAndMetadata(0));
}
consumer.commitSync(rewindCommit);
```

Now `__consumer_offsets` records offset 0, and any restart will resume from 0.

This distinction trips up many developers: they think `seekToBeginning` resets their consumer permanently, then are confused when it starts from the middle after a restart. The seek only changes the live position; the commit is what persists.

---

### Q4: What is a "poison pill" message and how do you handle it without crashing the consumer?

**Answer:**

A poison pill is a message in the Kafka log that causes your consumer to fail every time it tries to process it — a deserialization failure, a null field that causes NPE, a business rule violation, a message from an incompatible schema version. The dangerous property: because the consumer commits after successful processing, the failing offset never advances. The consumer retries the same message forever, blocking all subsequent messages on that partition.

The handling options from least to most sophisticated:

**Skip pattern** (shown in case-08): detect the failure, call `commitSync` with the bad offset + 1 — effectively discarding the record and advancing past it. Fast, but the record is silently lost. Always log it with the full payload first.

**Dead Letter Topic** (case-11): before skipping, publish the bad record to a separate `*-dlt` topic with additional headers (original topic, partition, offset, exception). Commit past it in the main topic. The DLT record can be manually inspected, replayed after a fix, or fed to an alert system. This is the production standard.

**Deserializer-level handling**: configure `ErrorHandlingDeserializer` as the deserializer. When deserialization fails, it wraps the raw bytes in a `DeserializationException` and delivers it to your listener as a special error record. You handle it inline. This is the Spring Kafka way for `@KafkaListener`.

**`DefaultErrorHandler` with DLT** (case-12): Spring's global error handler that handles retry logic, backoff, and DLT publishing automatically. You configure it once and all listeners benefit.

---

### Q5: When should you use `seekToEnd()` and what is the risk?

**Answer:**

`seekToEnd()` jumps the consumer's position to the latest offset on each partition — effectively saying "treat all existing messages as already processed." Any messages currently in the topic that the consumer hadn't processed yet are permanently skipped (for this group).

Legitimate use cases:
- **Intentional lag clearing**: after a major outage, you have 48 hours of backlog. Processing it sequentially would take days. If the business decides "old events are no longer actionable, start fresh," `seekToEnd` + `commitSync` clears the slate immediately.
- **Dev/test environment reset**: skip stale test data without deleting the topic.
- **New consumer group on a high-volume topic**: if you only care about events from now forward, `seekToEnd` + `commitSync` at group initialization avoids replaying history.

The risk: **permanent data loss for this group**. The skipped messages will never be processed by this group. If the messages contained business-critical events (payments received, orders placed), skipping them means those events go unprocessed. Always be explicit and deliberate. Always log the lag before and after. Consider publishing the skipped messages to a holding topic before committing past them.

---

### Q6: How do you commit a specific set of offsets per partition (not all at once)?

**Answer:**

`commitSync(Map<TopicPartition, OffsetAndMetadata> offsets)` accepts a map so you can commit exactly the offsets you want:

```java
Map<TopicPartition, OffsetAndMetadata> toCommit = new HashMap<>();
toCommit.put(new TopicPartition("my-topic", 0), new OffsetAndMetadata(42));
toCommit.put(new TopicPartition("my-topic", 2), new OffsetAndMetadata(17));
// partition 1 intentionally not committed — we'll process it later
consumer.commitSync(toCommit);
```

This is useful when you're processing partitions in parallel and partition 0's batch finished before partition 1. You commit partition 0's progress immediately without waiting for partition 1.

`OffsetAndMetadata` also accepts an optional `metadata` string (up to 4KB). You can store arbitrary information alongside the offset — a timestamp, a processing ID, a checksum of the last record. This metadata is returned when you call `consumer.committed()`, giving you a simple way to persist extra state in Kafka without a separate database.

The no-argument `commitSync()` (no map) commits all current positions for all assigned partitions in one call. This is the most common form — it's what we use after each batch in the normal processing path.

---

### Q7: What is `__consumer_offsets` and what happens if it becomes unavailable?

**Answer:**

`__consumer_offsets` is an internal Kafka topic (50 partitions by default) where all consumer group committed offsets are stored. It's a regular compacted Kafka topic — the broker appends committed offset records to it, and compaction ensures the latest value per `(group, topic, partition)` key is retained.

Every `commitSync()` and `commitAsync()` call your consumer makes is actually a produce operation to `__consumer_offsets`. Every `consumer.committed()` call is a fetch from it.

If `__consumer_offsets` becomes unavailable (broker crash during offset commit, network partition to the coordinator):
- `commitSync()` throws an exception — your consumer knows the commit failed and can retry or fail
- `commitAsync()` fires the error callback with a `CommitFailedException`
- The consumer's group coordinator connection is lost — heartbeats fail — eventually triggers a rebalance

In practice, `__consumer_offsets` has replication factor 3 in production clusters (`offsets.topic.replication.factor=3`), making it highly available. In our single-node Docker setup, replication factor is 1 — a broker restart wipes committed offsets if the log isn't flushed. This is why persistent Docker volumes matter even for development.

---

### Q8: What is the relationship between `max.poll.records` and offset management?

**Answer:**

`max.poll.records` limits how many records `poll()` returns per call. Its relationship to offset management is indirect but important:

**Batch size determines commit granularity**: if `max.poll.records = 500` and processing the batch takes 30 seconds, you commit 500 records at once. A crash halfway means all 500 are reprocessed. With `max.poll.records = 5`, you commit more frequently — smaller re-processing windows on failure.

**Affects `max.poll.interval.ms` compliance**: if `max.poll.records` is large and processing is slow, you may exceed `max.poll.interval.ms` between `poll()` calls. Kafka removes you from the group. Smaller `max.poll.records` = smaller batches = faster processing = more frequent `poll()` calls = stays within the interval.

**Offset management complexity**: with a small `max.poll.records` (like 5 in our demo), you can inspect exactly which records are in each batch and verify commits in the Docker CLI after every batch. With 500, the logs are a wall and batch boundaries are invisible.

Production tuning heuristic: start with `max.poll.records = 100-500`, measure per-batch processing time, ensure it's well under `max.poll.interval.ms / 2`, then tune upward for throughput or downward for failure-recovery granularity.

---

## 📊 Quick Reference Card

| Term | Definition |
|------|------------|
| `position(tp)` | Next offset to fetch — in-memory only, advances after poll(), not persisted |
| `committed(tps)` | Last committed offset — persisted to `__consumer_offsets`, survives restart |
| `seekToBeginning()` | Resets position to 0 — NOT permanent without a subsequent commitSync |
| `seekToEnd()` | Jumps position to latest — skips all existing messages for this group |
| `seek(tp, offset)` | Moves position to specific offset on one partition |
| `auto.offset.reset` | Bootstrap policy: earliest / latest / none — applies only when no committed offset |
| Poison pill | Message that causes consumer failure on every retry attempt |
| Skip pattern | `commitSync(badOffset + 1)` — discard bad record and advance |
| `__consumer_offsets` | Internal Kafka topic storing all committed offsets |

---

## ✅ Self-Assessment

After studying this Q&A, you should be able to:

- Explain the difference between `position()` and `committed()` with a concrete example
- Describe all three `auto.offset.reset` values and the exact condition each applies to
- Explain why `seekToBeginning()` alone doesn't make the rewind permanent
- Define a poison pill and describe three handling strategies (skip, DLT, DefaultErrorHandler)
- Explain when `seekToEnd()` is appropriate and what its permanent risk is
- Use `commitSync(Map<TopicPartition, OffsetAndMetadata>)` for per-partition commits
- Explain what `__consumer_offsets` is and what happens when a commit fails
- Describe how `max.poll.records` affects both offset management and `max.poll.interval.ms` compliance
