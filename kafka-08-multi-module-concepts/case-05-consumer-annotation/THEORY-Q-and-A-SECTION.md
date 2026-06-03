# Case 05: Q&A Document

## 🎓 Theory Section

### Core Concepts Covered

| Concept | Description |
|---------|-------------|
| **`@KafkaListener`** | Spring annotation that wires a method to a Kafka topic; Spring manages the poll loop |
| **`ConcurrentKafkaListenerContainerFactory`** | Spring factory that creates N consumer threads per listener |
| **`concurrency`** | Number of consumer threads (`KafkaConsumer` instances) within one listener |
| **Consumer group** | All consumers sharing a `group.id`; Kafka distributes partitions across them |
| **`AckMode.BATCH`** | Offsets committed after each `poll()` batch is fully processed |
| **`ENABLE_AUTO_COMMIT_CONFIG=false`** | Disables Kafka's timer-based commit; hands control to Spring AckMode |
| **`auto.offset.reset`** | Where a new consumer group starts: `earliest` (beginning) or `latest` (end) |
| **`AtomicInteger`** | Thread-safe integer — required when multiple consumer threads share state |

### The Commit Model Comparison

| Mode | Who commits | When | Risk |
|------|-------------|------|------|
| Kafka native auto-commit | Kafka background thread | On a timer (every 5s by default) | Commits before processing — message loss on crash |
| `AckMode.BATCH` (Spring) | Spring, after poll batch | After listener returns without exception | At-least-once — duplicate on crash between process and commit |
| `AckMode.RECORD` (Spring) | Spring, per record | After each listener call returns | Safer, slower |
| `AckMode.MANUAL` (Spring) | You, explicitly | When you call `ack.acknowledge()` | Full control — case-06 territory |

---

## 📝 Interview Q&A

### Q1: What does `@KafkaListener` actually do under the hood?

**Answer:**

`@KafkaListener` is processed by `KafkaListenerAnnotationBeanPostProcessor` at application startup. For each annotated method, Spring:

1. Creates a `MethodKafkaListenerEndpoint` that wraps the method as a message handler.
2. Passes it to `ConcurrentKafkaListenerContainerFactory`, which builds a `ConcurrentMessageListenerContainer`.
3. The container starts N `KafkaMessageListenerContainer` instances (one per `concurrency` unit), each running its own internal thread.
4. Each internal thread runs an infinite `poll()` loop: call `KafkaConsumer.poll(timeout)`, get a batch of records, invoke the listener method for each record, commit offsets (per the AckMode), repeat.

The key thing to understand: you never write a poll loop. Spring owns it. Your `@KafkaListener` method is just a callback — it receives one `ConsumerRecord` at a time (or a `List` for batch listeners), does its work, and returns. Spring handles the rest.

This is the fundamental difference between Spring Kafka and using the raw `KafkaConsumer` API directly (case-06): with `@KafkaListener`, the infrastructure is invisible. With raw `KafkaConsumer`, you own the entire loop.

---

### Q2: What is the relationship between `concurrency`, partition count, and consumer threads?

**Answer:**

`concurrency=N` creates N `KafkaConsumer` instances, all with the same `group.id`. When they connect to Kafka, a group rebalance happens and Kafka distributes the topic's partitions across the N consumers.

The rules:
- If `concurrency == partition count`: perfect assignment, one partition per thread. This is the ideal for throughput.
- If `concurrency < partition count`: some threads own multiple partitions (still works, less parallelism).
- If `concurrency > partition count`: some threads are assigned zero partitions and sit idle forever. This wastes memory and threads — never do it.

The practical implication: **partition count is the ceiling for parallelism**. No matter how many consumer threads you spin up, you can never have more active consumers than partitions for a single topic. This is why you plan partition counts generously upfront — you can add consumer threads dynamically, but you can't reduce partitions after the fact without data migration.

---

### Q3: What is the difference between Kafka's native `auto.commit` and Spring Kafka's `AckMode.BATCH`?

**Answer:**

Both automatically commit offsets without you calling anything manually, but they commit at different times with different safety guarantees.

**Kafka's native auto-commit** (`enable.auto.commit=true`): A background thread inside `KafkaConsumer` commits the current offset every `auto.commit.interval.ms` (default 5000ms). The commit happens on a timer — completely independent of whether your application has finished processing the records. If your listener is slow (takes 6s to process a batch) and the app crashes at second 5, Kafka already committed the offset at second 5 — the message is gone.

**Spring's `AckMode.BATCH`** (`enable.auto.commit=false` + Spring manages commits): Spring commits offsets after each `poll()` batch is fully processed — meaning after your listener method has returned successfully for every record in the batch. If your listener throws an exception, Spring does NOT commit — it can re-deliver the record. This ties commit timing to processing completion, not a wall clock.

The result: `AckMode.BATCH` gives you at-least-once delivery semantics. If the app crashes after processing but before Spring commits, you get duplicates on restart. That's acceptable and recoverable. With native auto-commit, you can get message loss — much worse.

---

### Q4: What does `auto.offset.reset` do, and when does it apply?

**Answer:**

`auto.offset.reset` tells a consumer where to start reading when there is **no committed offset** for its consumer group on a partition. This happens in exactly two scenarios: (1) the group is brand new and has never consumed from this topic, (2) the committed offset is stale (the offset points to a position that no longer exists in the log, because Kafka's log retention deleted old segments).

Values:
- `earliest` → start from offset 0 (the beginning of the partition log, or the earliest available if old segments were deleted). Use this in development, testing, or when you need to process historical data.
- `latest` → start from the current end of the log, consuming only new messages that arrive after the consumer starts. Use this in production consumers that don't care about historical data.
- `none` → throw an exception if no committed offset exists. Use this when missing historical data would be a bug — forces you to handle the "no prior offset" case explicitly.

**Critical nuance**: `auto.offset.reset` only applies when there's no committed offset. After the first successful commit, this setting is ignored — the consumer always resumes from its last committed offset, regardless of `auto.offset.reset`.

---

### Q5: Why is `AtomicInteger` required for a shared counter across concurrent listener threads?

**Answer:**

Three consumer threads call `consume()` concurrently. If the shared counter were a plain `int` field, a classic read-modify-write race condition could occur:

Thread 1 reads `counter = 5`. Thread 2 reads `counter = 5` (before Thread 1 writes back). Thread 1 writes `counter = 6`. Thread 2 writes `counter = 6`. Net result: two increments, but counter only went from 5 to 6. One increment was lost.

`AtomicInteger.incrementAndGet()` uses a Compare-And-Swap (CAS) CPU instruction. CAS reads the current value, computes the new value, and writes back — but only if the value hasn't changed since the read. If another thread modified it in the meantime, CAS retries. This is lock-free thread safety: no `synchronized` blocks, no locks, no blocking.

The broader lesson: whenever a field is accessed by multiple threads, you must use either synchronization (`synchronized`, `Lock`) or thread-safe types (`AtomicInteger`, `ConcurrentHashMap`, `volatile` for visibility-only). A plain field is a race condition waiting to happen. This applies to any shared state in your consumer — database connections, caches, request counters.

---

### Q6: What is a consumer group rebalance, and what triggers one?

**Answer:**

A rebalance is the process by which Kafka redistributes partitions across all consumers in a group. During a rebalance, all consumers in the group stop consuming — this is called a stop-the-world pause.

What triggers a rebalance:
- A new consumer joins the group (e.g. you scale up from 2 to 3 instances)
- A consumer leaves the group (graceful shutdown or crash)
- A consumer fails to send a heartbeat within `session.timeout.ms` (default 10s) — Kafka assumes it's dead
- The topic's partition count changes
- A consumer's poll takes longer than `max.poll.interval.ms` (default 5 minutes) — Kafka assumes it's stuck

During rebalance all consumers pause. A "group coordinator" broker orchestrates the redistribution. After rebalance completes, each consumer resumes from its last committed offset on its newly assigned partitions.

The production implication: slow `@KafkaListener` methods are dangerous. If your listener processes a batch that takes longer than `max.poll.interval.ms`, Kafka kicks the consumer out of the group mid-processing, triggers a rebalance, and the batch gets redelivered to another consumer (duplicate processing). Always keep listener methods fast, or tune `max.poll.interval.ms` if you have legitimately slow processing.

---

### Q7: Can a single `@KafkaListener` method consume from multiple topics? What are the trade-offs?

**Answer:**

Yes. The `topics` attribute accepts an array:
```java
@KafkaListener(topics = {"topic-a", "topic-b", "topic-c"}, ...)
public void consume(ConsumerRecord<String, Object> record) { ... }
```

The same listener method receives records from all listed topics. The `record.topic()` field tells you which topic each record came from, so you can branch your logic.

Trade-offs:

**Pro**: one consumer group subscription handles multiple topics. Useful when topics are logically related (e.g. all user events: `user-created`, `user-updated`, `user-deleted`) and share the same processing logic.

**Con**: all topics share the same `concurrency` setting and the same thread pool. If `topic-a` is high volume and `topic-b` is low volume, the same threads serve both — you can't tune them independently. Also, a deserialization failure on one topic can affect the listener for all topics if error handling isn't set up carefully.

**Best practice**: use multi-topic listeners only when topics are tightly coupled and similarly sized. For independent topics with different volumes, give each its own `@KafkaListener` with its own `containerFactory` (and therefore its own concurrency). This is the "multiple listener methods" approach — the second approach you have queued up for this case.

---

### Q8: What happens to in-flight messages when a consumer crashes mid-batch (before `AckMode.BATCH` commits)?

**Answer:**

With `AckMode.BATCH` and `enable.auto.commit=false`: if the consumer crashes after processing some records in a batch but before Spring commits the offsets, those records will be redelivered when the consumer restarts (or when another consumer in the group picks up the partition after rebalance).

This is **at-least-once delivery**: every message is guaranteed to be processed at least once, but may be processed more than once on crash/restart. Duplicates are possible.

The practical consequence: your listener must be idempotent — processing the same message twice should produce the same result as processing it once. Common patterns:
- Check a database `processed_message_ids` table before acting
- Use upsert semantics (INSERT ON DUPLICATE KEY UPDATE) in database writes
- Make downstream API calls idempotent (use PUT with the resource ID, not POST)

If you can't make your listener idempotent and need exactly-once: that's case-13's territory — transactions + idempotent producer + exactly-once consumer configuration. It's significantly more complex and has throughput trade-offs.

---

## 📊 Quick Reference Card

| Term | Definition |
|------|------------|
| `@KafkaListener` | Declares a method as a Kafka consumer; Spring manages the poll loop |
| `concurrency` | Number of consumer threads (KafkaConsumer instances) for one listener |
| `AckMode.BATCH` | Commit offsets after poll batch fully processed — Spring's default safe commit mode |
| `enable.auto.commit=false` | Disables Kafka's timer-based commit; required for Spring AckMode to work correctly |
| `auto.offset.reset` | Where to start reading when no committed offset exists: `earliest` / `latest` / `none` |
| Rebalance | Partition redistribution across a consumer group — pauses all consumers |
| At-least-once | Every message processed ≥ 1 time; duplicates possible on crash |
| `AtomicInteger` | Thread-safe integer for shared counters across concurrent threads |

---

## ✅ Self-Assessment

After studying this Q&A, you should be able to:

- Describe what Spring does under the hood when it processes `@KafkaListener`
- Explain the relationship between `concurrency`, partition count, and active threads
- Distinguish Kafka native auto-commit from Spring's `AckMode.BATCH` and explain why the latter is safer
- Explain `auto.offset.reset` and the two scenarios where it applies
- Explain why `AtomicInteger` is required instead of `int` in concurrent listeners
- Describe what triggers a consumer group rebalance and its performance impact
- Explain the at-least-once semantics of `AckMode.BATCH` and what idempotency means in this context
- Decide when multi-topic listeners are appropriate vs separate listeners per topic
