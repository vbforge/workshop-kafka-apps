# Case 13: Q&A Document

## 🎓 Theory Section

### Core Concepts Covered

| Concept | Description |
|---------|-------------|
| **Exactly-once semantics (EOS)** | A message is processed and its side-effects (downstream produces, offset commits) happen exactly once, regardless of failures or retries |
| **Transactional producer** | Producer with `transactional.id` set — participates in Kafka's multi-partition atomic write protocol |
| **Idempotent producer** | Sequence-numbered sends — duplicate retries are detected and discarded by the broker |
| **`KafkaTransactionManager`** | Spring bean that ties `@Transactional` method boundaries to Kafka `beginTransaction()`/`commitTransaction()`/`abortTransaction()` |
| **`read_committed` isolation** | Consumer only delivers records from committed transactions; uncommitted and aborted records are invisible |
| **Control batch** | Broker-internal record marking a transaction as COMMIT or ABORT — signals `read_committed` consumers to advance |
| **Zombie fencing** | Broker rejects produce requests from a producer with an outdated epoch for a given `transactional.id` |
| **Consume-process-produce (CPP)** | Fully atomic pattern: consumer offset commit happens INSIDE the same Kafka transaction as the downstream produce |

### How Kafka Transactions Work Internally

```
Producer                    Broker (Transaction Coordinator)           Broker (Data Partitions)
   │                                    │                                       │
   │── initTransactions() ─────────────►│                                       │
   │   (register transactional.id,       │                                       │
   │    get ProducerID + epoch)          │                                       │
   │                                    │                                       │
   │── beginTransaction() ──────────────► (local state only — no broker call)   │
   │                                    │                                       │
   │── send(processed-topic, record) ───────────────────────────────────────────►│
   │   (record written, marked pending) │                                       │
   │                                    │                                       │
   │── send(probe-topic, record) ────────────────────────────────────────────────►│
   │   (record written, marked pending) │                                       │
   │                                    │                                       │
   │── commitTransaction() ────────────►│                                       │
   │                        writes COMMIT control batch to both partitions ──────►│
   │                                    │                                       │
   │                            read_committed consumers NOW see both records    │
```

For rollback, `abortTransaction()` writes ABORT control batches instead.
The records still physically exist in the log — `read_committed` consumers skip them.

---

## 📝 Interview Q&A

### Q1: What is exactly-once semantics in Kafka, and what three components enable it?

**Answer:**

Exactly-once semantics (EOS) means that a message produces its effect exactly once in the system — no loss, no duplication — even in the presence of producer retries, broker failures, or consumer restarts.

Kafka achieves EOS with three components working together:

**1. Idempotent producer** (`enable.idempotence=true`):
Each producer instance is assigned a unique `ProducerID (PID)` by the broker. Every send to a partition includes a monotonically increasing sequence number per `(PID, partition)` pair. If the broker receives the same sequence number twice (duplicate due to retry), it discards the duplicate silently. This eliminates at-least-once duplication at the producer level.

**2. Transactional producer** (`transactional.id` set):
Extends idempotence to multi-partition atomic writes. The `transactional.id` is registered with the broker's Transaction Coordinator. The producer can then atomically write to multiple partitions — all writes commit or all abort. This eliminates partial writes across partitions.

**3. Read-committed consumer** (`isolation.level=read_committed`):
Consumers only see records from fully committed transactions. Records from in-flight or aborted transactions are invisible until committed — at which point they become visible atomically. This eliminates the "ghost record" problem where a consumer processes a record that later gets rolled back.

All three are required. Idempotence without transactions prevents per-partition duplicates but doesn't help with multi-partition atomicity. Transactions without `read_committed` consumers are invisible on the consume side. `read_committed` without a transactional producer has no effect.

---

### Q2: What does `KafkaTransactionManager` do, and why is `@Transactional` not enough without it?

**Answer:**

`@Transactional` is a Spring framework mechanism — it's a generic interceptor that calls `TransactionManager.doBegin()` on method entry and `doCommit()` or `doRollback()` on exit.

Without `KafkaTransactionManager`, Spring doesn't know how to translate those lifecycle hooks into Kafka-specific operations. The `@Transactional` annotation fires — Spring calls `doBegin()` — but since there's no Kafka-aware `PlatformTransactionManager` registered, nothing Kafka-specific happens. The `kafkaTemplate.send()` calls execute normally, outside any Kafka transaction.

`KafkaTransactionManager` implements `PlatformTransactionManager` and wires it to the `ProducerFactory`:

```java
@Bean
public KafkaTransactionManager<String, Object> kafkaTransactionManager() {
    return new KafkaTransactionManager<>(producerFactory());
}
```

Now when Spring's transaction interceptor fires:
- `doBegin()` → calls `kafkaTemplate.beginTransaction()` → calls `producer.beginTransaction()`
- `doCommit()` → calls `kafkaTemplate.commitTransaction()` → calls `producer.commitTransaction()`
- `doRollback()` → calls `kafkaTemplate.abortTransaction()` → calls `producer.abortTransaction()`

The `@Transactional` annotation also provides the exception handling logic: any unchecked exception causes rollback, normal return causes commit. This is exactly the behaviour you want for Kafka transactions — an unhandled `RuntimeException` mid-send should abort the whole transaction.

Important: if you have BOTH a `KafkaTransactionManager` and a JPA/database `TransactionManager` in the same application, you need `@Transactional("kafkaTransactionManager")` to specify which one. Otherwise Spring uses the default (usually the database one) and your Kafka sends are not transactional.

---

### Q3: What is `isolation.level=read_committed` and what problem does it solve?

**Answer:**

`isolation.level` is a consumer configuration that controls which records are visible to the consumer from a transactionally-produced topic.

**`read_uncommitted` (default)**: the consumer sees records as soon as they're written to the broker's log — including records from in-flight transactions that haven't committed yet, and records from transactions that will eventually be aborted. This is the "ghost record" problem.

Ghost record scenario:
1. Transactional producer sends record to `payments-topic` — offset 15.
2. Consumer (read_uncommitted) reads offset 15 — "payment processed!"
3. Producer's transaction aborts — offset 15 is now an aborted record.
4. Consumer has already processed a payment that was supposed to not exist.

**`read_committed`**: the consumer only sees records from committed transactions. The broker uses COMMIT/ABORT control batches as signals. When the consumer encounters a record from a pending transaction, it pauses at that position and waits. When it sees the COMMIT control batch, it delivers the records. When it sees the ABORT control batch, it skips those records and advances.

This means `read_committed` consumers have slightly higher latency — they may wait for the transaction to complete before reading. The upside is that they never process a record that doesn't exist from the application's perspective.

For `read_committed` to be effective, the consumer must be reading from a topic that transactional producers write to. Using `read_committed` on a topic written by non-transactional producers has no effect — all records are always visible.

---

### Q4: What is zombie fencing, and why does `transactional.id` need to be unique per producer instance?

**Answer:**

A zombie producer is a producer instance that believes it's still the active producer for a given `transactional.id`, but a new instance has been started (e.g. after a crash and restart). Without fencing, both instances could be writing simultaneously — causing data corruption.

**How zombie fencing works:**

Each time a producer registers a `transactional.id` with the broker's Transaction Coordinator, it receives a `(ProducerID, epoch)` pair. The `epoch` increments on every registration.

When a new instance starts with the same `transactional.id`, the Transaction Coordinator bumps the epoch. Any subsequent write attempt from the old instance (with the old epoch) is rejected by the broker:
```
ProducerFencedException: There is a newer producer with the same transactional.id
```

This guarantees that at most one producer instance is active per `transactional.id` at any time — even in a distributed environment with network partitions.

**Why uniqueness per instance matters:**

In a horizontally scaled service with 3 instances of `OrderProcessingService`, you need 3 unique transactional IDs:
```
instance-0: transactional.id = "order-processor-0"
instance-1: transactional.id = "order-processor-1"
instance-2: transactional.id = "order-processor-2"
```

If all three share the same `transactional.id`, starting instance-1 fences instance-0 and instance-2. Only one can be active, which defeats the purpose of horizontal scaling.

Typical production pattern using Spring Boot with instance indexing:
```yaml
kafka:
  producer:
    transactionalId: order-processor-${INSTANCE_ID:0}
```

Or with Kubernetes StatefulSet ordinal: `order-processor-${POD_NAME}`.

For case-13's single-instance demo, a static ID is fine. But don't forget this in production.

---

### Q5: What is the consume-process-produce (CPP) pattern, and why is it the "true" exactly-once pattern?

**Answer:**

The consume-process-produce pattern is the gold standard for Kafka exactly-once processing pipelines. It makes the consumer offset commit part of the same Kafka transaction as the downstream produce — so both happen atomically or neither does.

**The problem with separate ack and produce:**

In case-13's demo, the consumer uses `AckMode.RECORD` — the offset is committed after the listener returns. The transactional produce (inside `sendCommitted()`) happens first, then the offset commits separately. There's a window where:
1. Transaction commits — downstream record visible.
2. App crashes before offset commit.
3. Consumer restarts, re-processes the same input message.
4. Downstream record is produced again — duplicate.

**The CPP solution** uses `sendOffsetsToTransaction()`:

```java
// In a @Transactional method — AckMode.MANUAL required
@Transactional
public void processAndForward(ConsumerRecord<String, OrderMessage> record,
                               Acknowledgment ack) {
    // 1. Begin transaction (via @Transactional)

    // 2. Produce to downstream topic — inside the transaction
    kafkaTemplate.send(processedTopic, record.key(), record.value());

    // 3. Commit consumer offset AS PART OF THE SAME TRANSACTION
    Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(
            new TopicPartition(record.topic(), record.partition()),
            new OffsetAndMetadata(record.offset() + 1)
    );
    kafkaTemplate.sendOffsetsToTransaction(offsets, consumerGroupId);

    // 4. @Transactional commits both the produce AND the offset atomically
}
```

The broker's Transaction Coordinator handles the offset commit as a transactional write to the `__consumer_offsets` topic. On rollback, both the produced record AND the offset commit are aborted — the consumer re-reads the same message from its last committed offset.

**This is true exactly-once:** the input record is processed and its output appears exactly once in the downstream topic, with no duplicates on retry.

Case-13 demonstrates the simpler "transactional produce only" pattern to keep the focus clear. CPP with `sendOffsetsToTransaction()` is the production pattern for stateful streaming pipelines.

---

### Q6: What is an idempotent producer and how does sequence number tracking work?

**Answer:**

An idempotent producer prevents duplicate records when the producer retries a send.

**Why duplicates happen without idempotence:**

1. Producer sends record to broker.
2. Broker writes the record and sends ACK.
3. Network glitch — ACK is lost.
4. Producer's retry timer fires — re-sends the same record.
5. Broker writes the record AGAIN — duplicate.

**How idempotence solves it:**

When `enable.idempotence=true`, the broker assigns a unique `ProducerID (PID)` to the producer. Every batch sent to a partition includes:
- The producer's PID
- A monotonically increasing sequence number per `(PID, partition)` pair

The broker tracks the latest sequence number it has received and acknowledged for each `(PID, partition)`. When a retry arrives:
- Sequence number = expected next → write it (normal case)
- Sequence number < expected → duplicate, discard silently
- Sequence number > expected → out-of-order, throw `OutOfOrderSequenceException`

This deduplication is transparent to the application. The producer retries until it gets an ACK with the right sequence number, and the broker ensures only one copy exists in the log.

**Important limits:**

Idempotence is per-partition, per-PID. It does NOT prevent duplicates across different producer instances (different PIDs) — that requires transactions. And it does NOT survive broker restarts cleanly without transactions — the PID is ephemeral.

When `transactional.id` is set, idempotence is automatically enabled and the PID becomes durable (survives producer restart via the `transactional.id` registration).

---

### Q7: What is the `acks=all` requirement for transactional producers, and what does it protect against?

**Answer:**

`acks=all` means the broker's partition leader waits for ALL in-sync replicas (ISR) to acknowledge the record before responding to the producer. Setting `transactional.id` automatically enforces `acks=all` — the broker will reject transactional producers configured with `acks=1` or `acks=0`.

**What `acks=1` risks:**

1. Producer sends record, leader writes it to its log.
2. Leader sends ACK (acks=1 — only leader needed).
3. Leader crashes before replication to followers.
4. A follower is elected as new leader — it doesn't have the record.
5. The record is gone, even though the producer received an ACK.

For a non-transactional producer, this is a durability trade-off you might accept for throughput. For a transactional producer, this is a correctness violation — a committed transaction can silently disappear.

**What `acks=all` guarantees:**

The record is written to ALL ISRs before the ACK is sent. If the leader crashes after acknowledging, any ISR elected as new leader already has the record. The transaction's committed state survives any single broker failure.

**ISR configuration:**

`acks=all` is only as strong as your ISR settings:
- `min.insync.replicas=1` (default): with `acks=all`, the leader alone constitutes "all" ISRs → effectively the same as `acks=1` if the replica factor is 1.
- `min.insync.replicas=2`: at least 2 replicas must acknowledge → survives one broker failure.

For a single-broker dev setup (case-13), `min.insync.replicas=1` is the only option. In production, `replication.factor=3, min.insync.replicas=2` is the standard durable configuration.

---

### Q8: What happens to aborted transaction records in the Kafka log? Are they actually deleted?

**Answer:**

No — Kafka's log is append-only. Aborted records are NEVER physically deleted from the log immediately after abort. They remain in the log until the log segment's retention period expires and the segment is cleaned.

**What "invisible" actually means:**

After a transaction aborts, the Transaction Coordinator instructs the broker to write an ABORT control batch to each affected partition. The ABORT control batch is a special marker record in the log — it's not the data record itself, just a signal.

When a `read_committed` consumer reaches a data record that's part of an aborted transaction, it reads the ABORT control batch marker (which follows eventually in the log) and skips over the data records. From the consumer's perspective, those records don't exist.

A `read_uncommitted` consumer would read those same records normally — it ignores control batches and delivers all records regardless of transaction outcome.

**Practical implications:**

**Log size**: aborted records consume disk space until their segment is deleted. A pathological producer that begins transactions and never commits them (bug) fills the log with ghost records that can't be consumed. Monitor `kafka_log_log_size` per topic.

**Log compaction**: aborted records are handled specially by log compaction. The compactor sees the ABORT marker and is allowed to remove the corresponding data records from compacted segments. This is how disk is eventually reclaimed.

**Offset gaps**: because aborted records occupy real offsets in the log, consumer offsets can "jump" over many positions. An offset advancing from 5 to 15 with only a few committed records between them is normal if many transactions were aborted in that range.

**Debugging**: to inspect aborted records, you can use a `read_uncommitted` consumer or the `kafka-dump-log` tool with `--print-data-log` to see the raw log contents including abort markers.

---

## 📊 Quick Reference Card

| Term | Definition |
|------|------------|
| EOS | Exactly-once semantics — no loss, no duplication, regardless of failures |
| Idempotent producer | Sequence-numbered sends — broker deduplicates retries silently |
| Transactional producer | `transactional.id` set — enables atomic multi-partition writes |
| `KafkaTransactionManager` | Spring bean that maps `@Transactional` lifecycle to Kafka protocol calls |
| `read_committed` | Consumer isolation — only sees records from committed transactions |
| Control batch | COMMIT or ABORT marker in the log — signals `read_committed` consumers |
| Zombie fencing | Epoch-based protection against split-brain producers sharing a `transactional.id` |
| CPP pattern | Consume-process-produce — offset commit inside the same Kafka transaction |
| `sendOffsetsToTransaction()` | API for atomic consumer offset + produce in one transaction |

---

## ✅ Self-Assessment

After studying this Q&A, you should be able to:

- List the three components required for exactly-once semantics and explain each
- Explain why `@Transactional` alone is not enough — what `KafkaTransactionManager` adds
- Describe what `read_committed` prevents and what a "ghost record" is
- Explain zombie fencing and why `transactional.id` must be unique per producer instance
- Describe the CPP pattern and how `sendOffsetsToTransaction()` achieves atomic processing
- Explain how idempotent sequence numbers prevent duplicate records on retry
- Explain why `acks=all` is enforced for transactional producers
- Describe what happens to aborted records in the Kafka log (hint: they stay)
