# Scenario 05 — Manual Offset Management

**What this scenario demonstrates:**
- The difference between auto-commit and manual commit — and why it matters
- At-least-once vs at-most-once delivery guarantees
- Three manual commit strategies: per-batch, per-N-records, per-partition
- When each strategy is appropriate and what you trade off
---

## Prerequisites

- Docker running
- Kafka started from project root:
  ```bash
  docker-compose up -d
  docker-compose ps
  ```

---

## Topic Setup

`topic-manual-offset` with **2 partitions** — enough to observe per-partition commit behavior.

```bash
docker exec -it kafka-learning-broker \
  kafka-topics --create \
  --topic topic-manual-offset \
  --partitions 2 \
  --replication-factor 1 \
  --bootstrap-server localhost:19092
```

Verify:
```bash
docker exec -it kafka-learning-broker \
  kafka-topics --describe --topic topic-manual-offset --bootstrap-server localhost:19092
```
 
---

## The Core Concept Before You Run

Kafka tracks **offsets** — a sequential number per partition that marks how far a consumer group has read. 
When you commit an offset, you're telling Kafka: "I've processed everything up to here. If I restart, start me from the next record."

**At-most-once** (auto-commit): offset is committed on a timer, before you finish processing → messages can be lost on crash, never duplicated.

**At-least-once** (manual commit): offset is committed only after successful processing → messages are never lost, but may be reprocessed on crash.

Run each consumer below against the same producer and observe the difference in behaviors for each case.
 
---

## Part 1 — Auto-Commit (baseline, risky)

Terminal 1 — consumer:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_05_manual_offset_management.AutoCommitConsumer"
```

Terminal 2 — producer:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_05_manual_offset_management.OrderProducer"
```

**To observe message loss:**
1. Start AutoCommitConsumer and OrderProducer together
2. While messages are flowing, force-kill the consumer:
   press the **IntelliJ Stop button** (SIGKILL — bypasses hooks entirely)
3. Restart the consumer
4. Notice: some records are skipped — they were already committed by auto-commit
   before `processOrder()` finished
   Check what offset the group is at:
```bash
docker exec -it kafka-learning-broker \
  kafka-consumer-groups --bootstrap-server localhost:19092 \
  --group consumer-group-manual-offset-auto --describe
```

---

## Part 2 — Manual Commit per batch (safe, at-least-once)

Terminal 1 — consumer:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_05_manual_offset_management.ManualCommitConsumer"
```

Terminal 2 — producer:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_05_manual_offset_management.OrderProducer"
```

**What to observe:**
- Log shows `OK | key: ORDER-xxx` per record, then `Batch committed (N records)` once per poll batch
- Approximately 10% of orders trigger a simulated failure → `FAILED | key: ... — batch will NOT be committed`
- After a failure, restart the consumer — those failed orders are redelivered from the last committed offset
  **To prove at-least-once:**
1. Run the producer to send all 20 orders
2. Stop the consumer mid-processing with `Ctrl+C`
3. Restart the consumer
4. It picks up from the last committed offset — no orders are skipped
```bash
docker exec -it kafka-learning-broker \
  kafka-consumer-groups --bootstrap-server localhost:19092 \
  --group consumer-group-manual-offset --describe
```
 
---

## Part 3 — Batch commit every N records (balanced throughput)

Terminal 1 — consumer:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_05_manual_offset_management.BatchCommitConsumer"
```

Terminal 2 — producer:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_05_manual_offset_management.OrderProducer"
```

**What to observe:**
- Commit log appears every 5 records: `--- Batch commit at record #5 ---`
- On `Ctrl+C`, the shutdown hook fires → `finally` block runs → final `commitSync()` flushes
  any remaining records (1–4) that hadn't reached the commit threshold yet
- This guarantees no records are lost even when stopping mid-batch
```bash
docker exec -it kafka-learning-broker \
  kafka-consumer-groups --bootstrap-server localhost:19092 \
  --group consumer-group-manual-offset-batch --describe
```
 
---

## Part 4 — Per-partition commit (fine-grained control)

Terminal 1 — consumer:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_05_manual_offset_management.PerPartitionCommitConsumer"
```

Terminal 2 — producer:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_05_manual_offset_management.OrderProducer"
```

**What to observe:**
- Output shows `[Partition 0]` and `[Partition 1]` processing separately within each poll batch
- After all records from a partition are done, that partition's offset is committed independently:
  `[Partition 0] Committed up to offset 4`
- If partition 1 failed mid-batch, partition 0's progress would already be safely committed
  **The offset + 1 rule:**
  Kafka stores the *next* offset to read, not the last offset read.
  If the last processed record had offset 4, you commit `new OffsetAndMetadata(5)`.
  This is always `lastOffset + 1`.

```bash
docker exec -it kafka-learning-broker \
  kafka-consumer-groups --bootstrap-server localhost:19092 \
  --group consumer-group-manual-offset-per-partition --describe
```
 
---

## Commit Strategy Comparison

| Strategy | Class | Group suffix | Guarantee | Throughput | Use when |
|---|---|---|---|---|---|
| Auto-commit | `AutoCommitConsumer` | `-auto` | At-most-once | Highest | Loss is acceptable (metrics, logs) |
| Manual per-batch | `ManualCommitConsumer` | _(base)_ | At-least-once | Medium | Most transactional workloads |
| Batch every N | `BatchCommitConsumer` | `-batch` | At-least-once | High | High throughput with tolerable reprocessing window |
| Per-partition | `PerPartitionCommitConsumer` | `-per-partition` | At-least-once | Medium | Independent partition processing, partial failure isolation |
 
---

## Kafka CLI Reference

```bash
# Watch all consumer groups for this topic
docker exec -it kafka-learning-broker \
  kafka-consumer-groups --bootstrap-server localhost:19092 --list
 
# Check offsets and lag for a specific group
docker exec -it kafka-learning-broker \
  kafka-consumer-groups --bootstrap-server localhost:19092 \
  --group consumer-group-manual-offset --describe
 
# Read all messages from beginning
docker exec -it kafka-learning-broker \
  kafka-console-consumer --topic consumer-group-manual-offset \
  --from-beginning \
  --property print.key=true \
  --bootstrap-server localhost:19092
```
 
---

## Cleanup

```bash
docker exec -it kafka-learning-broker \
  kafka-topics --delete --topic consumer-group-manual-offset --bootstrap-server localhost:19092
 
docker-compose down -v
```
 
---

## What You Observed

| Concept | Where you saw it |
|---|---|
| At-most-once (message loss) | Part 1 — force-kill auto-commit consumer, restart, records skipped |
| At-least-once (reprocessing) | Part 2 — `Ctrl+C` mid-batch, restart, records redelivered from last commit |
| Batch commit efficiency | Part 3 — one commit per 5 records instead of one per record |
| Final commit on shutdown | Part 3 — `finally` block commits remaining records before close |
| Per-partition isolation | Part 4 — each partition's offset committed independently |
| `offset + 1` rule | Part 4 — `OffsetAndMetadata(lastOffset + 1)` explained in logs and code |

