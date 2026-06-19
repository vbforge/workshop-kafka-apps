# Case 15 — Batch Processing (`MAX_POLL_RECORDS` + Batch Listener + Manual Batch Commit)

| Field          | Value                          |
|----------------|--------------------------------|
| Module         | `case-15-batch-processing`     |
| Port           | `8095`                         |
| Topic          | `case-15-events`               |
| Consumer Group | `case-15-consumer-group`       |
| Spring Boot    | `4.0.6`                        |
| Java           | `21`                           |

---

## What This Case Covers

| Concept | Description |
|---------|-------------|
| **`setBatchListener(true)`** | The single config line that turns a Spring Kafka listener into a batch listener |
| **`List<ConsumerRecord<K,V>>`** | The listener method signature for batch mode — receives all records from one `poll()` |
| **`max.poll.records`** | Limits how many records a single `poll()` returns — the primary batch size knob |
| **`max.poll.interval.ms`** | Maximum time between `poll()` calls — must be > batch processing time |
| **Manual batch commit** | `ack.acknowledge()` called ONCE after processing the entire batch |
| **At-least-once semantics** | Batch not committed → redelivered on restart — all records in batch re-processed |

### Single-Record vs Batch Listener

```
Single-record listener (cases 05, 08, 09...):
  poll() → [record 1] → process → commit
  poll() → [record 2] → process → commit
  poll() → [record 3] → process → commit
  → 3 commits for 3 records

Batch listener (case-15, max.poll.records=50):
  poll() → [record 1 ... record 50] → process all 50 → commit once
  → 1 commit for 50 records
  → ~50x fewer commits = significantly higher throughput
```

### The Commit-Once Pattern

```java
// WRONG — loses the batch benefit:
for (ConsumerRecord<...> record : records) {
    process(record);
    ack.acknowledge();  // ← commit per record! 50 commits for 50 records
}

// CORRECT — one commit for the whole batch:
for (ConsumerRecord<...> record : records) {
    process(record);    // no commit inside the loop
}
ack.acknowledge();      // ← commit once after the loop
```

---

## Project Structure

```
case-15-batch-processing/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/vbforge/case15/
    │   │   ├── MainApp.java
    │   │   ├── config/
    │   │   │   └── KafkaConfig.java          # setBatchListener(true) here
    │   │   ├── controller/
    │   │   │   └── BatchController.java
    │   │   ├── model/
    │   │   │   ├── BatchStatus.java
    │   │   │   ├── EventMessage.java
    │   │   │   └── ProducerResponse.java
    │   │   └── service/
    │   │       ├── BatchConsumerService.java  # ← THE main class
    │   │       └── ProducerService.java
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/vbforge/case15/
            └── MainAppTests.java
```

---

## Quick Start

```bash
docker compose up -d
cd case-15-batch-processing
mvn spring-boot:run
```

---

## Interactive Demo

### Demo 1 — Send 200 messages and observe batching
```bash
# Send 200 messages at once
curl -X POST "http://localhost:8095/api/producer/send?count=200"

# Watch logs — you'll see batches of up to 50 records each:
# >>> [BATCH #1] Received 50 records
# >>> [BATCH #1] Committed 50 records in 12ms | first=partition=0 offset=0 last=...
# >>> [BATCH #2] Received 50 records
# ...
```

### Demo 2 — Check batch stats
```bash
curl http://localhost:8095/api/consumer/status
```
Expected after 200 messages with `max-poll-records=50`:
```json
{
  "totalReceived": 200,
  "totalCommitted": 200,
  "batchCount": 4,
  "avgBatchSize": 50.0,
  "recentBatches": [
    { "batchNumber": 1, "size": 50, "processingMs": 12, ... },
    ...
  ]
}
```

### Demo 3 — Variable batch sizes
```bash
# Send 75 messages — not a multiple of 50
curl -X POST "http://localhost:8095/api/producer/send?count=75"
curl http://localhost:8095/api/consumer/status
# avgBatchSize < 50 — first batch ~50, second batch ~25 (partition distribution varies)
```

### Demo 4 — Observe max.poll.records in action
Change `max-poll-records: 50` to `max-poll-records: 10` in `application.yml`. Restart. Send 100 messages. Watch 10 batches of ~10 records each vs 2 batches of ~50. Same messages, more commits, less throughput — the tradeoff is visible.

### Demo 5 — Verify commit lag via Docker CLI
```bash
# Before sending:
docker exec kafka-08-broker kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --describe --group case-15-consumer-group

# Send 100 messages, watch LAG in real time
curl -X POST "http://localhost:8095/api/producer/send?count=100"

# Check again — LAG drops in steps of ~50 as each batch commits
docker exec kafka-08-broker kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --describe --group case-15-consumer-group
```

---

## Verify via Docker CLI

```bash
# Consumer group offset progress
docker exec kafka-08-broker kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --describe --group case-15-consumer-group

# Topic partition info
docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 \
  --describe --topic case-15-events
```

---

## How to Break It (Failure Scenarios)

### Scenario 1 — Crash mid-batch (at-least-once proof)
1. Send 200 messages.
2. While the consumer is processing a batch (watch the logs), kill the app (`Ctrl+C`).
3. Restart — the in-progress batch (at most 50 records) is redelivered. Records from previous committed batches are NOT redelivered.
4. This is at-least-once: anything not committed before the crash is replayed.

### Scenario 2 — max.poll.records too small
Set `max-poll-records: 1`. Send 100 messages. Every record becomes a "batch" of 1. You get 100 commits for 100 records — identical overhead to a single-record listener. The "batch" config has no effect at size 1. This shows that batch processing only pays off when `max-poll-records` is meaningfully large.

### Scenario 3 — max.poll.interval.ms too small
Set `max-poll-interval-ms: 1000` (1 second). Add `Thread.sleep(2000)` in `processRecord()`. Spring Kafka / Kafka will detect that `poll()` wasn't called within the interval, remove the consumer from the group, and trigger a rebalance. Logs: `ConsumerCoordinator - [Consumer clientId=...] Seeking to ... due to group rebalance`. The 1-second timeout expires before the batch processing completes.

### Scenario 4 — Forget to call ack.acknowledge()
Remove `ack.acknowledge()` from `consumeBatch()`. Send messages. Process appears to work in logs, but check the Docker CLI — LAG never decreases. The consumer reads messages but never commits. On restart, all messages are redelivered.

---

## API Reference

| Method | Endpoint | Params | Returns |
|--------|----------|--------|---------|
| GET | `/api/health` | — | `200` string |
| POST | `/api/producer/send` | `count` (default 100) | `200 ProducerResponse` |
| GET | `/api/consumer/status` | — | `200 BatchStatus` |

---

## Configuration Reference

| Property | Value | Notes |
|----------|-------|-------|
| `server.port` | `8095` | Convention: case-N → port 809N |
| `kafka.topic.events` | `case-15-events` | — |
| `kafka.consumer.max-poll-records` | `50` | Batch size ceiling |
| `kafka.consumer.max-poll-interval-ms` | `120000` | 2 min — must be > batch processing time |
| `kafka.consumer.auto-offset-reset` | `earliest` | — |

---

## Learning Checklist

- [ ] Run Demo 1 — confirm batches of ≤50 records appear in logs
- [ ] Run Demo 2 — read `batchCount` and `avgBatchSize` from `/consumer/status`
- [ ] Run Demo 3 — observe variable batch sizes when count is not a multiple of `max-poll-records`
- [ ] Run Scenario 1 — crash mid-batch, observe replay on restart (at-least-once)
- [ ] Run Scenario 4 — remove `ack.acknowledge()`, confirm LAG never clears via Docker CLI
- [ ] Explain why you call `ack.acknowledge()` outside the processing loop, not inside
- [ ] Explain the relationship between `max.poll.records` and `max.poll.interval.ms`
- [ ] Articulate the throughput tradeoff: large batch = fewer commits = higher throughput but larger replay window on failure

---

## See Also

- [THEORY-Q-and-A-SECTION.md](THEORY-Q-and-A-SECTION.md)
- case-08 → offset management (`seek()`, manual commit of specific offsets)
- case-13 → transactions (exactly-once delivery for batch output)
