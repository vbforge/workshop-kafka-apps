# Case 06 — Consumer Manual Poll (`poll()` + Manual Commit + Pause/Resume + `seek()`)

| Field          | Value                                        |
|----------------|----------------------------------------------|
| Module         | `case-06-consumer-manual-poll`               |
| Port           | `8086`                                       |
| Topic          | `case-06-topic`                              |
| Consumer Group | `case-06-consumer-group`                     |
| Spring Boot    | `4.0.6`                                      |
| Java           | `21`                                         |

---

## What This Case Covers

| Concept | Description |
|---------|-------------|
| **Raw `KafkaConsumer`** | Direct use of `org.apache.kafka.clients.consumer.KafkaConsumer` — no Spring container |
| **Manual poll loop** | You write the `while (running) { poll() → process → commit }` loop yourself |
| **`commitSync()`** | Blocking offset commit — you choose exactly when and what to commit |
| **`pause()` / `resume()`** | Stop/restart fetching from partitions without leaving the consumer group |
| **`seek()`** | Rewind or skip to any offset on any partition at runtime |
| **`WakeupException`** | The thread-safe shutdown signal for a running poll loop |
| **Thread-safety discipline** | `KafkaConsumer` is not thread-safe — all calls must be on the poll thread |
| **AtomicBoolean flags** | Cross-thread signalling pattern: HTTP thread sets flag, poll thread acts on it |

---

## Project Structure

```
case-06-consumer-manual-poll/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/vbforge/case06/
    │   │   ├── MainApp.java
    │   │   ├── config/
    │   │   │   └── KafkaConfig.java             # Producer + consumer properties map (no listener factory)
    │   │   ├── controller/
    │   │   │   └── ConsumerController.java       # pause, resume, seek, status endpoints
    │   │   ├── model/
    │   │   │   ├── WorkMessage.java
    │   │   │   ├── ConsumerStatus.java
    │   │   │   └── ProducerResponse.java
    │   │   └── service/
    │   │       ├── ManualPollConsumerService.java  # ← THE main class
    │   │       └── ProducerService.java
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/vbforge/case06/
            └── MainAppTests.java
```

---

## Quick Start

#### 1. Start Kafka
```bash
docker compose up -d
```

#### 2. Run the module
```bash
cd case-06-consumer-manual-poll
mvn spring-boot:run
```

The consumer loop starts automatically on app startup (via `@PostConstruct`).

---

## Interactive Demo — Run in This Order

```bash
# 1. Load the topic with 12 messages (max-poll-records=5, so you'll see 3 batches)
curl -X POST "http://localhost:8086/api/producer/send?count=12"

# 2. Watch the logs — you'll see poll batches of 5, 5, 2 with manual commit after each
# The consumer processes them immediately since it's already running

# 3. Check consumer status
curl http://localhost:8086/api/consumer/status
# → { "running": true, "paused": false, "totalProcessed": 12, "currentState": "RUNNING" }

# 4. PAUSE the consumer
curl -X POST http://localhost:8086/api/consumer/pause

# 5. Send 5 more messages while paused — they queue up in Kafka
curl -X POST "http://localhost:8086/api/producer/send?count=5"

# 6. Check status — totalProcessed unchanged, Kafka holds the 5 new messages
curl http://localhost:8086/api/consumer/status
# → { "paused": true, "totalProcessed": 12, "currentState": "PAUSED" }

# 7. RESUME — consumer immediately picks up the 5 queued messages
curl -X POST http://localhost:8086/api/consumer/resume

# 8. SEEK partition 0 back to offset 0 — replay all messages from the beginning
curl -X POST "http://localhost:8086/api/consumer/seek?partition=0&offset=0"
# Watch the logs — messages replay from offset 0. totalProcessed climbs again.
```

---

## Verify via Docker CLI

```bash
# Check committed offsets for the consumer group
docker exec kafka-08-broker kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --describe \
  --group case-06-consumer-group
```
After processing and committing, `CURRENT-OFFSET` advances. After a seek + reprocess, you'll see `CURRENT-OFFSET` reset then advance again.

---

## How to Break It (Failure Scenarios)

### Scenario 1 — Crash between process and commit (at-least-once replay)
Add a simulated crash: in `ManualPollConsumerService.pollLoop()`, after `processRecord()` but before `commitSync()`, add `if (totalProcessed.get() == 7) throw new RuntimeException("simulated crash")`. Restart the app — the records from the last uncommitted batch replay from the last committed offset. This demonstrates at-least-once delivery.

### Scenario 2 — Pause backpressure demo
Pause the consumer, then rapidly send 50 messages:
```bash
curl -X POST http://localhost:8086/api/consumer/pause
curl -X POST "http://localhost:8086/api/producer/send?count=50"
```
Check the Docker CLI consumer group — `LAG` climbs to 50 while paused. Resume — lag drains in batches of 5.

### Scenario 3 — seek() as poison pill skip
If a bad message causes processing to fail at a known offset, seek past it:
```bash
# Skip offset 3 on partition 0 — jump directly to offset 4
curl -X POST "http://localhost:8086/api/consumer/seek?partition=0&offset=4"
```

### Scenario 4 — Violating thread safety (DON'T do this in production)
To understand WHY thread safety matters, temporarily call `consumer.pause(...)` directly in the HTTP controller instead of going through the `AtomicBoolean` flag. You'll encounter a `ConcurrentModificationException` or silent data corruption — proving `KafkaConsumer` must only be called from its own thread.

---

## API Reference

| Method | Endpoint | Params | Returns |
|--------|----------|--------|---------|
| GET | `/api/producer/health` | — | `200` string |
| POST | `/api/producer/send` | `count` (default 10) | `200 ProducerResponse` |
| GET | `/api/consumer/status` | — | `200 ConsumerStatus` |
| POST | `/api/consumer/pause` | — | `200` string |
| POST | `/api/consumer/resume` | — | `200` string |
| POST | `/api/consumer/seek` | `partition` (default 0), `offset` (default 0) | `200` string |

### ConsumerStatus shape
```json
{
  "running": true,
  "paused": false,
  "totalProcessed": 17,
  "totalCommitted": 17,
  "currentState": "RUNNING",
  "checkedAt": "2025-01-15T10:30:00.123"
}
```

---

## Configuration Reference

```yaml
kafka:
  topic:
    name: case-06-topic
  consumer:
    groupID: case-06-consumer-group
    auto-offset-reset: earliest
    poll-timeout-ms: 3000     # how long poll() blocks if no records available
    max-poll-records: 5       # max records per poll() — keep small for observability
```

---

## Learning Checklist

- [ ] Run the full interactive demo — observe pause, resume, seek in real time
- [ ] Explain why `KafkaConsumer` is not thread-safe and what the consequence is
- [ ] Explain the `AtomicBoolean` flag pattern for cross-thread signalling
- [ ] Explain why `wakeup()` is used for shutdown instead of calling `close()` directly
- [ ] Explain the difference between `pause()` and unsubscribing from a partition
- [ ] Explain `commitSync(offsetsToCommit)` — why offset N+1, not N?
- [ ] Describe two production scenarios where manual poll is better than `@KafkaListener`
- [ ] Explain what `MAX_POLL_INTERVAL_MS` is and why it matters for manual poll

---

## See Also

- [THEORY-Q-and-A-SECTION.md](THEORY-Q-and-A-SECTION.md)
- case-05 → `@KafkaListener` (Spring manages the loop you wrote here manually)
- case-08 → offset management deep-dive (`seek()` strategies, `earliest`/`latest`/`none`)
