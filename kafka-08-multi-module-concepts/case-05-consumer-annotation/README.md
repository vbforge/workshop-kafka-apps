# Case 05 — Consumer Annotation (`@KafkaListener` + Auto Commit + Concurrent Consumers)

| Field          | Value                                       |
|----------------|---------------------------------------------|
| Module         | `case-05-consumer-annotation`               |
| Port           | `8085`                                      |
| Topic          | `case-05-topic` (3 partitions)              |
| Consumer Group | `case-05-consumer-group`                    |
| Concurrency    | `3` (one thread per partition)              |
| Spring Boot    | `4.0.6`                                     |
| Java           | `21`                                        |

---

## What This Case Covers

| Concept | Description |
|---------|-------------|
| **`@KafkaListener`** | Declarative consumer wiring — Spring manages the poll loop |
| **`concurrency`** | N consumer threads in the same group, each owning one partition |
| **`AckMode.BATCH`** | Spring commits offsets after each poll batch is processed (not Kafka's native auto-commit) |
| **`ENABLE_AUTO_COMMIT_CONFIG=false`** | Hands offset commit control to Spring Kafka's AckMode |
| **Thread names in logs** | Observable proof that different partitions are processed by different threads |
| **`AtomicInteger`** | Thread-safe counter shared across concurrent listener threads |
| **`ConsumerRecord<K,V>`** | Full record metadata (key, partition, offset) in the listener |

---

## Project Structure

```
case-05-consumer-annotation/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/vbforge/case05/
    │   │   ├── MainApp.java
    │   │   ├── config/
    │   │   │   └── KafkaConfig.java
    │   │   ├── controller/
    │   │   │   └── ProducerController.java
    │   │   ├── model/
    │   │   │   ├── WorkMessage.java
    │   │   │   └── ProducerResponse.java
    │   │   └── service/
    │   │       ├── ProducerService.java
    │   │       └── ConsumerService.java
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/vbforge/case05/
            └── MainAppTests.java
```

---

## Quick Start

#### 1. Start Kafka
```bash
docker compose up -d
```

#### 2. Create topic with 3 partitions
```bash
docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 \
  --create \
  --topic case-05-topic \
  --partitions 3 \
  --replication-factor 1
```

#### 3. Run the module
```bash
cd case-05-consumer-annotation
mvn spring-boot:run
```

#### 4. Fire the bulk send
```bash
curl -X POST "http://localhost:8085/api/producer/send-bulk"
```
This sends 9 messages — 3 per key (`alpha`, `beta`, `gamma`) across all 3 partitions. Watch the logs immediately.

---

## The Concurrency Demo — What to Look For in Logs

After calling `/send-bulk`, you should see log entries like this **interleaved** from different threads:

```
[case-05-consumer-group-0-C-1]  Thread: ...C-1  |  Partition: 0  |  Key: alpha
[case-05-consumer-group-0-C-3]  Thread: ...C-3  |  Partition: 2  |  Key: gamma
[case-05-consumer-group-0-C-2]  Thread: ...C-2  |  Partition: 1  |  Key: beta
[case-05-consumer-group-0-C-1]  Thread: ...C-1  |  Partition: 0  |  Key: alpha
```

Three things to confirm:
1. **Three different thread names** (`C-1`, `C-2`, `C-3`) — three concurrent consumers
2. **Each thread owns one partition** — `C-1` always handles partition 0, etc.
3. **Same key always → same thread** — because same key → same partition → same consumer thread

---

## How to Break It (Failure Scenarios)

### Scenario 1 — concurrency > partition count (idle threads)
Change `concurrency: 3` to `concurrency: 5` in `application.yml`, restart. Kafka can only assign 3 partitions across 5 threads — 2 threads sit idle forever. Check logs: you'll see only 3 thread names processing messages, never 4 or 5.
```bash
# Restore
concurrency: 3
```

### Scenario 2 — concurrency = 1 (sequential consumption)
Change `concurrency: 3` to `concurrency: 1`, restart. Now one thread polls all 3 partitions in round-robin. Send 9 messages — all arrive on `C-1`. No parallelism, no interleaving. This is how most beginners accidentally configure Kafka consumers.

### Scenario 3 — auto-offset-reset = latest (miss historical messages)
Change `auto-offset-reset: earliest` to `latest` in `application.yml`. Stop the app, send 5 messages via `/send-bulk`, then start the app. The consumer starts from the current end of the log — it misses all 5 messages sent while it was down. Switch back to `earliest` to read them.

### Scenario 4 — native auto-commit vs Spring AckMode (the dangerous difference)
In `KafkaConfig`, try changing `ENABLE_AUTO_COMMIT_CONFIG` to `true` and removing the `setAckMode` call. Now Kafka's background timer commits offsets every `auto.commit.interval.ms` (default 5s) regardless of whether your listener processed the message. Add a `Thread.sleep(10_000)` inside `consume()` to simulate slow processing — Kafka commits the offset while your listener is still sleeping. If you crash the app during the sleep, the message is lost (offset committed but not processed).

---

## API Reference

| Method | Endpoint | Params | Returns |
|--------|----------|--------|---------|
| GET | `/api/producer/health` | — | `200` string |
| POST | `/api/producer/send-bulk` | `keys` (opt), `messagesPerKey` (opt, default 3) | `200 ProducerResponse` |
| POST | `/api/producer/send-single` | `key` (required), `content` (opt) | `200 MessageSummary` |

### ProducerResponse shape
```json
{
  "messagesSent": 9,
  "messages": [
    { "messageId": "uuid", "key": "alpha", "partition": 0, "offset": 0 },
    { "messageId": "uuid", "key": "beta",  "partition": 1, "offset": 0 },
    { "messageId": "uuid", "key": "gamma", "partition": 2, "offset": 0 }
  ],
  "sentAt": "2025-01-15T10:30:00.123"
}
```

---

## Configuration Reference

```yaml
kafka:
  topic:
    name: case-05-topic
  consumer:
    groupID: case-05-consumer-group
    concurrency: 3          # consumer threads = partition count for full utilization
    auto-offset-reset: earliest
```

---

## Learning Checklist

- [ ] Run `/send-bulk`, confirm 3 different thread names in the logs
- [ ] Confirm each thread consistently processes the same partition
- [ ] Set `concurrency: 1`, restart, resend — confirm all messages on one thread
- [ ] Set `concurrency: 5`, restart, resend — confirm only 3 threads are active
- [ ] Explain why `ENABLE_AUTO_COMMIT_CONFIG=false` + `AckMode.BATCH` is safer than native auto-commit
- [ ] Explain what `auto-offset-reset: earliest` vs `latest` does and when each applies
- [ ] Explain why `AtomicInteger` is required instead of `int` in a concurrent listener
- [ ] Explain the relationship: `concurrency` ≤ `partition count` for optimal assignment

---

## See Also

- [THEORY-Q-and-A-SECTION.md](THEORY-Q-and-A-SECTION.md)
- case-06 → manual poll + manual commit (you control the poll loop entirely)
- case-07 → consumer groups (multiple groups, rebalancing)
- case-08 → offset management and `seek()`
