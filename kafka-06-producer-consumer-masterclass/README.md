# kafka-06-producer-consumer-masterclass

> **Spring Boot 3 · Java 17 · Apache Kafka (KRaft, no ZooKeeper)**

A complete, production-pattern reference for every Kafka **producer** and **consumer** pattern you will encounter in real Java projects. 
Each scenario is self-contained, clearly named, and heavily commented — designed to be read alongside the code.

---

## What This Project Covers

Two sides of Kafka are covered: the **producer** (how messages are sent) and the **consumer** (how messages are received and processed). Each row below is a named scenario with its own endpoint or listener method you can run and observe directly.

### Producer Patterns — tested via Postman / curl

| # | Postman Request | Pattern | Blocks HTTP thread? | Client sees Kafka result? |
|---|-----------------|---------|---------------------|--------------------------|
| P1 | `POST /api/producer/send-sync` | Synchronous send | ✅ Yes — waits for broker ACK | ✅ Yes |
| P2 | `POST /api/producer/send-async-callback` | Async fire-and-forget with callback | ❌ No | ❌ No — result logged in background |
| P3 | `POST /api/producer/send-async-future` | Async with `CompletableFuture` | ❌ No — Spring MVC suspends response | ✅ Yes |
| P4a | `POST /api/producer/send-with-key/priority1` | Keyed send — high priority | ✅ Yes | ✅ Yes |
| P4b | `POST /api/producer/send-with-key/normal` | Keyed send — standard priority | ✅ Yes | ✅ Yes |
| P5a | `POST /api/producer/transactional/success,success,success` | Transactional — all commit | ✅ Yes | ✅ Yes — list of committed events |
| P5b | `POST /api/producer/transactional/success,fail` | Transactional — rollback | ✅ Yes | ❌ No — HTTP 500, nothing committed |
| P6a | `GET /api/consumer/manual?partition=0&offset=0` | Manual consumer — low-level `seek()` + `poll()` | ✅ Yes | ✅ Yes — returns messages as JSON |
| P6b | `GET /api/consumer/manual?partition=1&offset=0` | Manual consumer — different partition | ✅ Yes | ✅ Yes — empty if nothing sent there |

### Consumer Scenarios — observed in application logs

| # | Log prefix | Listener method | What triggers it | Key concept |
|---|------------|-----------------|------------------|-------------|
| C1 | `[GROUP-A]` `[GROUP-B]` | `consumeGeneralGroupA/B` | Any send to `general-topic` | Two independent groups both receive every message (broadcasting) |
| C2 | `[METADATA]` | `consumeWithMetadata` | Any send to `general-topic` | Extracting `@Header` values: offset, partition, group ID |
| C3 | `[REPLAY]` | `consumeFromBeginning` | App startup + any send | `@PartitionOffset(initialOffset="0")` — reads all history from partition 0 |
| C4 | `[PRIORITY-1]` | `consumePriorityOnly` | `send-with-key/priority1` only | `setRecordFilterStrategy` — passes only `key=priority1` |
| C4 | `[OTHER-PRIORITY]` | `consumeNonPriority` | Any key except `priority1` | Inverse filter — skips `priority1`, passes everything else |
| C5 | `[ERROR-HANDLER]` | `consumeWithErrorHandling` | Any send to `priority-topic` | `DefaultErrorHandler` + `FixedBackOff(500ms, 2)` — 3 total attempts then skip |
| C6 | `[TRANSACTIONAL]` | `consumeTransactional` | `transactional/success,...` commit only | `read_committed` isolation — rolled-back messages are never visible |
 
---

## Project Structure

```
kafka-06-producer-consumer-masterclass/
├── docker-compose.yml                          ← Kafka KRaft (no ZooKeeper)
├── kafka-06-masterclass.postman_collection.json
├── pom.xml
└── src/main/
    ├── java/com/vbforge/kafka06/
    │   ├── Kafka06Application.java
    │   ├── config/
    │   │   ├── KafkaTopicConfig.java           ← creates topics via KafkaAdmin
    │   │   ├── KafkaProducerConfig.java         ← standard + transactional KafkaTemplate
    │   │   └── KafkaConsumerConfig.java         ← 5 listener container factories
    │   ├── model/
    │   │   └── MessageEvent.java               ← the event object serialized to JSON
    │   ├── producer/service/
    │   │   └── ProducerService.java            ← all 5 producer patterns
    │   ├── consumer/
    │   │   ├── MessageListener.java            ← all 6 @KafkaListener scenarios
    │   │   └── service/
    │   │       └── ManualConsumerService.java  ← low-level KafkaConsumer API
    │   └── controller/
    │       ├── ProducerController.java         ← REST endpoints → ProducerService
    │       └── ConsumerController.java         ← REST endpoint → ManualConsumerService
    └── resources/
        └── application.yml
```

---

## Prerequisites

| Tool | Version |
|------|---------|
| Java | 17      |
| Maven | 3.9+    |
| Docker Desktop | Latest  |

---

## Quick Start

```bash
# 1. Start Kafka (KRaft mode — single broker, no ZooKeeper)
docker compose up -d

# 2. Run the Spring Boot application
mvn spring-boot:run

# App starts on http://localhost:8080
# Topics are created automatically on startup
```

---

## API Reference

### Producer Endpoints

| Endpoint | Method | Pattern | Blocks thread? | Client sees Kafka result? |
|----------|--------|---------|----------------|--------------------------|
| `/api/producer/send-sync` | POST | Synchronous | ✅ Yes | ✅ Yes |
| `/api/producer/send-async-callback` | POST | Async callback | ❌ No | ❌ No (fire-and-forget) |
| `/api/producer/send-async-future` | POST | Async future | ❌ No | ✅ Yes |
| `/api/producer/send-with-key/{key}` | POST | Keyed | ✅ Yes | ✅ Yes |
| `/api/producer/transactional/{keys}` | POST | Transactional | ✅ Yes | ✅ Yes |

### Consumer Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/consumer/manual?partition=0&offset=0` | GET | Read from specific partition + offset |

---

## Kafka Topics

| Topic | Partitions | Purpose |
|-------|------------|---------|
| `general-topic` | 2 | Sync / async / metadata / replay demos |
| `priority-topic` | 3 | Key-based filtering + error handler demo |
| `transactional-topic` | 3 | Exactly-once transactional demo |

---

## Key Concepts Explained

### Why two KafkaTemplate beans?
A transactional producer must be initialized with a fixed `transactional.id`. 
Mixing transactional and non-transactional sends on the same template causes broker errors. Separate beans — separate factories.

### Why one factory per consumer scenario?
Each `ConcurrentKafkaListenerContainerFactory` is configured differently: isolation level, error handler, filter strategy. 
A `@KafkaListener` picks its factory via `containerFactory = "..."`. Sharing one factory for all scenarios is not possible without compromising configuration.

### Why is `ManualConsumerService` creating a new consumer per request?
`KafkaConsumer` is NOT thread-safe. The original code injected a shared `Consumer` bean — that breaks under concurrent HTTP requests. 
Creating a fresh consumer per request (and closing it via try-with-resources) is correct and safe.

### What does `read_committed` actually do?
Without it: a consumer can read messages that were part of a transaction that later rolled back — a phantom read. 
With `read_committed`, the consumer sees messages only after the transaction is fully committed to the broker.

### How does key-based filtering work?
`setRecordFilterStrategy(record -> condition)` — if the lambda returns `true`, the record is **skipped**. 
If it returns `false`, the record is **passed** to the listener. T
wo factories with inverse conditions = two listeners that never see each other's messages on the same topic.

---

## Running the Scenarios

### P1–P3 — Sync and Async Producer Sends

```bash
# P1: Sync — HTTP responds only after Kafka ACK
# Watch logs: [SYNC] topic=general-topic partition=X offset=Y
# Then: [GROUP-A], [GROUP-B], [METADATA], [REPLAY] all fire
curl -X POST http://localhost:8080/api/producer/send-sync
 
# P2: Async callback — HTTP responds immediately, result logged in background
# Watch logs: [ASYNC-CALLBACK] OK — partition=X offset=Y  (appears after HTTP 200)
curl -X POST http://localhost:8080/api/producer/send-async-callback
 
# P3: Async future — HTTP waits non-blocking, responds with delivery result
# Watch logs: [FUTURE] Delivered to partition=X offset=Y
curl -X POST http://localhost:8080/api/producer/send-async-future
```

### P4 + C4 — Keyed Send and Message Filtering

```bash
# P4a: key=priority1 → only [PRIORITY-1] consumer fires, [OTHER-PRIORITY] is silent
curl -X POST http://localhost:8080/api/producer/send-with-key/priority1
 
# P4b: any other key → only [OTHER-PRIORITY] consumer fires, [PRIORITY-1] is silent
curl -X POST http://localhost:8080/api/producer/send-with-key/normal
```

### P5 + C6 — Transactional Send and Read-Committed Consumer

```bash
# P5a: all keys are "success" → transaction commits → [TRANSACTIONAL] fires 3 times
curl -X POST http://localhost:8080/api/producer/transactional/success,success,success
 
# P5b: "fail" triggers rollback → transaction aborts → [TRANSACTIONAL] fires ZERO times
curl -X POST http://localhost:8080/api/producer/transactional/success,fail
```

### P6 — Manual Consumer (low-level seek + poll)

```bash
# First send a few messages so there is something to read back
curl -X POST http://localhost:8080/api/producer/send-sync
curl -X POST http://localhost:8080/api/producer/send-async-callback
 
# P6a: read all messages in partition 0 starting from offset 0
curl "http://localhost:8080/api/consumer/manual?partition=0&offset=0"
 
# P6b: read partition 1 — will return empty if all messages landed on partition 0
curl "http://localhost:8080/api/consumer/manual?partition=1&offset=0"
```

### C5 — Error Handling with Retry

```bash
# The error-handler listener subscribes to priority-topic and ALWAYS throws.
# Any keyed send triggers it. Watch logs for the retry cycle then clean skip:
# [ERROR-HANDLER] Retry #1 ... Retry #2 ... Retry #3 ... Backoff exhausted
curl -X POST http://localhost:8080/api/producer/send-with-key/normal
```

### C1, C2, C3 — Consumer Group, Metadata, Replay

These fire automatically on every send to `general-topic` (P1, P2, P3).
No separate curl needed — just watch the logs after any of the above sends:

```
[GROUP-A]   → consumeGeneralGroupA   (independent consumer group)
[GROUP-B]   → consumeGeneralGroupB   (second independent group — same messages)
[METADATA]  → consumeWithMetadata    (logs offset, partition, group ID headers)
[REPLAY]    → consumeFromBeginning   (reads from offset 0 on startup and on every send)
```
 
---

## Testing with Postman

Import `kafka-06-masterclass.postman_collection.json` into Postman. Each request has a description explaining what to expect in the HTTP response and application logs.

**Recommended test order:**
1. Run request `1` (sync) — confirms Kafka is up and listeners work
2. Run `2` and `3` — compare async behaviors in logs
3. Run `4a` then `4b` — observe which listener fires for each key
4. Run `5a` then `5b` — compare committed vs rolled-back consumers
5. Run `6a` after step 1 — retrieve the message you sent in step 1

---

## Expected Log Output

```
[SYNC]         topic=general-topic partition=0 offset=0
[GROUP-A]      Received: MessageEvent(category=sync, ...)
[GROUP-B]      Received: MessageEvent(category=sync, ...)
[METADATA]     group=group-metadata partition=0 offset=0
[REPLAY]       From offset 0, partition 0: MessageEvent(...)
 
[PRIORITY-1]   HIGH-PRIORITY message received: ...   ← only for key=priority1
[OTHER-PRIORITY] Standard message received: ...       ← only for non-priority1 keys
 
[ERROR-HANDLER] Retry #1 | topic=priority-topic ...
[ERROR-HANDLER] Retry #2 | topic=priority-topic ...
 
[TRANSACTIONAL] Committed message received: ...       ← only after full commit
```
 
---

## References for additional info

- [Spring Kafka Reference Docs](https://docs.spring.io/spring-kafka/reference/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Confluent cp-kafka Docker image](https://hub.docker.com/r/confluentinc/cp-kafka)

---

## Author

**vbforge** — [github.com/vbforge](https://github.com/vbforge)
