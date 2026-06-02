# Case 04 — Producer Key-Based Partitioning

| Field          | Value                                       |
|----------------|---------------------------------------------|
| Module         | `case-04-producer-key-based`                |
| Port           | `8084`                                      |
| Topic          | `case-04-topic-keyed` (3 partitions)        |
| Consumer Group | `case-04-consumer-group`                    |
| Spring Boot    | `4.0.6`                                     |
| Java           | `21`                                        |

---

## What This Case Covers

| Concept | Description |
|---------|-------------|
| **Key-based partitioning** | Same key always routes to the same partition |
| **Default partitioner** | Kafka's built-in murmur2 hash — deterministic, opaque |
| **Custom partitioner** | `Partitioner` implementation — region-based explicit routing |
| **Null-key send** | No key → sticky/round-robin across partitions, no ordering guarantee |
| **`ConsumerRecord<K,V>`** | Full record metadata in consumer (key, partition, offset) |
| **Dual KafkaTemplate beans** | Two producer factories, two templates, injected by `@Qualifier` |

---

## Project Structure

```
case-04-producer-key-based/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/vbforge/case04/
    │   │   ├── MainApp.java
    │   │   ├── config/
    │   │   │   ├── KafkaConfig.java          # Two producer factories + consumer
    │   │   │   └── CustomPartitioner.java    # Region-based Partitioner impl (Partitioner is an interface)
    │   │   ├── controller/
    │   │   │   └── ProducerController.java
    │   │   ├── model/
    │   │   │   ├── KeyedMessage.java
    │   │   │   └── KeyedSendResult.java
    │   │   └── service/
    │   │       ├── ProducerService.java
    │   │       └── ConsumerService.java
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/vbforge/case04/
            └── MainAppTests.java
```

---

## Quick Start

#### 1. Start Kafka
```bash
docker compose up -d
```

#### 2. Create topic with 3 partitions
The topic auto-creates with 1 partition by default. For this case to be interesting, create it manually with 3 partitions:
```bash
docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 \
  --create \
  --topic case-04-topic-keyed \
  --partitions 3 \
  --replication-factor 1
```
If the topic already exists with 1 partition, delete and recreate:
```bash
docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 \
  --delete --topic case-04-topic-keyed

docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 \
  --create --topic case-04-topic-keyed \
  --partitions 3 --replication-factor 1
```

#### 3. Run this module
```bash
cd case-04-producer-key-based
mvn spring-boot:run
```

---

## The Key Demo — Run These in Order

```bash
# Step 1: Same key 5 times — should return SAME partition every time
curl -X POST "http://localhost:8084/api/producer/send-keyed?key=user-42&content=message1"
curl -X POST "http://localhost:8084/api/producer/send-keyed?key=user-42&content=message2"
curl -X POST "http://localhost:8084/api/producer/send-keyed?key=user-42&content=message3"
# All three responses: "partition": X  — same X every time

# Step 2: Different key — likely different partition
curl -X POST "http://localhost:8084/api/producer/send-keyed?key=user-99&content=other"
# Response: "partition": Y  — likely different from X (depends on hash)

# Step 3: Custom partitioner — predictable region routing
curl -X POST "http://localhost:8084/api/producer/send-custom-partitioner?key=eu-london"
# Response: "partition": 0

curl -X POST "http://localhost:8084/api/producer/send-custom-partitioner?key=us-nyc"
# Response: "partition": 1

curl -X POST "http://localhost:8084/api/producer/send-custom-partitioner?key=asia-tokyo"
# Response: "partition": 2

# Step 4: No key — partition varies (sticky per batch window)
curl -X POST "http://localhost:8084/api/producer/send-no-key?content=no-key-1"
curl -X POST "http://localhost:8084/api/producer/send-no-key?content=no-key-2"
curl -X POST "http://localhost:8084/api/producer/send-no-key?content=no-key-3"
# Partitions will vary across calls
```

---

## Verify via Docker CLI

```bash
# Show messages with their partition numbers
docker exec kafka-08-broker kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic case-04-topic-keyed \
  --from-beginning \
  --property print.partition=true \
  --property print.key=true \
  --max-messages 50
```

Output format: `Partition:N\tkey\tvalue`

---

## How to Break It (Failure Scenarios)

### Scenario 1 — Partition count change breaks key routing
Send 3 messages with `key=user-42`, note the partition. Then increase partitions to 6:
```bash
docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 \
  --alter --topic case-04-topic-keyed --partitions 6
```
Send more messages with `key=user-42`. The partition will change — because `murmur2(key) % 6 ≠ murmur2(key) % 3`. This is the production gotcha: **never increase partition count on a topic with ordering-sensitive keyed data without a migration plan**.

### Scenario 2 — Null key loses ordering
Send 10 messages via `/send-no-key`. Watch the partition field in responses — it varies. Now imagine these are 10 events for the same user: they'll be consumed out of order across different consumer threads.

### Scenario 3 — Custom partitioner with unknown prefix
```bash
curl -X POST "http://localhost:8084/api/producer/send-custom-partitioner?key=unknown-key"
```
The `CustomPartitioner` falls back to hash-based routing. Check the debug log: `CustomPartitioner: key='unknown-key' → default hash partition N`.

---

## API Reference

| Method | Endpoint | Required Params | Returns |
|--------|----------|----------------|---------|
| GET | `/api/producer/health` | — | `200` string |
| POST | `/api/producer/send-keyed` | `key` | `200 KeyedSendResult` |
| POST | `/api/producer/send-custom-partitioner` | `key` | `200 KeyedSendResult` |
| POST | `/api/producer/send-no-key` | — | `200 KeyedSendResult` |

### KeyedSendResult response shape
```json
{
  "messageId": "f83c2a1b-...",
  "key": "user-42",
  "content": "message1",
  "partition": 1,
  "offset": 3,
  "brokerTimestamp": 1736949000000,
  "sendDurationMs": 12,
  "respondedAt": "2025-01-15T10:30:00.123"
}
```

---

## Configuration Reference

```yaml
kafka:
  topic:
    keyed: case-04-topic-keyed
  consumer:
    groupID: case-04-consumer-group
  producer:
    send-timeout-seconds: 5
```

Key producer config (in `KafkaConfig.java`):

| Config | Value | Notes |
|--------|-------|-------|
| Default: `PARTITIONER_CLASS_CONFIG` | *(absent)* | Uses Kafka's DefaultPartitioner (murmur2) |
| Custom: `PARTITIONER_CLASS_CONFIG` | `CustomPartitioner.class` | Region-based routing |

---

## Learning Checklist

- [ ] Send the same key 5 times, confirm same partition in every response
- [ ] Send a different key, observe a different partition
- [ ] Use the custom partitioner with `eu-*`, `us-*`, `asia-*` keys — confirm partitions 0, 1, 2
- [ ] Use the custom partitioner with an unknown key — observe the fallback hash routing in logs
- [ ] Send 6 keyless messages, confirm the partition varies
- [ ] Explain WHY same key → same partition (deterministic hash function)
- [ ] Explain the production danger of increasing partition count on a keyed topic
- [ ] Explain when null-key sends are acceptable
- [ ] Explain why `ConsumerRecord<K,V>` is used instead of `KeyedMessage` directly in the consumer

---

## See Also

- [THEORY-Q-and-A-SECTION.md](THEORY-Q-and-A-SECTION.md)
- case-07 → consumer groups + partition assignment (how partitions map to consumers)
- case-08 → offset management and `seek()` (navigating within partitions)
- case-13 → idempotent producer (safe retries with ordering preserved)
