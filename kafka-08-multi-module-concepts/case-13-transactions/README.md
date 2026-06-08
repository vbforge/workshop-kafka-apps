# Case 13 — Transactions (`KafkaTransactionManager` + `@Transactional` + exactly-once semantics)

| Field           | Value                                                    |
|-----------------|----------------------------------------------------------|
| Module          | `case-13-transactions`                                   |
| Port            | `8093`                                                   |
| Orders Topic    | `case-13-orders-topic`                                   |
| Processed Topic | `case-13-processed-topic`                                |
| Probe Topic     | `case-13-probe-topic`                                    |
| Consumer Group  | `case-13-consumer-group`                                 |
| Spring Boot     | `4.0.6`                                                  |
| Java            | `21`                                                     |

---

## What This Case Covers

| Concept | Description |
|---------|-------------|
| **Transactional producer** | `transactional.id` + `enable.idempotence=true` + `acks=all` — the three required producer settings |
| **`KafkaTransactionManager`** | Spring bean that bridges `@Transactional` with Kafka's native transaction protocol |
| **`@Transactional` on Kafka service** | All `kafkaTemplate.send()` calls inside the method are wrapped in one atomic Kafka transaction |
| **Commit path** | Normal method return → `commitTransaction()` → both records visible simultaneously |
| **Rollback path** | `RuntimeException` thrown → `abortTransaction()` → neither record visible despite being sent |
| **`isolation.level=read_committed`** | Consumer only sees records from committed transactions — rolled-back records are invisible |
| **Idempotent producer** | Sequence numbers prevent duplicate records on retry — prerequisite for transactions |
| **Zombie fencing** | `transactional.id` allows the broker to fence zombie producer instances on restart |

---

## Flow Diagram

```
POST /send?rollback=false               POST /send?rollback=true
         │                                        │
         ▼                                        ▼
  TransactionalProducerService           TransactionalProducerService
  .sendOrder(amount, false)              .sendOrder(amount, true)
         │                                        │
         ▼                                        ▼
  case-13-orders-topic               case-13-orders-topic
         │                                        │
         ▼                                        ▼
  OrderConsumerService               OrderConsumerService
  .consume()                         .consume()
         │                                        │
         ▼                                        ▼
  sendCommitted()                    sendRolledBack()
  @Transactional                     @Transactional
         │                                        │
  beginTransaction()             beginTransaction()
         │                                        │
  send → processed-topic         send → processed-topic  (queued, not visible yet)
  send → probe-topic             RuntimeException thrown!
         │                                        │
  commitTransaction()            abortTransaction()
         │                                        │
         ▼                                        ▼
  ✓ Both records visible         ✗ Both records invisible
    to read_committed               (ABORT control batch written)
    consumers
```

---

## Project Structure

```
case-13-transactions/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/vbforge/case13/
    │   │   ├── MainApp.java
    │   │   ├── config/
    │   │   │   └── KafkaConfig.java                  # transactional producer + KafkaTransactionManager
    │   │   ├── controller/
    │   │   │   └── TransactionsController.java
    │   │   ├── model/
    │   │   │   ├── OrderMessage.java
    │   │   │   └── ProducerResponse.java
    │   │   └── service/
    │   │       ├── TransactionalProducerService.java  # @Transactional commit + rollback demos
    │   │       ├── OrderConsumerService.java           # reads orders, triggers transaction
    │   │       └── ProcessedConsumerService.java       # read_committed proof consumer
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/vbforge/case13/
            └── MainAppTests.java
```

---

## Quick Start

```bash
# 1. Start Kafka
docker compose up -d

# 2. Create all three topics
docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 --create \
  --topic case-13-orders-topic --partitions 1 --replication-factor 1

docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 --create \
  --topic case-13-processed-topic --partitions 1 --replication-factor 1

docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 --create \
  --topic case-13-probe-topic --partitions 1 --replication-factor 1

# 3. Run
cd case-13-transactions
mvn spring-boot:run
```

---

## Demo

```bash
# 1. Committed transaction — both records appear on their topics
curl -X POST "http://localhost:8093/api/commit?amount=150.00"
# Logs:
#   >>> [TX-PRODUCER] BEGIN transaction orderId=ORD-XXXXXXXX
#   >>> [TX-PRODUCER] send#1 to=case-13-processed-topic partition=0 offset=0
#   >>> [TX-PRODUCER] send#2 to=case-13-probe-topic partition=0 offset=0
#   >>> [TX-PRODUCER] COMMIT transaction orderId=ORD-XXXXXXXX
#   >>> [PROCESSED-CONSUMER] ✓ Received committed record #1 orderId=ORD-XXXXXXXX

# 2. Rolled-back transaction — NEITHER record appears on any topic
curl -X POST "http://localhost:8093/api/rollback?amount=200.00"
# Response: {"result":"ROLLED_BACK","reason":"Simulated rollback: ..."}
# Logs:
#   >>> [TX-PRODUCER] BEGIN transaction (will rollback) orderId=ORD-YYYYYYYY
#   >>> [TX-PRODUCER] send#1 queued (not yet committed) to=case-13-processed-topic partition=0 offset=1
#   >>> [TX-PRODUCER] Simulated processing failure — about to throw
#   (Spring aborts transaction — no consumer output follows)
# Notice: ProcessedConsumerService does NOT log for this message

# 3. Full flow via orders topic
curl -X POST "http://localhost:8093/api/send?amount=75.00&rollback=false"
curl -X POST "http://localhost:8093/api/send?amount=75.00&rollback=true"

# 4. Verify atomicity
curl http://localhost:8093/api/status
# {
#   "ordersConsumed_committed":  2,
#   "ordersConsumed_rolledBack": 1,
#   "processedTopic_received":   2,
#   "atomicityCheck": "✓ PASS — committed count matches processed-topic received count"
# }
```

---

## Verify via Docker CLI

```bash
# Check what landed in processed-topic — only committed records appear
docker exec kafka-08-broker kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic case-13-processed-topic \
  --from-beginning \
  --max-messages 10

# Check probe-topic — same count as processed-topic (both or neither, atomically)
docker exec kafka-08-broker kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic case-13-probe-topic \
  --from-beginning \
  --max-messages 10

# The aborted records still physically exist in the broker log
# but cannot be consumed with read_committed.
# Check the raw log offset count (includes aborted):
docker exec kafka-08-broker kafka-get-offsets \
  --bootstrap-server localhost:19092 \
  --topic case-13-processed-topic
```

---

## How to Break It (Failure Scenarios)

### Scenario 1 — Remove `isolation.level=read_committed`
In `application.yml`, change `isolationLevel: read_committed` to `isolationLevel: read_uncommitted`.
Restart. Call `POST /rollback`. Watch the `ProcessedConsumerService` logs — it NOW receives
the rolled-back record briefly before the abort marker arrives.
This is the ghost-record problem: you process a record that was never committed.
Change back to `read_committed` to fix.

### Scenario 2 — Remove `KafkaTransactionManager`
Comment out the `kafkaTransactionManager()` bean in `KafkaConfig`.
Call `POST /commit`. Both records appear as expected — BUT they are no longer
sent inside a Kafka transaction. If the broker crashed between send#1 and send#2,
send#1 would be committed and send#2 would be lost. No atomicity.
The `@Transactional` annotation does nothing without the matching `KafkaTransactionManager`.

### Scenario 3 — Change `acks` from `all` to `1`
In `KafkaConfig`, change `ProducerConfig.ACKS_CONFIG` to `"1"`.
Transactions will still work functionally in the single-broker demo.
But with `acks=1`, if the partition leader crashes after acknowledging the commit
but before replication, the committed record is lost. `acks=all` is required for
true durability in production. The broker will actually reject `acks != all` when
`enable.idempotence=true` — try it and watch the startup error.

### Scenario 4 — Duplicate transactional.id in two instances
Start a second instance of the app (change the port to 8094, keep the same `transactional.id`).
The second producer will fence the first. Any in-flight transaction from the first
instance gets aborted by the broker. This is zombie fencing in action — production
protection against split-brain producers.

---

## API Reference

| Method | Endpoint      | Params                                        | Returns                        |
|--------|---------------|-----------------------------------------------|--------------------------------|
| GET    | `/api/health` | —                                             | `200` string                   |
| POST   | `/api/send`   | `amount` (default `99.99`), `rollback` (default `false`) | `200 ProducerResponse` |
| POST   | `/api/commit` | `amount` (default `99.99`)                    | `200 ProducerResponse`         |
| POST   | `/api/rollback` | `amount` (default `99.99`)                  | `500` with rollback details    |
| GET    | `/api/status` | —                                             | `200` atomicity counters       |

---

## Configuration Reference

```yaml
kafka:
  producer:
    transactionalId: case-13-tx-producer   # unique ID — enables Kafka transactional protocol

  consumer:
    isolationLevel: read_committed          # only see records from committed transactions
```

**Three required producer settings for exactly-once:**

| Setting | Value | Why |
|---------|-------|-----|
| `transactional.id` | non-null string | Activates Kafka transaction protocol |
| `enable.idempotence` | `true` | Prevents duplicates on retry (auto-set by transactional.id) |
| `acks` | `all` | Ensures durability — required by idempotence |

---

## Learning Checklist

- [ ] Send one commit + one rollback, then verify `/status` shows atomicityCheck=PASS
- [ ] In logs, find the "send#1 queued (not yet committed)" line — confirm no consumer output follows it for the rollback
- [ ] Explain why `acks=all` is required for transactional producers
- [ ] Explain what `KafkaTransactionManager` does and why `@Transactional` alone is not enough
- [ ] Explain what `isolation.level=read_committed` prevents
- [ ] Change to `read_uncommitted`, send a rollback, and observe the ghost record
- [ ] Explain zombie fencing and why `transactional.id` must be unique per producer instance
- [ ] Describe the consume-process-produce atomic pattern (see THEORY doc Q5)

---

## See Also

- [THEORY-Q-and-A-SECTION.md](THEORY-Q-and-A-SECTION.md)
- case-12 → Global error handler (error recovery infrastructure)
- case-14 → Observability (metrics, health, tracing)
