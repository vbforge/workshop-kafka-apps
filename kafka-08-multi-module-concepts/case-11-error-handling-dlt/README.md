# Case 11 — Error Handling: Dead Letter Topic (`DeadLetterPublishingRecoverer` + `@DltHandler`)

| Field          | Value                                              |
|----------------|----------------------------------------------------|
| Module         | `case-11-error-handling-dlt`                       |
| Port           | `8091`                                             |
| Main Topic     | `case-11-topic`                                    |
| DLT Topic      | `case-11-topic.DLT`                                |
| Consumer Groups| `case-11-consumer-group` · `case-11-dlt-consumer-group` |
| Spring Boot    | `4.0.6`                                            |
| Java           | `21`                                               |

---

## What This Case Covers

| Concept | Description |
|---------|-------------|
| **`DeadLetterPublishingRecoverer`** | Replaces "log and skip" recovery — publishes failed record to DLT with diagnostic headers |
| **DLT naming convention** | `<original-topic>.DLT` — Spring's default; configurable via destination function |
| **`kafka_dlt-*` headers** | Diagnostic headers added automatically: original topic, partition, offset, exception class, message, stacktrace |
| **DLT consumer** | Separate `@KafkaListener` on the DLT topic — separate group, no error handler |
| **`@DltHandler`** | Alternative annotation for handling DLT records inline with the main listener |
| **Infinite loop prevention** | DLT factory has NO error handler — DLT processing failures log and move on |
| **Destination function** | `(record, exception) -> TopicPartition` — controls where failed records land |

---

## Flow Diagram

```
Producer → case-11-topic
                │
                ▼
        OrderConsumerService
        @KafkaListener
                │
        throws exception?
           │         │
          NO        YES
           │         │
           ▼         ▼
      ✓ commit   DefaultErrorHandler
                      │
               retries exhausted OR
               non-retryable exception
                      │
                      ▼
          DeadLetterPublishingRecoverer
                      │
                      ▼
           case-11-topic.DLT  ← enriched with kafka_dlt-* headers
                      │
                      ▼
            DltConsumerService
            @KafkaListener (dltContainerFactory)
            logs headers → alert → persist → (optionally) replay
```

---

## Project Structure

```
case-11-error-handling-dlt/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/vbforge/case11/
    │   │   ├── MainApp.java
    │   │   ├── config/
    │   │   │   └── KafkaConfig.java            # DeadLetterPublishingRecoverer + 2 factories
    │   │   ├── controller/
    │   │   │   └── DltController.java
    │   │   ├── exception/
    │   │   │   └── NonRetryableOrderException.java
    │   │   ├── model/
    │   │   │   ├── OrderMessage.java
    │   │   │   └── ProducerResponse.java
    │   │   └── service/
    │   │       ├── ProducerService.java
    │   │       ├── OrderConsumerService.java    # main topic listener
    │   │       └── DltConsumerService.java      # DLT listener — reads kafka_dlt-* headers
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/vbforge/case11/
            └── MainAppTests.java
```

---

## Quick Start

```bash
docker compose up -d

# Create both topics explicitly
docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 --create \
  --topic case-11-topic --partitions 1 --replication-factor 1

docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 --create \
  --topic case-11-topic.DLT --partitions 1 --replication-factor 1

cd case-11-error-handling-dlt
mvn spring-boot:run
```

---

## Demo

```bash
# 1. Happy path
curl -X POST "http://localhost:8091/api/send?failureMode=none"
# Logs: ✓ Processed order=ORD-XXXX

# 2. Transient failure → retries → DLT
curl -X POST "http://localhost:8091/api/send?failureMode=transient"
# Logs:
#   >>> [ERROR-HANDLER] attempt=1 ...
#   >>> [ERROR-HANDLER] attempt=2 ...
#   >>> [DLT-RECOVERER] Publishing to DLT | topic=case-11-topic partition=0 offset=1
#   ╔══ DLT RECORD #1 RECEIVED
#   ║  orderId: ORD-XXXX
#   ║  kafka_dlt-original-topic: case-11-topic
#   ║  kafka_dlt-original-offset: 1
#   ║  kafka_dlt-exception-fqcn: java.lang.RuntimeException
#   ╚══

# 3. Non-retryable → instant DLT (no retry lines)
curl -X POST "http://localhost:8091/api/send?failureMode=non-retryable&amount=-50"
# Logs: single attempt → immediately DLT

# 4. Check status
curl http://localhost:8091/api/status
```

---

## Verify via Docker CLI

```bash
# Confirm records landed in the DLT
docker exec kafka-08-broker kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic case-11-topic.DLT \
  --from-beginning \
  --property print.headers=true \
  --max-messages 5
```

The `--print-headers` flag shows all `kafka_dlt-*` headers inline with the record value.

---

## How to Break It (Failure Scenarios)

### Scenario 1 — DLT-of-DLT infinite loop (deliberately broken, then fixed)
Temporarily add `factory.setCommonErrorHandler(errorHandler())` to `dltContainerFactory()`. Send a `failureMode=transient` order. Watch logs — the DLT consumer fails too, which publishes to `case-11-topic.DLT.DLT`, which the DLT consumer tries to read... Remove the error handler from the DLT factory to fix it.

### Scenario 2 — Missing DLT topic
Drop the DLT topic then send a failing message:
```bash
docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 --delete --topic case-11-topic.DLT
```
`DeadLetterPublishingRecoverer` will throw `UnknownTopicOrPartitionException` — the main consumer gets stuck. Re-create the DLT topic to recover. In production: pre-create DLT topics, don't rely on auto-creation.

### Scenario 3 — DLT lag monitoring
Send 20 `failureMode=transient` messages in quick succession, then stop the app before the DLT consumer processes them:
```bash
for i in {1..20}; do curl -X POST "http://localhost:8091/api/send?failureMode=transient"; done
```
Check DLT lag:
```bash
docker exec kafka-08-broker kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --describe --group case-11-dlt-consumer-group
```
LAG on the DLT topic is your "failed message backlog" — a key production alert metric.

---

## API Reference

| Method | Endpoint | Params | Returns |
|--------|----------|--------|---------|
| GET | `/api/health` | — | `200` string |
| POST | `/api/send` | `failureMode` (default `none`), `amount` (default `99.99`) | `200 ProducerResponse` |
| GET | `/api/status` | — | `200` counters map |

---

## Learning Checklist

- [ ] Send `transient` and observe the full flow: retries → DLT → DLT consumer headers
- [ ] Send `non-retryable` and confirm no retry lines before DLT routing
- [ ] Use Docker CLI `--print-headers` to see `kafka_dlt-*` headers on the raw record
- [ ] Explain why the DLT factory has NO error handler
- [ ] Explain the destination function and when you'd route to different DLT partitions
- [ ] Explain what `@DltHandler` is and how it differs from a separate `@KafkaListener`
- [ ] Explain why DLT lag is a critical production alert metric
- [ ] Describe the replay path: how would you re-process DLT records after a code fix?

---

## See Also

- [THEORY-Q-and-A-SECTION.md](THEORY-Q-and-A-SECTION.md)
- case-09 → `DefaultErrorHandler` basics (the foundation this case builds on)
- case-10 → `ExponentialBackOff` (the retry policy used before DLT routing)
- case-12 → Global error handler (configure once for all listeners)
