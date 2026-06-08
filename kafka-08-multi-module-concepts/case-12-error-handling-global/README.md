# Case 12 — Error Handling: Global Error Handler (`DefaultErrorHandler` + shared DLT)

| Field           | Value                                                                             |
|-----------------|-----------------------------------------------------------------------------------|
| Module          | `case-12-error-handling-global`                                                   |
| Port            | `8092`                                                                            |
| Topics          | `case-12-orders-topic` · `case-12-payments-topic` · `case-12-notifications-topic` |
| Shared DLT      | `case-12-global.DLT`                                                              |
| Consumer Groups | `case-12-orders-group` · `case-12-payments-group` · `case-12-notifications-group` · `case-12-dlt-group` |
| Spring Boot     | `4.0.6`                                                                           |
| Java            | `21`                                                                              |

---

## What This Case Covers

| Concept | Description |
|---------|-------------|
| **Global `DefaultErrorHandler`** | Single `@Bean` shared by all container factories — one place to configure retries, non-retryable exceptions, and recovery for every listener in the app |
| **Shared DLT** | All three topics route failed records to ONE shared DLT (`case-12-global.DLT`) — simpler operational surface than per-topic DLTs |
| **`kafka_dlt-original-topic` header** | DLT consumer reads this header to branch on source topic and apply topic-specific handling logic within a single consumer |
| **Factory builder pattern** | `buildFactory(groupId)` helper avoids copy-pasting identical factory configuration three times |
| **Non-retryable exceptions** | `NonRetryableException` registered ONCE on the global handler — applies to all three listeners automatically |
| **Retry listener** | `setRetryListeners()` provides a cross-cutting log of every retry attempt across all topics |
| **When NOT to go global** | If topics have genuinely different retry budgets, separate factories remain the right choice |

---

## Flow Diagram

```
POST /send/orders?failureMode=X
POST /send/payments?failureMode=X         →  ProducerService
POST /send/notifications?failureMode=X

                    ┌──────────────────────────────────┐
                    │         KafkaConfig               │
                    │  globalRecoverer()   ←──────────┐ │
                    │  globalErrorHandler() ←──────┐  │ │
                    │                              │  │ │
                    │  ordersFactory      ──uses──►│  │ │
                    │  paymentsFactory    ──uses──►│  │ │
                    │  notificationsFactory─uses──►│  │ │
                    └──────────────────────────────────┘

OrdersConsumer           PaymentsConsumer       NotificationsConsumer
@KafkaListener           @KafkaListener         @KafkaListener
(ordersContainerFactory) (paymentsFactory)      (notificationsFactory)
        │                       │                       │
   throws ex?              throws ex?              throws ex?
        │                       │                       │
        └───────────────────────┴───────────────────────┘
                                │
                       globalErrorHandler
                                │
                    retries exhausted OR non-retryable?
                                │
                       globalRecoverer()
                                │
                                ▼
                       case-12-global.DLT
                                │
                                ▼
                        GlobalDltConsumer
                  reads kafka_dlt-original-topic header
                  branches: orders → alert order team
                            payments → page finance on-call
                            notifications → low priority log
```

---

## Project Structure

```
case-12-error-handling-global/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/vbforge/case12/
    │   │   ├── MainApp.java
    │   │   ├── config/
    │   │   │   └── KafkaConfig.java               # global handler + buildFactory() + 4 factories
    │   │   ├── controller/
    │   │   │   └── GlobalErrorHandlingController.java
    │   │   ├── exception/
    │   │   │   └── NonRetryableException.java
    │   │   ├── model/
    │   │   │   ├── GenericMessage.java
    │   │   │   └── ProducerResponse.java
    │   │   └── service/
    │   │       ├── ProducerService.java
    │   │       ├── OrdersConsumer.java
    │   │       ├── PaymentsConsumer.java
    │   │       ├── NotificationsConsumer.java
    │   │       └── GlobalDltConsumer.java          # header-based topic routing
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/vbforge/case12/
            └── MainAppTests.java
```

---

## Quick Start

```bash
# 1. Start Kafka
docker compose up -d

# 2. Create all four topics explicitly
docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 --create \
  --topic case-12-orders-topic --partitions 1 --replication-factor 1

docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 --create \
  --topic case-12-payments-topic --partitions 1 --replication-factor 1

docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 --create \
  --topic case-12-notifications-topic --partitions 1 --replication-factor 1

docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 --create \
  --topic case-12-global.DLT --partitions 1 --replication-factor 1

# 3. Run
cd case-12-error-handling-global
mvn spring-boot:run
```

---

## Demo

```bash
# 1. Happy path — all three topics
curl -X POST "http://localhost:8092/api/send/orders?failureMode=none"
curl -X POST "http://localhost:8092/api/send/payments?failureMode=none"
curl -X POST "http://localhost:8092/api/send/notifications?failureMode=none"

# 2. Transient failure — retries fire, then shared DLT receives the record
curl -X POST "http://localhost:8092/api/send/payments?failureMode=transient"
# Logs:
#   >>> [GLOBAL-HANDLER] attempt=1 topic=case-12-payments-topic ...
#   >>> [GLOBAL-HANDLER] attempt=2 topic=case-12-payments-topic ...
#   >>> [GLOBAL-RECOVERER] Routing to DLT | sourceTopic=case-12-payments-topic
#   ╔══ GLOBAL DLT — source: case-12-payments-topic
#   ║  [PAYMENTS-DLT] → URGENT — alert finance team, page on-call
#   ╚══

# 3. Non-retryable — instant DLT, zero retries
curl -X POST "http://localhost:8092/api/send/notifications?failureMode=non-retryable"
# Logs: single attempt → directly DLT → [NOTIFICATIONS-DLT] → log, low priority

# 4. Status — see all three listener counts + DLT breakdown by source topic
curl http://localhost:8092/api/status
# {
#   "successfullyProcessed": { "orders": 1, "payments": 1, "notifications": 1 },
#   "dltReceivedBySourceTopic": {
#     "case-12-payments-topic": 1,
#     "case-12-notifications-topic": 1
#   },
#   "note": "All three listeners share ONE globalErrorHandler bean. ..."
# }
```

---

## Verify via Docker CLI

```bash
# Confirm records landed in the shared DLT with source topic headers
docker exec kafka-08-broker kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic case-12-global.DLT \
  --from-beginning \
  --property print.headers=true \
  --max-messages 10

# Confirm all four topics exist
docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 --list | grep case-12
```

---

## How to Break It (Failure Scenarios)

### Scenario 1 — Prove the global handler is truly global
Change `maxElapsedTimeMs` in `application.yml` from `5000` to `500`. Restart.
Send `failureMode=transient` to any topic — retries exhaust much faster.
**One config change, three topics affected simultaneously.** That's the whole point.
Change it back and observe the difference in retry count in logs.

### Scenario 2 — Add a new listener without touching error config
Add a fourth consumer (e.g. `InvoicesConsumer`) listening to `case-12-invoices-topic`.
Wire its factory: `return buildFactory("case-12-invoices-group")`.
Send a transient failure — it inherits the global error handler instantly.
Zero modifications to `globalErrorHandler()` or `globalRecoverer()`.

### Scenario 3 — Per-topic retry budget (when global is NOT appropriate)
Payments need a 10-second retry budget. Notifications only get 1 second.
To achieve this: remove `notificationsContainerFactory` from `buildFactory()` and
give it its own factory with a separate `DefaultErrorHandler` using a 1s `maxElapsedTime`.
This is the deliberate trade-off: flexibility over uniformity.

### Scenario 4 — DLT source topic routing
Send two transient failures — one to orders, one to payments:
```bash
curl -X POST "http://localhost:8092/api/send/orders?failureMode=transient"
curl -X POST "http://localhost:8092/api/send/payments?failureMode=transient"
```
Both land in `case-12-global.DLT`. Check the `GlobalDltConsumer` logs —
each record shows a different `[ORDERS-DLT]` vs `[PAYMENTS-DLT]` branch.
The shared DLT consumer correctly identifies each record's source via the header.

---

## API Reference

| Method | Endpoint                    | Params                      | Returns             |
|--------|-----------------------------|-----------------------------|---------------------|
| GET    | `/api/health`               | —                           | `200` string        |
| POST   | `/api/send/orders`          | `failureMode` (default `none`) | `200 ProducerResponse` |
| POST   | `/api/send/payments`        | `failureMode` (default `none`) | `200 ProducerResponse` |
| POST   | `/api/send/notifications`   | `failureMode` (default `none`) | `200 ProducerResponse` |
| GET    | `/api/status`               | —                           | `200` counters map  |

**`failureMode` values:**
- `none` → processed cleanly
- `transient` → exhausts retry budget → DLT
- `non-retryable` → instant DLT, zero retries

---

## Configuration Reference

```yaml
kafka:
  retry:
    initialIntervalMs: 300     # first retry delay
    multiplier: 2.0            # each retry doubles the wait
    maxIntervalMs: 2000        # cap at 2s between retries
    maxElapsedTimeMs: 5000     # total retry budget — after this, goes to DLT
```

Changing any of these in one place affects ALL three listeners simultaneously.

---

## Learning Checklist

- [ ] Send `transient` to all three topics and confirm the same retry pattern applies to each
- [ ] Send `non-retryable` and confirm zero retry lines before DLT routing
- [ ] Check `/api/status` — verify DLT count breakdown correctly shows per-source-topic
- [ ] Explain what `buildFactory(groupId)` buys you vs copy-pasting three factory methods
- [ ] Explain why `NonRetryableException` only needs to be registered once here vs case-11
- [ ] Explain when global handler is WRONG (different retry budgets per topic)
- [ ] Add a fourth listener in 5 minutes and confirm it inherits the error config automatically
- [ ] Explain what `kafka_dlt-original-topic` header enables in the DLT consumer

---

## See Also

- [THEORY-Q-and-A-SECTION.md](THEORY-Q-and-A-SECTION.md)
- case-11 → `DeadLetterPublishingRecoverer` and `@DltHandler` (the foundation this case extends)
- case-10 → `ExponentialBackOff` in depth
- case-13 → Kafka transactions and exactly-once semantics
