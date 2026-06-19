# Case 14 — DTO Validation (`Bean Validation / JSR-380 + Custom Validators`)

| Field          | Value                          |
|----------------|--------------------------------|
| Module         | `case-14-validation-dto`       |
| Port           | `8094`                         |
| Events Topic   | `case-14-events`               |
| Rejected Topic | `case-14-rejected`             |
| Consumer Group | `case-14-consumer-group`       |
| Spring Boot    | `4.0.6`                        |
| Java           | `21`                           |

---

## What This Case Covers

| Concept | Description |
|---------|-------------|
| **Bean Validation (JSR-380)** | Constraint annotations on DTO fields: `@NotBlank`, `@Size`, `@Min`, `@Max`, `@DecimalMin`, `@Pattern` |
| **`@Valid` on nested objects** | Recursive validation — constraints inside `DeliveryAddress` and `OrderItem` are validated when `@Valid` is present |
| **Custom `ConstraintValidator`** | `@ValidOrderType` annotation + `OrderTypeValidator` class — validates against an allowed-set of enum-like strings |
| **Programmatic validation** | `Validator.validate(dto)` returns `Set<ConstraintViolation<T>>` — full control, full context |
| **Rejection pattern** | Invalid messages are committed (offset advanced) but published to `case-14-rejected` topic with structured error info |
| **MANUAL ack mode** | Consumer commits only after explicit handling — prevents silent offset advancement on processing errors |

### The Validation Flow

```
Producer sends OrderEventDto JSON → Kafka
                                        ↓
                              @KafkaListener receives
                                        ↓
                     objectMapper.convertValue() → OrderEventDto
                                        ↓
                         validator.validate(dto) → violations
                                    /         \
                              empty            non-empty
                                /                   \
                        processValid()         processInvalid()
                         (log, forward)         (log + publish to
                                                case-14-rejected)
                                    \         /
                                  ack.acknowledge()
                               (offset committed in both cases)
```

---

## Project Structure

```
case-14-validation-dto/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/vbforge/case14/
    │   │   ├── MainApp.java
    │   │   ├── config/
    │   │   │   └── KafkaConfig.java
    │   │   ├── controller/
    │   │   │   └── ValidationController.java
    │   │   ├── model/
    │   │   │   ├── OrderEventDto.java         # ← The main learning artifact
    │   │   │   ├── ProducerResponse.java
    │   │   │   └── ValidationResult.java
    │   │   ├── service/
    │   │   │   ├── ProducerService.java
    │   │   │   └── ValidationConsumerService.java   # ← validates + routes
    │   │   └── validator/
    │   │       ├── ValidOrderType.java         # ← custom annotation
    │   │       └── OrderTypeValidator.java     # ← custom ConstraintValidator
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/vbforge/case14/
            └── MainAppTests.java
```

---

## Quick Start

```bash
docker compose up -d
cd case-14-validation-dto
mvn spring-boot:run
```

---

## Interactive Demo

### Demo 1 — Send a valid order (all constraints pass)
```bash
curl -X POST http://localhost:8094/api/producer/send/valid
```
Expected in logs:
```
>>> [VALID] orderId=ORD-... type=STANDARD amount=149.99 items=2
```

### Demo 2 — Send with invalid order type (custom validator)
```bash
curl -X POST http://localhost:8094/api/producer/send/invalid/bad-order-type
```
Expected in logs:
```
VIOLATION: orderType: orderType must be one of: STANDARD, EXPRESS, BULK, DIGITAL
>>> [REJECTED] orderId=ORD-BADTYPE-... — 1 violation(s)
>>> [REJECTED] Published to topic=case-14-rejected
```

### Demo 3 — Send with bad postal code (@Pattern)
```bash
curl -X POST http://localhost:8094/api/producer/send/invalid/bad-postal-code
```
Expected:
```
VIOLATION: deliveryAddress.postalCode: postalCode must be 2–10 digits
```
Note the nested path `deliveryAddress.postalCode` — Bean Validation walks nested `@Valid` objects and reports the full field path.

### Demo 4 — Multiple violations at once
```bash
curl -X POST http://localhost:8094/api/producer/send/invalid/multiple-violations
```
Expected: All violations are reported simultaneously — Bean Validation collects all failures before returning. You'll see 4+ violation lines.

### Demo 5 — Send a batch
```bash
curl -X POST "http://localhost:8094/api/producer/send/batch?count=10"
```

### Demo 6 — Check consumer stats
```bash
curl http://localhost:8094/api/consumer/stats
# → {"totalReceived":N, "totalAccepted":N, "totalRejected":N}
```

### Demo 7 — Observe the rejected topic
```bash
docker exec kafka-08-broker kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic case-14-rejected \
  --from-beginning
```
You'll see `ValidationResult` JSON objects with the `violations` array.

---

## Verify via Docker CLI

```bash
# Check consumer group lag
docker exec kafka-08-broker kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --describe --group case-14-consumer-group

# Watch both topics
docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 \
  --list | grep case-14
```

---

## How to Break It (Failure Scenarios)

### Scenario 1 — Remove `@Valid` from a nested field
In `OrderEventDto`, remove `@Valid` from the `deliveryAddress` field. Send an order with an invalid postal code. The `@Pattern` violation on `deliveryAddress.postalCode` will NOT appear — Hibernate Validator won't recurse into the object without `@Valid`. The order will pass as valid even with a bad postal code.

### Scenario 2 — Return false for null in OrderTypeValidator
Change `OrderTypeValidator.isValid()` to return `false` when `value == null`. Now every message without an `orderType` field will fail with BOTH `@NotBlank` and `@ValidOrderType` messages. This demonstrates why the null-handling convention exists — constraint validators should be composable, each responsible for one check.

### Scenario 3 — Change ack mode from MANUAL_IMMEDIATE to BATCH
In `KafkaConfig`, change the ack mode to BATCH. Now if the application crashes mid-batch, Kafka may redeliver some already-processed messages. Observe the `totalAccepted` counter jump backwards relative to committed offsets.

### Scenario 4 — Don't commit on rejection
Remove `ack.acknowledge()` from the `violations.isEmpty()` false branch. Send an invalid order. Restart the app. The invalid message will be redelivered forever — it's at the head of the partition and we never advance past it.

---

## API Reference

| Method | Endpoint | Params | Description |
|--------|----------|--------|-------------|
| GET | `/api/health` | — | Liveness check |
| GET | `/api/consumer/stats` | — | Accepted / rejected counters |
| POST | `/api/producer/send/valid` | — | Send one valid order |
| POST | `/api/producer/send/batch` | `count` (default 5) | Send N valid orders |
| POST | `/api/producer/send/invalid/missing-fields` | — | Missing required fields |
| POST | `/api/producer/send/invalid/bad-order-type` | — | Fails `@ValidOrderType` |
| POST | `/api/producer/send/invalid/bad-amount` | — | Negative `totalAmount` |
| POST | `/api/producer/send/invalid/bad-postal-code` | — | Fails `@Pattern` on nested DTO |
| POST | `/api/producer/send/invalid/empty-items` | — | Empty items list |
| POST | `/api/producer/send/invalid/multiple-violations` | — | Several violations at once |

---

## Configuration Reference

| Property | Value | Notes |
|----------|-------|-------|
| `server.port` | `8094` | Convention: case-N → port 809N |
| `kafka.topic.events` | `case-14-events` | Inbound events |
| `kafka.topic.rejected` | `case-14-rejected` | Failed validation sink |
| `kafka.consumer.groupID` | `case-14-consumer-group` | — |
| `kafka.consumer.auto-offset-reset` | `earliest` | Replay on restart |

---

## Learning Checklist

- [ ] Run Demo 1 — confirm a valid order passes all constraints
- [ ] Run Demo 2 — see custom validator fire with a human-readable message
- [ ] Run Demo 3 — see nested path `deliveryAddress.postalCode` in violation output
- [ ] Run Demo 4 — confirm all violations are returned simultaneously (not fail-fast)
- [ ] Run Scenario 1 — remove `@Valid` and confirm nested validation stops working
- [ ] Explain why `OrderTypeValidator.isValid()` returns `true` for null
- [ ] Explain the difference between `@NotNull` and `@NotBlank`
- [ ] Explain why we `ack.acknowledge()` even for rejected messages
- [ ] Read the `case-14-rejected` topic in Docker CLI and inspect the JSON

---

## See Also

- [THEORY-Q-and-A-SECTION.md](THEORY-Q-and-A-SECTION.md)
- case-08 → offset management (seek past bad messages permanently)
- case-11 → Dead Letter Topic (the production-grade rejection pattern)
