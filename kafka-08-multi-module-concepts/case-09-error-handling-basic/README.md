# Case 09 — Error Handling Basic (`DefaultErrorHandler` + Retryable vs Non-Retryable)

| Field          | Value                                          |
|----------------|------------------------------------------------|
| Module         | `case-09-error-handling-basic`                 |
| Port           | `8089`                                         |
| Topic          | `case-09-topic`                                |
| Consumer Group | `case-09-consumer-group`                       |
| Spring Boot    | `4.0.6`                                        |
| Java           | `21`                                           |

---

## What This Case Covers

| Concept | Description |
|---------|-------------|
| **`DefaultErrorHandler`** | Spring Kafka's built-in error handler — retries, classification, recovery |
| **`FixedBackOff`** | Immediate retries with no delay — N retries total |
| **Retryable exceptions** | Exceptions the handler will retry up to `maxAttempts` times |
| **Non-retryable exceptions** | Exceptions that skip straight to recovery — `addNotRetryableExceptions()` |
| **Recovery action** | What happens after all retries exhausted: log + commit past the record |
| **`setRetryListeners()`** | Hook into retry lifecycle — observe attempt numbers in logs |
| **`AckMode.RECORD`** | Commit after each individual record — works with `DefaultErrorHandler` |

---

## Failure Modes

| `failureMode` | Exception thrown | Retried? | Log pattern |
|---------------|-----------------|----------|-------------|
| `none` | — | — | `✓ Processed successfully` |
| `transient` | `TransientProcessingException` | ✓ Yes — up to 3 attempts | `Retry attempt #1`, `#2` → recovery |
| `fatal` | `FatalProcessingException` | ✗ No — straight to recovery | 1 attempt → recovery |
| `npe` | `NullPointerException` | ✗ No — straight to recovery | 1 attempt → recovery |

---

## Project Structure

```
case-09-error-handling-basic/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/vbforge/case09/
    │   │   ├── MainApp.java
    │   │   ├── config/
    │   │   │   └── KafkaConfig.java               # DefaultErrorHandler + FixedBackOff
    │   │   ├── controller/
    │   │   │   └── ErrorHandlingController.java
    │   │   ├── exception/
    │   │   │   ├── TransientProcessingException.java
    │   │   │   └── FatalProcessingException.java
    │   │   ├── model/
    │   │   │   ├── TaskMessage.java
    │   │   │   └── ProducerResponse.java
    │   │   └── service/
    │   │       ├── ProducerService.java
    │   │       └── TaskConsumerService.java
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/vbforge/case09/
            └── MainAppTests.java
```

---

## Quick Start

```bash
docker compose up -d
cd case-09-error-handling-basic
mvn spring-boot:run
```

---

## Demo — Run in This Order

```bash
# 1. Happy path — no errors
curl -X POST "http://localhost:8089/api/send?failureMode=none&content=good-message"
# Logs: >>> [CONSUMER] ✓ Processed successfully

# 2. Transient failure — 3 retry attempts, then recovery
curl -X POST "http://localhost:8089/api/send?failureMode=transient&content=bad-but-retryable"
# Logs:
#   >>> [CONSUMER] ✗ Throwing TransientProcessingException
#   >>> [ERROR-HANDLER] Retry attempt #1 for offset=1
#   >>> [CONSUMER] ✗ Throwing TransientProcessingException  (retry 1)
#   >>> [ERROR-HANDLER] Retry attempt #2 for offset=1
#   >>> [CONSUMER] ✗ Throwing TransientProcessingException  (retry 2)
#   [DefaultErrorHandler] recovery: logged and committed past offset=1

# 3. Fatal failure — NO retries, straight to recovery
curl -X POST "http://localhost:8089/api/send?failureMode=fatal&content=bad-data"
# Logs:
#   >>> [CONSUMER] ✗ Throwing FatalProcessingException
#   [DefaultErrorHandler] recovery: logged and committed past offset=2
#   (no retry lines — skipped immediately)

# 4. NPE — also non-retryable
curl -X POST "http://localhost:8089/api/send?failureMode=npe"

# 5. Prove partition is not blocked after errors
curl -X POST "http://localhost:8089/api/send?failureMode=none&content=after-errors"
# Logs: ✓ Processed — consumer resumed normally

# 6. Check counters
curl http://localhost:8089/api/status
```

---

## How to Break It (Failure Scenarios)

### Scenario 1 — Remove non-retryable classification
In `KafkaConfig.errorHandler()`, comment out `handler.addNotRetryableExceptions(FatalProcessingException.class)`. Send `failureMode=fatal`. Now you'll see 3 retry attempts before recovery — wasted effort for data that can never succeed.

### Scenario 2 — Increase maxAttempts
Change `maxAttempts: 3` to `maxAttempts: 5` in `application.yml`. Send `failureMode=transient`. Watch 4 retry attempts before recovery — longer processing delay per bad message.

### Scenario 3 — What recovery looks like (default)
The default recovery action is: log the error at ERROR level and commit the offset (move past the bad record). No DLT, no re-publishing — just skip and continue. This is acceptable for low-importance data but dangerous for financial events. case-11 adds DLT publishing to the recovery action.

---

## API Reference

| Method | Endpoint | Params | Returns |
|--------|----------|--------|---------|
| GET | `/api/health` | — | `200` string |
| POST | `/api/send` | `failureMode` (default `none`), `content` (opt) | `200 ProducerResponse` |
| GET | `/api/status` | — | `200` counters map |

---

## Configuration Reference

```yaml
kafka:
  error:
    maxAttempts: 3    # 1 original attempt + 2 retries
```

---

## Learning Checklist

- [ ] Run all four failure modes, observe the log difference between transient and fatal
- [ ] Confirm that after any failure, the next `failureMode=none` message processes fine
- [ ] Explain `FixedBackOff(0L, maxAttempts - 1)` — why `maxAttempts - 1`?
- [ ] Explain `addNotRetryableExceptions()` and why it matters for efficiency
- [ ] Explain what the default recovery action does (log + commit past)
- [ ] Explain why `AckMode.RECORD` is appropriate here vs `AckMode.BATCH`
- [ ] Explain what `setRetryListeners()` is for and two production uses of it

---

## See Also

- [THEORY-Q-and-A-SECTION.md](THEORY-Q-and-A-SECTION.md)
- case-10 → retry with `ExponentialBackOff` (realistic production backoff)
- case-11 → Dead Letter Topic (recovery publishes to DLT instead of just logging)
- case-12 → `DefaultErrorHandler` as global handler across all listeners
