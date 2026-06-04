# Case 10 — Error Handling Retry (`ExponentialBackOff` + Recoverable vs Non-Recoverable)

| Field          | Value                                          |
|----------------|------------------------------------------------|
| Module         | `case-10-error-handling-retry`                 |
| Port           | `8090`                                         |
| Topic          | `case-10-topic`                                |
| Consumer Group | `case-10-consumer-group`                       |
| Spring Boot    | `4.0.6`                                        |
| Java           | `21`                                           |

---

## What This Case Covers

| Concept | Description |
|---------|-------------|
| **`ExponentialBackOff`** | Retry intervals that grow geometrically — gives overloaded systems time to recover |
| **`initialInterval`** | First wait between retries (500ms) |
| **`multiplier`** | Each interval = previous × multiplier (2.0 = doubling) |
| **`maxInterval`** | Cap on any single interval — prevents waits growing unbounded (4000ms) |
| **`maxElapsedTime`** | Total retry budget in wall-clock ms — stop retrying after this (10000ms) |
| **Eventually-succeed pattern** | Message fails N times then succeeds — the canonical transient error scenario |
| **Retry budget exhaustion** | What happens when `maxElapsedTime` runs out — recovery action runs |

---

## Backoff Schedule (with default config)

```
Attempt 1 (original) → FAIL
  wait 500ms
Attempt 2 (retry 1) → FAIL
  wait 1000ms   (500 × 2.0)
Attempt 3 (retry 2) → FAIL
  wait 2000ms   (1000 × 2.0)
Attempt 4 (retry 3) → FAIL
  wait 4000ms   (2000 × 2.0, capped at maxInterval)
Total elapsed ≈ 7500ms < 10000ms → one more retry possible
Attempt 5 (retry 4) → FAIL
Total elapsed ≈ 11500ms > 10000ms → BUDGET EXHAUSTED → recovery
```

---

## Project Structure

```
case-10-error-handling-retry/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/vbforge/case10/
    │   │   ├── MainApp.java
    │   │   ├── config/
    │   │   │   └── KafkaConfig.java               # ExponentialBackOff + DefaultErrorHandler
    │   │   ├── controller/
    │   │   │   └── RetryController.java
    │   │   ├── exception/
    │   │   │   └── NonRetryableException.java
    │   │   ├── model/
    │   │   │   ├── TaskMessage.java                # includes succeedOnAttempt
    │   │   │   └── ProducerResponse.java
    │   │   └── service/
    │   │       ├── ProducerService.java
    │   │       └── RetryConsumerService.java       # tracks per-message attempt counts
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/vbforge/case10/
            └── MainAppTests.java
```

---

## Quick Start

```bash
docker compose up -d
cd case-10-error-handling-retry
mvn spring-boot:run
```

---

## Demo — Three Scenarios

### Scenario 1: Eventually Succeed (the key demo)
```bash
# Fails attempts 1 and 2, succeeds on attempt 3
curl -X POST "http://localhost:8090/api/send/eventually-succeed?succeedOnAttempt=3"
```
Watch logs — you'll see:
```
attempt=1 FAILING → [BACKOFF] attempt=1 waiting...
(500ms gap)
attempt=2 FAILING → [BACKOFF] attempt=2 waiting...
(1000ms gap)
attempt=3 ✓ SUCCEEDED
```
The partition resumes processing immediately after success.

### Scenario 2: Always Fail (budget exhaustion)
```bash
curl -X POST "http://localhost:8090/api/send/always-fail"
```
Watch logs — retries with growing gaps until ~10s total, then DefaultErrorHandler recovery runs.

### Scenario 3: Non-Retryable (instant skip)
```bash
curl -X POST "http://localhost:8090/api/send/non-retryable"
```
Logs show attempt=1 then immediately recovery — no backoff waits at all.

### Compare timing side by side:
```bash
# Send all three back to back, note log timestamps between attempts
curl -X POST "http://localhost:8090/api/send/non-retryable"         # instant
curl -X POST "http://localhost:8090/api/send/eventually-succeed?succeedOnAttempt=2"  # ~500ms
curl -X POST "http://localhost:8090/api/send/always-fail"           # ~10 seconds
```

---

## How to Break It (Failure Scenarios)

### Scenario 1 — Reduce maxElapsedTime to see fewer retries
Change `maxElapsedTimeMs: 10000` to `maxElapsedTimeMs: 1500`. Now only attempt 1 + retry 1 (500ms wait) fit within the budget — retry 2 (would need another 1000ms) is over budget. Recovery fires after just 2 attempts.

### Scenario 2 — Set succeedOnAttempt beyond retry budget
```bash
curl -X POST "http://localhost:8090/api/send/eventually-succeed?succeedOnAttempt=99"
```
The message never reaches attempt 99 — the retry budget runs out first. DefaultErrorHandler recovery runs instead of success. Demonstrates that `maxElapsedTime` is the hard ceiling regardless of intent.

### Scenario 3 — Multiplier = 1.0 (becomes FixedBackOff)
Change `multiplier: 2.0` to `multiplier: 1.0`. Now all retry intervals equal `initialIntervalMs` — identical to `FixedBackOff(500ms, N)`. Observe flat timing in logs vs the growing gaps with multiplier=2.0.

---

## API Reference

| Method | Endpoint | Params | Returns |
|--------|----------|--------|---------|
| GET | `/api/health` | — | `200` string |
| POST | `/api/send/always-fail` | `content` (opt) | `200 ProducerResponse` |
| POST | `/api/send/eventually-succeed` | `content` (opt), `succeedOnAttempt` (default 3) | `200 ProducerResponse` |
| POST | `/api/send/non-retryable` | `content` (opt) | `200 ProducerResponse` |
| GET | `/api/status` | — | `200` counters + delivery attempt map |

---

## Configuration Reference

```yaml
kafka:
  retry:
    initialIntervalMs: 500    # first wait between retries
    multiplier: 2.0           # each interval = previous × multiplier
    maxIntervalMs: 4000       # cap per interval
    maxElapsedTimeMs: 10000   # total retry budget
```

---

## Learning Checklist

- [ ] Run `eventually-succeed?succeedOnAttempt=3` and observe growing gaps in logs
- [ ] Run `always-fail` and observe budget exhaustion after ~4-5 attempts
- [ ] Run `non-retryable` and confirm instant recovery with no backoff gaps
- [ ] Explain the four `ExponentialBackOff` parameters and what each controls
- [ ] Calculate the backoff schedule for `initial=100ms, multiplier=3.0, max=5000ms`
- [ ] Explain why `maxElapsedTime` is more production-safe than a fixed retry count
- [ ] Explain why exponential backoff is better than fixed backoff for overloaded services
- [ ] Explain the `succeedOnAttempt > retry budget` scenario and its outcome

---

## See Also

- [THEORY-Q-and-A-SECTION.md](THEORY-Q-and-A-SECTION.md)
- case-09 → `FixedBackOff` + retryable vs non-retryable classification (foundation)
- case-11 → Dead Letter Topic (what happens AFTER retry budget exhausted in production)
- case-12 → Global `DefaultErrorHandler` across all listeners
