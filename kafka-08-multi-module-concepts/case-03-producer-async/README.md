# Case 03 — Producer Async

| Field         | Value                                      |
|---------------|--------------------------------------------|
| Module        | `case-03-producer-async`                   |
| Port          | `8083`                                     |
| Topic         | `case-03-topic-async`                      |
| Consumer Group| `case-03-consumer-group`                   |
| Spring Boot   | `4.0.6`                                    |
| Java          | `21`                                       |

---

## What This Case Covers

Three patterns for non-blocking Kafka sends using `CompletableFuture`:

| Pattern | Method | Use Case |
|---------|--------|----------|
| `whenComplete()` callback | Single lambda for success + failure | General production default |
| `thenAccept()` + `exceptionally()` | Split handlers | When success/failure paths have different logic |
| Fire-and-forget (error-only) | Error callback only | Metrics, telemetry, heartbeats |

Key observable difference vs case-02:
- **case-02** → HTTP response arrives **after** broker ACK (blocking)
- **case-03** → HTTP response arrives **before** broker ACK (non-blocking, HTTP 202)

---

## Project Structure

```
case-03-producer-async/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/vbforge/case03/
    │   │   ├── MainApp.java
    │   │   ├── config/
    │   │   │   └── KafkaConfig.java
    │   │   ├── controller/
    │   │   │   └── ProducerController.java
    │   │   ├── model/
    │   │   │   ├── AsyncSendReceipt.java
    │   │   │   └── MyMessageObject.java
    │   │   └── service/
    │   │       ├── ProducerService.java
    │   │       └── ConsumerService.java
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/vbforge/case03/
            └── MainAppTests.java
```

---

## Quick Start

#### 1. Start Kafka (from project root)
```bash
docker compose up -d
```

#### 2. Run this module
```bash
cd case-03-producer-async
mvn spring-boot:run
```

#### 3. Send a message (any pattern)
```bash
# Pattern 1 — whenComplete callback
curl -X POST "http://localhost:8083/api/producer/send-callback?content=Hello+Async"

# Pattern 2 — split handlers
curl -X POST "http://localhost:8083/api/producer/send-split?content=Hello+Split"

# Pattern 3 — fire and forget
curl -X POST "http://localhost:8083/api/producer/send-fire-forget?content=Hello+FireForget"
```

All three return **HTTP 202 Accepted** immediately. Watch the logs — the callback fires **after** the response is returned.

---

## Verify via Docker CLI

```bash
# List messages on the async topic
docker exec kafka-08-broker kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic case-03-topic-async \
  --from-beginning \
  --max-messages 5
```

---

## Observe the Thread Ordering

Run any endpoint and watch your IntelliJ/terminal logs. You'll see this sequence:

```
[HTTP thread]        >>> [CALLBACK] Submitting message ID: abc-123 — returning immediately
[HTTP thread]        HTTP 202 → returned to client
[Kafka I/O thread]   >>> [CALLBACK] Delivered message ID: abc-123 | partition=0 offset=7
[Consumer thread]    ****** Message Received *****
```

The callback and consumer log entries appear **after** the HTTP response — different threads, different timing.

---

## How to Break It (Failure Scenarios)

### Scenario 1 — Stop Kafka mid-send (callback failure path)
```bash
docker compose stop kafka
```
Then send a message. Because `RETRIES_CONFIG = 0`, the send fails immediately. The `whenComplete` and `exceptionally` callbacks will log the error. The HTTP response still comes back 202 (because the failure happens on the I/O thread after the HTTP response was sent).

**Observation:** This is the "silent failure" problem with async sends. The client got 202 but the message never arrived. Solutions: idempotent producer (case-13), a retry table, or outbox pattern.

```bash
docker compose start kafka
```

### Scenario 2 — Compare response timing vs case-02
Run case-02's `/send-with-timeout` and case-03's `/send-callback` side by side. Measure response time in Postman. The case-02 response will be slower (waits for ACK). The case-03 response will be near-instant.

### Scenario 3 — Watch fire-and-forget silence
Stop Kafka, call `/send-fire-forget`. The HTTP call succeeds (202). No error in response. Only the log shows `FAILED silently`. A client with no log access would never know.

---

## API Reference

| Method | Endpoint | Params | Returns |
|--------|----------|--------|---------|
| GET | `/api/producer/health` | — | `200 OK` string |
| POST | `/api/producer/send-callback` | `content` (optional) | `202 AsyncSendReceipt` |
| POST | `/api/producer/send-split` | `content` (optional) | `202 AsyncSendReceipt` |
| POST | `/api/producer/send-fire-forget` | `content` (optional) | `202 AsyncSendReceipt` |

### AsyncSendReceipt response shape
```json
{
  "messageId": "f83c2a1b-...",
  "content": "Hello Async",
  "acceptedAt": "2025-01-15T10:30:00.123",
  "status": "ACCEPTED"
}
```

Note: no `partition` or `offset` in the response — those are only available in the callback (server-side log), not the HTTP response.

---

## Configuration Reference

```yaml
kafka:
  topic:
    async: case-03-topic-async
  consumer:
    groupID: case-03-consumer-group
  default:
    message: Hello from Async Producer! (default message)
```

Key producer config (in `KafkaConfig.java`):

| Config | Value | Why |
|--------|-------|-----|
| `ACKS_CONFIG` | `"1"` | Leader must confirm before callback fires |
| `RETRIES_CONFIG` | `0` | Fail fast for observability in learning |
| `LINGER_MS_CONFIG` | `0` | Send immediately, no batching delay |
| `REQUEST_TIMEOUT_MS_CONFIG` | `10000` | Kafka internal broker timeout |

---

## Learning Checklist

- [ ] Run all three endpoints, observe logs, note the thread names in the callback vs HTTP response
- [ ] Confirm the HTTP response comes back **before** the broker callback log line
- [ ] Stop Kafka mid-send, observe callback error logs — notice the HTTP client still got 202
- [ ] Explain why HTTP 202 is more correct than 200 for async sends
- [ ] Describe the "silent failure" risk of fire-and-forget and when it's acceptable
- [ ] Explain the difference between `whenComplete` and `thenAccept + exceptionally`
- [ ] Explain what `LINGER_MS` does and when you'd raise it in production

---

## See Also

- [THEORY-Q-and-A-SECTION.md](THEORY-Q-and-A-SECTION.md) — deep-dive Q&A for interviews
- case-01 → baseline async (no callbacks, true fire-and-forget)
- case-02 → sync blocking `.get()` for contrast
- case-13 → idempotent producer (safe retries, solves the duplicate problem)
