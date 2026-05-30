# Case 02: Synchronous Producer

| Property | Value |
|----------|-------|
| **Module** | `case-02-producer-sync` |
| **Parent** | `kafka-08-multi-module-concepts` |
| **Java** | 21 |
| **Spring Boot** | 4.0.6 |
| **Kafka** | cp-kafka 8.0.0 (KRaft mode) |
| **App port** | 8082 |

---

* [Test with HTTP Requests](#test-with-http-requests)
    * [**Health check:**](#health-check)
    * [**Pattern 1 — Blocking send (no timeout):**](#pattern-1--blocking-send-no-timeout)
    * [**Pattern 2 — Bounded blocking (configured timeout from `application.yml`):**](#pattern-2--bounded-blocking-configured-timeout-from-applicationyml)
    * [**Pattern 3 — Custom timeout per call:**](#pattern-3--custom-timeout-per-call)

---

## 📖 What This Case Covers

- **Blocking send** with `Future.get()` — thread halts until broker ACK
- **Bounded blocking** with `Future.get(timeout, unit)` — the correct production pattern
- **Caller-controlled timeout** — different priorities, different deadlines
- `RecordMetadata` inspection — partition, offset, broker timestamp returned in response
- `ExecutionException` vs `TimeoutException` — two distinct failure modes
- `ACKS_CONFIG`, `REQUEST_TIMEOUT_MS_CONFIG`, `RETRIES_CONFIG` — producer config knobs
- Why `RETRIES=0` makes sense in a learning environment (want to see failures immediately, not have Kafka silently retry and mask them)

> **The core contrast with case-01:** In case-01, `kafkaTemplate.send()` returned immediately —
> we had no idea whether Kafka accepted the message at response time.
> In case-02, we call `.get()` which **blocks the calling thread** until the broker confirms
> the write. The HTTP response only goes back to the client *after* Kafka has committed the message.
> You can actually see the partition and offset in the response — that's broker-confirmed delivery.

---

## 📁 Project Structure

```
case-02-producer-sync/
├── src/main/java/com/vbforge/case02/
│   ├── config/
│   │   └── KafkaConfig.java                  # Producer/Consumer configuration
│   ├── controller/
│   │   └── ProducerController.java           # REST endpoints (3 sync patterns)
│   ├── model/
│   │   ├── MyMessageObject.java                      # DTO with id, content, timestamp
│   │   └── SendResultMetadata.java                   # Response DTO with broker metadata
│   ├── service/
│   │   ├── ConsumerService.java              # @KafkaListener (proves delivery)
│   │   └── ProducerService.java              # All three blocking send patterns
│   └── MainApp.java
├── src/main/resources/
│   └── application.yml
├── src/test/java/
│   └── MainAppTests.java
├── pom.xml
├── README.md
└── THEORY-Q-and-A-SECTION.md
```

---

## 🚀 Quick Start

### Step 1: Start Kafka
From the **project root** (`kafka-08-multi-module-concepts/`):
```bash
docker compose up -d
```

### Step 2: Build & Run
```bash
cd case-02-producer-sync
mvn clean install
mvn spring-boot:run
```

Expected output:
```
Started MainApp in X.XXX seconds
```

## Test with HTTP Requests

> Note: all requests are in: [check and run http requests](src/main/java/com/vbforge/case02/requests)

### **Health check:**
```bash
GET http://localhost:8082/api/producer/health
```
Response: `"Sync Producer is Running!"`

---

### **Pattern 1 — Blocking send (no timeout):**

- Normal Flow — Success with ACK
- **Goal:** Verify that a message is successfully sent, the broker acknowledges it, and RecordMetadata is returned.

```bash
POST http://localhost:8082/api/producer/send-blocking
POST http://localhost:8082/api/producer/send-blocking?content=My sync message
```
Response:
```json
{
  "message": {
    "id": "a1b2c3d4-...",
    "content": "My sync message",
    "timestamp": "2024-01-15T10:30:45.123"
  },
  "partition": 0,
  "offset": 0,
  "brokerTimestamp": 1705315845123,
  "sendDurationMs": 14,
  "respondedAt": "2024-01-15T10:30:45.137"
}
```

> Notice: `sendDurationMs` shows how long the thread was blocked. On a local Kafka it's ~5-20ms.
> In production with network latency it can be 50-200ms per message — that cost adds up fast.

**What happens under the hood:**
1) Producer creates MyMessageObject with unique ID
2) kafkaTemplate.send() returns ListenableFuture<SendResult>
3) .get() blocks the thread until broker ACK arrives
4) SendResult provides RecordMetadata with partition, offset, timestamp
5) Custom SendResultMetadata is returned to the client

```text
.get() with no timeout arguments blocks INDEFINITELY.
This is dangerous in production — if Kafka is slow or down, your HTTP request thread hangs forever (or until the server kills it).
We expose this in the API so you can see what "naked blocking" looks like.
In reality: NEVER use this in production. Always pass a timeout.
```

---

### **Pattern 2 — Bounded blocking (configured timeout from `application.yml`):**

- Bounded Timeout — Production-Safe Blocking
- **Goal:** Demonstrate the correct production pattern — waiting for broker ACK but with an upper time bound so the thread never hangs indefinitely.
- **Timeout configured:** `${kafka.producer.send-timeout-seconds}=5` (5 seconds max wait)

```bash
POST http://localhost:8082/api/producer/send-with-timeout
POST http://localhost:8082/api/producer/send-with-timeout?content=Safe sync message
```
Same response shape as pattern 1.

**What happens under the hood:**
1) Producer creates `MyMessageObject` with unique ID
2) `kafkaTemplate.send()` returns `ListenableFuture<SendResult>`
3) `.get(5, TimeUnit.SECONDS)` blocks but **only for max 5 seconds**
4) If ACK arrives in time → proceed normally with `RecordMetadata`
5) If no ACK after 5 seconds → `TimeoutException` is thrown immediately

**Critical distinction from Scenario 1:**

| Aspect | Scenario 1 (`.get()`) | Scenario 2 (`.get(timeout)`) |
|--------|----------------------|------------------------------|
| **Max wait time** | Infinite (forever) | Fixed (5 seconds) |
| **Thread safety** | ❌ Dangerous | ✅ Production-ready |
| **Exception on timeout** | Never occurs | `TimeoutException` |
| **Use case** | Learning only | Real applications |

```text
⚠️ IMPORTANT: On TimeoutException, we DON'T know if the message was written to Kafka!
The broker might have:
  a) Never received it (network issue)
  b) Received but was too slow to ACK (overloaded)
  c) Written successfully but ACK got lost

This is why idempotent producers and unique message IDs matter — 
consumers must handle possible duplicates after timeouts.
```

**Why this pattern is production-safe:**
- HTTP thread won't hang forever if Kafka is down
- can return a meaningful error to the client within seconds
- Thread pool won't exhaust due to stuck requests
- Timeout value can be tuned per business requirement

**When to increase/decrease timeout:**
- **Lower timeout (1-2s):** Fast failures for real-time UI, health checks
- **Higher timeout (10-30s):** Batch jobs, non-critical async processing
- **Very high timeout (60s+):** Rare — usually indicates infrastructure problem

**Comparison between Scenario 1 and 2:**

```
Scenario 1 (.get()):
Thread → [---BLOCKS FOREVER---] → (never returns if broker dead)
         ↑
         DANGEROUS — thread leak!

Scenario 2 (.get(5, SECONDS)):
Thread → [--wait max 5s--] → ACK (23ms) ✓
         or
Thread → [--wait max 5s--] → TIMEOUT (5000ms) ✗
         ↑
         SAFE — thread always unblocks
```

---

### **Pattern 3 — Custom timeout per call:**

- Custom Timeout — Caller-Controlled Deadlines
- **Goal:** Demonstrate per-request timeout control — different API clients can specify their own timeout based on their SLA requirements.
- **Timeout passed:** Caller decides — 2 seconds in this example (could be 1s for real-time UI or 30s for batch jobs)

```bash
POST http://localhost:8082/api/producer/send-custom-timeout?content=Priority message&timeoutSeconds=10
POST http://localhost:8082/api/producer/send-custom-timeout?content=Low priority&timeoutSeconds=1
```
Same response shape. Experiment with `timeoutSeconds=1` on a slow system to trigger a timeout.

**What happens under the hood:**

1) Producer creates `MyMessageObject` with unique ID
2) `kafkaTemplate.send()` returns `ListenableFuture<SendResult>`
3) `.get(timeoutSeconds, TimeUnit.SECONDS)` uses the **caller-provided** timeout value
4) Different from Scenario 2 — timeout isn't fixed in config, it's dynamic per request
5) Each caller can specify their own deadline based on business priority

**How this differs from Scenario 2:**

| Aspect | Scenario 2 (`Send With Timeout`) | Scenario 3 (`Send With Custom Timeout`) |
|--------|----------------------------------|-----------------------------------------|
| **Timeout source** | Fixed in `application.yml`       | Passed as method parameter              |
| **Flexibility** | ❌ Same for all callers           | ✅ Per-request control                   |
| **Use case** | One-size-fits-all timeout        | Different SLAs per client               |
| **API design** | No parameter needed              | Caller provides timeout                 |

```text
🎯 REAL-WORLD USE CASE EXAMPLES:

┌─────────────────────────────────────────────────────────────────┐
│  Critical Payment Processing (5s timeout)                       │
│  POST /api/producer/send-custom-timeout?timeoutSeconds=5        │
│  → Must know success/failure quickly, can't leave user waiting  │
├─────────────────────────────────────────────────────────────────┤
│  Analytics Logging (30s timeout)                                │
│  POST /api/producer/send-custom-timeout?timeoutSeconds=30       │
│  → Can tolerate longer waits, less business critical            │
├─────────────────────────────────────────────────────────────────┤
│  Health Check (1s timeout)                                      │
│  POST /api/producer/send-custom-timeout?timeoutSeconds=1        │
│  → Fast failure to detect broker issues immediately             │
├─────────────────────────────────────────────────────────────────┤
│  Batch Processing (60s timeout)                                 │
│  POST /api/producer/send-custom-timeout?timeoutSeconds=60       │
│  → Background job, willing to wait longer for success           │
└─────────────────────────────────────────────────────────────────┘
```

**Important considerations for custom timeouts:**

```text
⚠️ VALIDATION NEEDED IN PRODUCTION:

public SendResultMetadata sendWithCustomTimeout(String content, int sendTimeoutSeconds) {
    // Protect against abuse
    if (sendTimeoutSeconds < 1) {
        throw new IllegalArgumentException("Timeout must be at least 1 second");
    }
    if (sendTimeoutSeconds > 60) {
        throw new IllegalArgumentException("Timeout cannot exceed 60 seconds");
    }
    // ... rest of method
}
```

**Why custom timeouts matter:**

- **User experience:** Real-time UI needs fast failure (2-3s max)
- **Cost control:** Prevent one slow client from exhausting thread pool
- **Multi-tenancy:** VIP customers get higher timeouts than free tier
- **Degraded mode:** Reduce timeouts when system is under stress
- **Testing:** Easily simulate timeout behavior without reconfiguring

**Testing the timeout boundary:**

```bash
# Should succeed (normal broker response within 5 seconds)
curl -X POST "http://localhost:8082/api/producer/send-custom-timeout?content=Hello from Kafka (send-with-custom-timeout)!&timeoutSeconds=5"
# POST 

# Should timeout (if broker takes >1 second to ACK)
curl -X POST "http://localhost:8082/api/producer/send-custom-timeout?content=Hello from Kafka (send-with-custom-timeout)!&timeoutSeconds=1"

# Should be rejected by validation (if implemented)
curl -X POST "http://localhost:8082/api/producer/send-custom-timeout?content=Hello from Kafka (send-with-custom-timeout)!&timeoutSeconds=0"
```

---

## Verify Consumer Output

After any send, check the application logs:
```
****** Message Received *****
 * ID:        c29c9d73-ac88-44cf-a718-13a101314c0c
 * Content:   {message is respected by request content}
 * Timestamp: 2026-05-30T13:26:44.965345400
******************************
```

**Key observation:** The consumer log appears *almost simultaneously* with the HTTP response.
With async (case-01), the response came back before the consumer had processed it.
With sync, the message is committed by the time you see the HTTP response — consumer fires right after.

---

## 🐳 Verify via Docker CLI

```bash
# List topics
docker exec kafka-08-broker kafka-topics --list --bootstrap-server localhost:9092

# Describe the sync topic (check partition count, offsets)
docker exec kafka-08-broker kafka-topics \
  --describe --topic case-02-topic-sync --bootstrap-server localhost:9092

# Watch messages live
docker exec -it kafka-08-broker kafka-console-consumer \
  --topic case-02-topic-sync \
  --from-beginning \
  --bootstrap-server localhost:9092

# Check consumer group lag (should be 0 if app is running)
docker exec kafka-08-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group case-02-consumer-group
```

---

## 🔥 How to Break It (Failure Scenarios)

### Scenario 1: Kafka not running → TimeoutException
```bash
docker compose down
mvn spring-boot:run
POST http://localhost:8082/api/producer/send-with-timeout
```
**Expected:** After `send-timeout-seconds` (5s), you get HTTP 500 with:
```
Kafka send timed out after 5s — broker did not ACK
```

### Scenario 2: Trigger TimeoutException deliberately via tiny custom timeout
```bash
# Start Kafka, run the app
POST http://localhost:8082/api/producer/send-custom-timeout?timeoutSeconds=0
```
**Expected:** Immediate `TimeoutException` — the thread gives up before Kafka can respond.

### Scenario 3: Compare blocking cost — send 10 messages, time it

- `10-messages-timing-measure.ps1`

```powershell
for ($i = 1; $i -le 10; $i++) {

    $url = "http://localhost:8082/api/producer/send-with-timeout?content=msg-$i"

    $result = Measure-Command {
        Invoke-RestMethod -Method Post -Uri $url | Out-Null
    }

    Write-Host "$($result.TotalMilliseconds) ms"
}
```
**Observation:** Each request takes ~10-20ms (waiting for ACK).
With async (case-01), all 10 would fire near-instantly.
This is the real cost of sync — tangible in the numbers.

Expected output:
```
...path-to-file-with-script...com\vbforge\case02\requests> .\10-messages-timing-measure.ps1
100.1872 ms
22.8459 ms
12.9001 ms
13.9501 ms
13.4882 ms
14.8719 ms
12.9925 ms
15.0082 ms
14.3595 ms
13.7556 ms
```

### Scenario 4: Wrong topic name
Change `kafka.topic.sync` in `application.yml` to `nonexistent-topic` while `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`.
**Expected:** `ExecutionException` wrapping a Kafka `UnknownTopicOrPartitionException`.

### Scenario 5: Kafka too slow — distinguish TimeoutException vs ExecutionException
| Exception | Cause | Meaning |
|-----------|-------|---------|
| `TimeoutException` | `.get(N, unit)` expired | Broker didn't respond in time — message state unknown |
| `ExecutionException` | Broker actively rejected | Message was NOT written — cause is wrapped inside |
| `InterruptedException` | Thread was interrupted | Rare in normal flow — restore the interrupt flag! |

---

## 📤 API Reference

| Method | Endpoint | Parameters | Response |
|--------|----------|------------|----------|
| GET | `/api/producer/health` | — | `String` |
| POST | `/api/producer/send-blocking` | `content` (optional) | `SendResult` |
| POST | `/api/producer/send-with-timeout` | `content` (optional) | `SendResult` |
| POST | `/api/producer/send-custom-timeout` | `content` (optional), `timeoutSeconds` (default: 3) | `SendResult` |

---

## ⚙️ Configuration Reference

| Property | Value | Description |
|----------|-------|-------------|
| `server.port` | 8082 | HTTP server port |
| `spring.kafka.bootstrap-servers` | localhost:9092 | Kafka broker |
| `kafka.topic.sync` | case-02-topic-sync | Topic name |
| `kafka.consumer.groupID` | case-02-consumer-group | Consumer group |
| `kafka.producer.send-timeout-seconds` | 5 | Timeout for `/send-with-timeout` |
| `ProducerConfig.ACKS_CONFIG` | "1" | Leader partition must ACK |
| `ProducerConfig.RETRIES_CONFIG` | 0 | No retries — fail fast for learning |
| `ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG` | 10000 | Kafka-level response deadline |

---

## 🛑 Stop Everything

```bash
# Stop Spring Boot
Ctrl+C

# Stop Kafka
cd kafka-08-multi-module-concepts
docker compose down

# Remove with data
docker compose down -v
```

---

## ✅ Learning Checklist

- App starts on port 8082
- `/send-blocking` returns `SendResult` with partition + offset visible
- `/send-with-timeout` same result, bounded by 5s timeout
- `/send-custom-timeout?timeoutSeconds=0` triggers immediate `TimeoutException`
- Consumer logs appear right after HTTP response (contrast to async case-01)
- `sendDurationMs` in the response shows the actual blocking cost
- Docker CLI confirms message at the expected offset

- **[check more theory and Q&A section](THEORY-Q-and-A-SECTION.md)**
