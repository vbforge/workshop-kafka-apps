# Case 16 — Messaging Patterns (Request-Reply, Correlation IDs, Header-Based Routing, Filtering)

| Field          | Value                                             |
|----------------|---------------------------------------------------|
| Module         | `case-16-messaging-patterns`                      |
| Port           | `8816`                                            |
| Request Topic  | `case-16-request-topic`                           |
| Reply Topic    | `case-16-reply-topic`                             |
| Priority Topic | `case-16-routed-priority-topic`                   |
| Standard Topic | `case-16-routed-standard-topic`                   |
| Consumer Group | `case-16-consumer-group`                          |
| Spring Boot    | `4.0.6`                                           |
| Java           | `21`                                              |

---

## What This Case Covers

| Concept | Description |
|---------|-------------|
| **Request-Reply (RPC over Kafka)** | Synchronous-style call over async Kafka using `ReplyingKafkaTemplate` |
| **Correlation IDs** | Auto-managed by Spring Kafka — `KafkaHeaders.CORRELATION_ID` links reply to request |
| **`ReplyingKafkaTemplate`** | Spring Kafka's built-in RPC abstraction — `sendAndReceive()` returns a `CompletableFuture` |
| **`@SendTo`** | Listener annotation — return value is published as a reply, REPLY_TOPIC header used as destination |
| **Header-based routing** | Producer sets `priority` header and routes to different topics accordingly |
| **`RecordFilterStrategy`** | Attached to container factory — filters records before they reach `@KafkaListener` |
| **`@Header`** | Injects a Kafka message header value as a `@KafkaListener` method parameter |

### Request-Reply Flow

```
HTTP Client
    │
    ▼
POST /api/rpc/send?payload=hello
    │
    ▼
RpcClientService.sendAndReceive()
    │  builds ProducerRecord with:
    │    - KafkaHeaders.REPLY_TOPIC  = "case-16-reply-topic"
    │    - KafkaHeaders.CORRELATION_ID = <auto UUID bytes>
    ▼
[case-16-request-topic]
    │
    ▼
RpcServerService.handleRequest()  ← @KafkaListener + @SendTo
    │  returns ReplyMessage
    ▼
[case-16-reply-topic]  (reply has same CORRELATION_ID)
    │
    ▼
ReplyingKafkaTemplate (internal listener)
    │  matches CORRELATION_ID → resolves CompletableFuture
    ▼
RpcClientService.sendAndReceive() unblocks
    │
    ▼
HTTP Response: { requestId, result, roundTripMs, ... }
```

---

## Project Structure

```
case-16-messaging-patterns/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/vbforge/case16/
    │   │   ├── MainApp.java
    │   │   ├── config/
    │   │   │   └── KafkaConfig.java
    │   │   ├── controller/
    │   │   │   └── MessagingPatternsController.java
    │   │   ├── model/
    │   │   │   ├── RequestMessage.java
    │   │   │   ├── ReplyMessage.java
    │   │   │   └── RpcResponse.java
    │   │   └── service/
    │   │       ├── RpcClientService.java      ← sends request, waits for reply
    │   │       ├── RpcServerService.java      ← handles request, sends reply via @SendTo
    │   │       └── HeaderRoutingService.java  ← routes by header, reads headers in listener
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/vbforge/case16/
            └── MainAppTests.java
```

---

## Quick Start

```bash
docker compose up -d
cd case-16-messaging-patterns
mvn spring-boot:run
```

---

## Interactive Demo

### Demo 1: Full RPC Round-Trip
```bash
# Send a request — blocks until reply arrives, returns result + round-trip time
curl -X POST "http://localhost:8816/api/rpc/send?payload=hello+world&priority=STANDARD"

# Expected response:
# {
#   "requestId": "...",
#   "result": "PROCESSED: HELLO WORLD [seq=1]",
#   "success": true,
#   "roundTripMs": 45,
#   "completedAt": "..."
# }

# Check how many requests the server has handled
curl http://localhost:8816/api/rpc/stats
```

### Demo 2: RPC Error Handling (empty payload)
```bash
# Server returns success=false when payload is blank
curl -X POST "http://localhost:8816/api/rpc/send?payload=&priority=STANDARD"
# → { "success": false, "result": null, "roundTripMs": ... }
```

### Demo 3: Header-Based Routing
```bash
# Route a HIGH priority message → lands on case-16-routed-priority-topic
curl -X POST "http://localhost:8816/api/routing/send?payload=urgent+task&priority=HIGH"

# Route a STANDARD priority message → lands on case-16-routed-standard-topic
curl -X POST "http://localhost:8816/api/routing/send?payload=background+job&priority=STANDARD"

# Send several of each
for i in 1 2 3; do
  curl -X POST "http://localhost:8816/api/routing/send?payload=high-$i&priority=HIGH"
  curl -X POST "http://localhost:8816/api/routing/send?payload=std-$i&priority=STANDARD"
done

# Check counts
curl http://localhost:8816/api/routing/stats
# → { "priorityProcessed": 3, "standardProcessed": 3 }
```

### Demo 4: Observe Correlation IDs in Kafka
```bash
# Peek at the reply topic and see the correlation-id header on each record
docker exec kafka-08-broker kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic case-16-reply-topic \
  --from-beginning \
  --property print.headers=true \
  --max-messages 3
```

---

## Verify via Docker CLI

```bash
# See all 4 topics
docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 --list | grep case-16

# Describe reply topic
docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 \
  --describe --topic case-16-reply-topic

# Consume from request topic with headers visible
docker exec kafka-08-broker kafka-console-consumer \
  --bootstrap-server localhost:19092 \
  --topic case-16-request-topic \
  --from-beginning \
  --property print.headers=true \
  --max-messages 5
```

---

## How to Break It (Failure Scenarios)

### Scenario 1 — Reply topic mismatch
Change `kafka.topics.reply` in `application.yml` to a non-existent topic. The server's `@SendTo` will try to publish the reply there. The client will time out after `request-timeout-ms` (10s). Shows why `REPLY_TOPIC` header and the template's reply listener must agree.

### Scenario 2 — Server not running (simulate isolated client)
Comment out `@KafkaListener` in `RpcServerService`. Start the app, call `/api/rpc/send`. The request sits on the topic with no consumer — client times out at 10s with `{ "success": false }`. This is how RPC-over-Kafka degrades vs. throwing immediately like HTTP.

### Scenario 3 — Priority filter inversion
In `KafkaConfig.routedListenerContainerFactory()`, change the filter to:
```java
factory.setRecordFilterStrategy(record -> {
    byte[] h = record.headers().lastHeader("priority").value();
    return "HIGH".equals(new String(h)); // filter OUT high priority messages
});
```
Now `handlePriorityMessage()` receives nothing even from `routed-priority-topic`. `priorityProcessed` stays 0. Demonstrates how `RecordFilterStrategy` silently drops records — not an error, just a filter.

---

## API Reference

| Method | Endpoint | Params | Returns |
|--------|----------|--------|---------|
| GET | `/api/health` | — | `200` string |
| POST | `/api/rpc/send` | `payload` (required), `priority` (default STANDARD) | `200 RpcResponse` |
| GET | `/api/rpc/stats` | — | `200` object |
| POST | `/api/routing/send` | `payload` (required), `priority` (default STANDARD) | `200` string |
| GET | `/api/routing/stats` | — | `200` object |

---

## Configuration Reference

| Key | Default | Description |
|-----|---------|-------------|
| `kafka.topics.request` | `case-16-request-topic` | Server listens here, client sends here |
| `kafka.topics.reply` | `case-16-reply-topic` | Client's ReplyingKafkaTemplate listens here |
| `kafka.topics.routed-priority` | `case-16-routed-priority-topic` | Destination for HIGH priority messages |
| `kafka.topics.routed-standard` | `case-16-routed-standard-topic` | Destination for STANDARD priority messages |
| `kafka.consumer.request-timeout-ms` | `10000` | Max wait for RPC reply before timeout |

---

## Learning Checklist

- [ ] Call `/api/rpc/send` and observe the round-trip time in the response
- [ ] Find the `CORRELATION_ID` and `REPLY_TOPIC` headers using `kafka-console-consumer --print.headers=true`
- [ ] Send with empty payload — confirm `success=false` reply arrives (not a timeout)
- [ ] Route both HIGH and STANDARD messages and confirm `/api/routing/stats` counts split correctly
- [ ] Explain why `@SendTo` with no argument works — where does the reply topic come from?
- [ ] Explain the difference between `future.getSendFuture().get()` and `future.get()`
- [ ] Explain when `RecordFilterStrategy` is preferable to topic-per-priority routing

---

## See Also

- [THEORY-Q-and-A-SECTION.md](THEORY-Q-and-A-SECTION.md)
- case-04 → key-based routing (producer-side routing by key, not header)
- case-17 → Testcontainers (integration tests for exactly this kind of multi-topic flow)
