# Case 16: Q&A Document

## 🎓 Theory Section

### Core Concepts Covered

| Concept | Description |
|---------|-------------|
| **Request-Reply (RPC over Kafka)** | Synchronous-semantics call implemented over an async messaging system |
| **Correlation ID** | A unique token added to the request; the reply echoes it so the sender can match the response |
| **`ReplyingKafkaTemplate`** | Spring Kafka wrapper that manages correlation IDs and reply listeners automatically |
| **`@SendTo`** | Method annotation on a `@KafkaListener` — return value is published as the reply |
| **Temporary / dedicated reply topic** | A topic the client created (or designated) solely for receiving replies |
| **Header-based routing** | Producer sets a header value and chooses the target topic based on it |
| **`RecordFilterStrategy`** | Consumer-side filter attached to the container factory — drops records before they reach the listener |
| **`@Header`** | Spring Kafka parameter annotation — injects a Kafka record header value into a listener method |

### Request-Reply Mental Model

```
Normal async Kafka:
  Producer → [topic] → Consumer
  Producer never knows if Consumer processed the message, or what the result was.

Request-Reply Kafka:
  Client       → [request-topic] → Server
                                     │ processes
  Client ←     [reply-topic]   ←── Server (reply includes CORRELATION_ID)

Matching: Client sends CORRELATION_ID=abc-123 on the request.
          Server echoes CORRELATION_ID=abc-123 on the reply.
          Client sees abc-123 → resolves the pending CompletableFuture.
          Any reply without matching ID is dropped.
```

---

## 📝 Interview Q&A

### Q1: What is the Request-Reply pattern in Kafka and why would you use it instead of HTTP?

**Answer:**

Request-Reply (also called RPC over Kafka) lets a producer send a message and block until a corresponding reply arrives — giving you synchronous semantics over an asynchronous transport.

The mechanics: the producer includes a `REPLY_TOPIC` header (where to send the reply) and a `CORRELATION_ID` header (a unique token). A consumer processes the request and publishes a response to the reply topic with the same correlation ID. The producer is watching the reply topic and when a message arrives with its correlation ID, it unblocks.

Why use it instead of HTTP:

**Decoupling**: the "server" side doesn't need to know the client's address. The reply goes to a Kafka topic — any instance of the client is subscribed and picks it up.

**Resilience**: if the server is temporarily down, the request sits on the topic until the server comes back. HTTP would fail immediately with a connection error.

**Observability**: the request-reply pair is persisted in Kafka (for the retention window). You can inspect, replay, or audit the full exchange.

**Backpressure**: Kafka naturally buffers excess requests — the server processes at its own rate. HTTP under overload returns 503 or times out.

Tradeoff: higher latency (two network hops via broker vs. one direct HTTP call), more infrastructure, and timeouts behave differently (client times out waiting for a reply rather than getting a connection error).

Use Request-Reply when services are already using Kafka, you need the durability/buffering, or you want the decoupling. Use HTTP when you need minimal latency or the services have no reason to be on a message bus.

---

### Q2: Explain the role of the Correlation ID — who generates it, where is it stored, and how is it matched?

**Answer:**

The Correlation ID is a unique token — typically a UUID — that ties a specific reply to its originating request.

**Generation**: `ReplyingKafkaTemplate` generates it automatically when you call `sendAndReceive()`. It creates a random byte array and adds it as a `KafkaHeaders.CORRELATION_ID` header on the outgoing `ProducerRecord`. You never generate it manually when using the template.

**Where it lives**: as a Kafka record header on the message in the request topic. Headers are key-value byte arrays attached to a Kafka record — they travel with the record through the broker but are not part of the JSON body. The server-side Spring Kafka listener framework automatically copies the `CORRELATION_ID` header onto the reply record when `@SendTo` is used.

**Matching**: `ReplyingKafkaTemplate` maintains an internal `Map<CorrelationKey, CompletableFuture>`. When a reply arrives on the reply topic, Spring Kafka reads its `CORRELATION_ID` header, looks it up in the map, and calls `future.complete(replyRecord)`. If no match is found (stale reply, wrong template instance), the record is ignored.

Timeout: if no reply arrives within the configured timeout (`defaultReplyTimeout`), the future is completed with a `KafkaReplyTimeoutException`. This prevents memory leaks from orphaned pending futures.

In manual implementations (without `ReplyingKafkaTemplate`), you generate the correlation ID yourself, store the `Map<String, CompletableFuture>` yourself, and copy the header in the server listener yourself. The template removes all this boilerplate.

---

### Q3: What is `ReplyingKafkaTemplate` and what does it do internally that `KafkaTemplate` doesn't?

**Answer:**

`KafkaTemplate` is a fire-and-forget (or fire-and-ack) producer. It sends a record and optionally waits for broker acknowledgment. It has no concept of a reply.

`ReplyingKafkaTemplate<K, V, R>` extends this with the request-reply contract. Internally it:

1. **Manages a reply listener container**: it wraps a `ConcurrentMessageListenerContainer` that subscribes to the reply topic. This container is started when the template starts (it implements `SmartLifecycle`) and runs continuously in the background.

2. **Correlation tracking**: it holds a `ConcurrentHashMap<CorrelationKey, RequestReplyFuture>` mapping correlation IDs to pending futures.

3. **Header injection**: `sendAndReceive()` generates a `CORRELATION_ID` and sets it on the `ProducerRecord`. It also reads the `REPLY_TOPIC` header if you've set it, or falls back to a configured default reply topic.

4. **Reply dispatch**: when the reply listener receives a message, it extracts the `CORRELATION_ID`, finds the matching future, and completes it with the reply record.

5. **Timeout management**: it schedules a timeout task for each pending request. If the timer fires before the reply arrives, the future is completed exceptionally.

You wire it by providing: a `ProducerFactory` (for sending requests) and a `ConcurrentMessageListenerContainer` (for receiving replies). The container must NOT be shared with any `@KafkaListener` — the template owns it exclusively.

---

### Q4: What does `@SendTo` do and how does Spring Kafka know where to send the reply?

**Answer:**

`@SendTo` on a `@KafkaListener` method tells Spring Kafka: "take the return value of this method and publish it to a topic as the reply."

The destination is resolved in this order:

1. **`REPLY_TOPIC` header on the incoming request**: if the producer set `KafkaHeaders.REPLY_TOPIC`, Spring Kafka uses that topic. This is the dynamic option — each client specifies its own reply topic. `ReplyingKafkaTemplate` sets this header automatically.

2. **`@SendTo("specific-topic")` argument**: if you hard-code the topic in the annotation, it always replies there, regardless of the header.

3. **No destination**: if neither is set, Spring Kafka throws an exception.

Spring Kafka also copies the `CORRELATION_ID` header from the incoming request record to the outgoing reply record automatically. This is the whole mechanism — without `@SendTo` copying the correlation ID, the client's `ReplyingKafkaTemplate` wouldn't be able to match the reply.

The container factory must have `setReplyTemplate()` configured. Without it, `@SendTo` has no producer to send with and the annotation is silently ignored — your replies never arrive and the client times out.

Critical gotcha: the method must NOT be `void`. If you declare `void handleRequest(...)`, the return value is null and no reply is sent. Always return the reply type explicitly.

---

### Q5: What is `RecordFilterStrategy` and when should you use it instead of topic-per-concern routing?

**Answer:**

`RecordFilterStrategy` is an interface you set on a `ConcurrentKafkaListenerContainerFactory`. It receives each `ConsumerRecord` before it reaches your `@KafkaListener` and returns a boolean: `true` = filter out (discard), `false` = pass through.

The filtering happens after the record is consumed from Kafka but before your business logic. The offset still advances — you're not "putting it back". Filtered records are simply dropped and never reach the listener method.

**When to prefer `RecordFilterStrategy` over separate topics:**

Use filtering when:
- You have many categories but low volume and don't want to manage dozens of topics
- The filter criterion is dynamic (read from a database, toggleable at runtime)
- You want a single consumer group to share one topic but each instance only handles its subset
- Schema complexity makes topic proliferation impractical

Use separate topics when:
- You need throughput isolation — a flood of STANDARD messages shouldn't delay HIGH priority processing
- You need separate retention policies per priority class
- Ops team wants clean topic names and separate monitoring per type
- Consumer scaling needs to be independent (autoscale priority consumers, not standard)

A hybrid is also common: route to two topics (HIGH / STANDARD) AND attach a filter on the STANDARD consumer to also drop messages where `retryCount > 5` (send to DLT instead). Routing handles coarse-grained separation; filtering handles fine-grained exceptions within a topic.

One subtle thing: `RecordFilterStrategy` runs on the consumer thread before the listener, meaning it does add per-record overhead. For very high-throughput topics, even a trivial filter adds measurable latency. Topic-based routing offloads the routing decision to the producer and incurs zero consumer overhead.

---

### Q6: How do you read Kafka message headers in a `@KafkaListener` method?

**Answer:**

Spring Kafka maps Kafka record headers to listener method parameters using `@Header`. The annotation comes from `org.springframework.messaging.handler.annotation.Header`.

```java
@KafkaListener(topics = "my-topic")
public void handle(
    MyPayload message,                                           // deserialized value
    @Header("priority") String priority,                        // Kafka header → String
    @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,          // built-in topic header
    @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,     // built-in partition
    @Header(KafkaHeaders.OFFSET) long offset,                   // built-in offset
    @Header(name = "optional-thing", required = false, defaultValue = "N/A") String optional
) { ... }
```

Key points:

**Byte array conversion**: Kafka headers are raw `byte[]`. Spring Kafka automatically converts to `String` if the parameter type is `String`. If you need the raw bytes, declare the parameter as `byte[]`.

**Required vs optional**: `required = true` (default) throws an exception if the header is absent. `required = false` delivers `null`. `defaultValue` provides a String fallback when the header is missing (implies `required = false`).

**Built-in headers**: Spring Kafka injects standard metadata headers automatically: `KafkaHeaders.RECEIVED_TOPIC`, `KafkaHeaders.RECEIVED_PARTITION`, `KafkaHeaders.OFFSET`, `KafkaHeaders.RECEIVED_KEY`, `KafkaHeaders.TIMESTAMP`. You don't need the producer to set these — the framework sets them from the `ConsumerRecord` metadata.

**`ConsumerRecord` alternative**: if you need everything, accept a `ConsumerRecord<K, V>` parameter and read headers from `record.headers()` directly. This gives you the raw `Iterable<Header>` with full control.

---

### Q7: What happens if two instances of the same service both have `ReplyingKafkaTemplate` listening on the same reply topic and consumer group?

**Answer:**

This is a real production gotcha. If two instances share the same reply consumer group, Kafka will partition-assign the reply topic partitions between them. Each instance only gets some of the reply messages. The instance that originally sent the request may not be the one that receives the reply.

When instance A sends a request and sets `REPLY_TOPIC=case-16-reply-topic`, the server sends the reply to that topic. The reply may land on instance B's consumer thread. Instance B looks up the `CORRELATION_ID` in its own `Map<CorrelationKey, CompletableFuture>` — it's not there (instance A sent that request). The reply is dropped. Instance A's `sendAndReceive()` call times out.

Solutions:

**Unique reply topic per instance**: each instance listens on `reply-topic-{instanceId}`. The producer sets the header to its own instance topic. Replies are guaranteed to reach the right instance. Downside: N topics for N instances.

**Unique consumer group per instance**: set `replyGroupId` to `reply-group-${random.uuid}` or `reply-group-${spring.application.instance-id}`. With a unique group, each instance gets ALL partitions of the reply topic. Every reply reaches every instance — the one that knows the correlation ID resolves it, others drop it. Slight overhead (all instances consume all replies) but correct.

**Headers-based partition routing**: advanced setup where the client adds a `REPLY_PARTITION` header. The server sends the reply to a specific partition that only the originating instance reads. Zero wasteful consumption but requires the client to manage its partition assignment.

`ReplyingKafkaTemplate` uses a UUID group ID by default when you don't set one explicitly — this is intentional to avoid the cross-instance problem. The `replyContainer` in `KafkaConfig` sets its own group; make sure it's unique per deployment instance in production.

---

### Q8: What is the difference between a "temporary reply topic" and a "permanent shared reply topic"?

**Answer:**

A **temporary reply topic** is created dynamically per request (or per client session), used to receive exactly one reply, then deleted. This approach is impractical with Kafka — topic creation is expensive (seconds), Kafka is not designed for ephemeral topics, and topic proliferation (thousands of temp topics) creates operational problems. Temporary topics are common in AMQP (RabbitMQ) but are an anti-pattern in Kafka.

A **permanent shared reply topic** — the standard Kafka approach — is a single topic (`case-16-reply-topic`) that persists across all requests and all clients. Multiple requests' replies coexist in the topic. Correlation IDs distinguish which reply belongs to which request.

Trade-offs of the shared permanent approach:
- All clients share the same topic → simpler ops, fewer topics
- Replies from ALL clients land on the same topic → correct routing depends on correlation IDs
- Replies are retained for the retention period → stale replies might be seen after restart (the template's correlation map is in-memory — after restart it has no pending futures, so old replies are simply unmatched and dropped)
- Multiple instances of the same service need unique consumer groups (see Q7)

The retention risk: if the service restarts while requests are in-flight, the pending `CompletableFuture` map is cleared. When the reply eventually arrives, no future matches it — dropped. The HTTP caller has already received a timeout response. This is acceptable behavior — the reply's message is durable (stays in Kafka for retention period), but the in-memory state linking it to a waiting caller is gone. Production systems either use short timeouts (< retry window) or implement persistent correlation tracking (store pending request state in Redis/DB).
