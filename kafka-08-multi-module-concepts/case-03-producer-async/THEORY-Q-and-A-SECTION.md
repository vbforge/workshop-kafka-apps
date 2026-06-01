# Case 03: Q&A Document

## 🎓 Theory Section

### Core Concepts Covered

| Concept | Description |
|---------|-------------|
| **Non-blocking send** | `kafkaTemplate.send()` returns a `CompletableFuture` immediately; the calling thread is never blocked |
| **`whenComplete()`** | Callback that fires on the Kafka I/O thread when the send resolves (success or failure) |
| **`thenAccept()`** | Callback that fires only on success; receives the `SendResult` |
| **`exceptionally()`** | Callback that fires only on failure; receives the `Throwable` |
| **HTTP 202 Accepted** | Correct HTTP status for async operations — "accepted for processing, outcome unknown" |
| **Kafka I/O thread** | Kafka's internal thread that does actual network writes and fires callbacks |
| **Fire-and-forget** | Sending without caring about the outcome — appropriate only for non-critical data |
| **`LINGER_MS`** | How long the producer waits to accumulate more messages before sending a batch |

### The Async Progression

| Case | Pattern | Outcome visibility | Thread behavior |
|------|---------|-------------------|-----------------|
| case-01 | `send()` — no callback | None | Fire and forget |
| case-02 | `send().get()` — blocking | Immediate, in-line | Calling thread blocked |
| case-03 | `send()` + callbacks | Delayed, in callback | Calling thread free |

---

## 📝 Interview Q&A

### Q1: What does `kafkaTemplate.send()` actually return, and how does that enable async patterns?

**Answer:**

`kafkaTemplate.send()` returns a `CompletableFuture<SendResult<K, V>>`. The `CompletableFuture` is a Java concurrency primitive representing a result that will be available at some point in the future — it may already be complete, or it may complete later.

The moment `send()` is called, the message is placed into the producer's internal buffer and the future is returned immediately — before any network I/O happens. The calling thread is never blocked.

The future becomes resolved (either successfully or exceptionally) when the Kafka I/O thread either receives a broker ACK or gives up after exhausting retries. At that point, any callbacks registered on the future (`whenComplete`, `thenAccept`, `exceptionally`) are invoked on the I/O thread.

This is what enables the async pattern: the HTTP request thread can return a response while the I/O thread handles delivery in the background.

---

### Q2: What is the difference between `whenComplete()`, `thenAccept()`, and `exceptionally()`?

**Answer:**

All three are `CompletableFuture` callback methods, but they handle different conditions:

`whenComplete(BiConsumer<T, Throwable>)` fires in **both** success and failure cases. The first argument is the result (null on failure), the second is the exception (null on success). This is the most general and most commonly used in production because you handle everything in one place.

`thenAccept(Consumer<T>)` fires **only on success**. The result is guaranteed non-null. It returns `CompletableFuture<Void>`, meaning you can't pass a value forward — it's used for side effects only.

`exceptionally(Function<Throwable, T>)` fires **only on failure**. It receives the `Throwable` and must return a value of type `T` (the same type as the original future). In practice, when used after `thenAccept`, `T` is `Void` and you return `null` — you're just using it for the side effect (logging, alerting).

You'd choose split handlers when the success path and failure path have genuinely different logic — for example, success triggers a domain event while failure triggers a PagerDuty alert. If both paths share common logging boilerplate, `whenComplete` keeps it cleaner.

---

### Q3: Why do we return HTTP 202 instead of HTTP 200 for async sends?

**Answer:**

HTTP semantics have specific meanings:

`200 OK` means: the request was received, processed, and completed. When applied to a Kafka send, this would imply the message is confirmed on the broker — which is not true for an async send.

`202 Accepted` means: the request was accepted for processing, but processing has not been completed (and may not have started). This accurately describes what happens when we return from an async send — the message is in the producer buffer, but broker delivery has not occurred yet.

Returning 200 for an async send is a lie to the client. The message could still fail after the 200 was returned. If the client sees 200 and assumes confirmed delivery, you have a contract violation — and silent data loss when failures occur.

The 202 response signals to the client: "we've queued this, but if you need confirmation, you'll need a separate mechanism" (webhook, polling endpoint, correlation ID lookup, etc.).

---

### Q4: What thread runs the callback in `whenComplete()`, and why does that matter?

**Answer:**

The callback runs on the **Kafka producer's I/O thread** — the internal thread that manages actual network writes to the broker.

This matters for several reasons:

**Shared mutable state is dangerous.** The I/O thread and the HTTP request thread run concurrently. If your callback modifies shared state (a counter, a list, a database connection pool) without proper synchronization, you have a race condition. In our case we only log, which is thread-safe, so there's no issue.

**Don't do slow work in callbacks.** The I/O thread is responsible for ALL Kafka sends in the application. If your callback does something slow (a database write, a synchronous HTTP call), you're blocking the thread that processes every outgoing message. This will degrade throughput globally. For slow post-send work, submit it to a separate executor: `CompletableFuture.runAsync(() -> slowWork(), myExecutor)`.

**Don't throw unchecked exceptions from callbacks.** An unhandled exception in a `whenComplete` callback swallows the error silently. Always wrap callback bodies in try-catch.

---

### Q5: What is the "silent failure" problem with async sends, and how do you solve it in production?

**Answer:**

With async sends, the HTTP client gets a success response (202) before broker delivery is confirmed. If the broker is down, the network drops, or retries are exhausted, the send fails — but the client already got 202 and believes the message was accepted.

This creates silent data loss: the business operation appeared to succeed, but the Kafka message was never written.

Three production patterns to address this:

**Outbox pattern**: Instead of writing to Kafka directly, write to a database "outbox" table in the same transaction as your business data. A separate process reads the outbox and publishes to Kafka with retry logic. The business transaction commits atomically with the outbox record — no Kafka failure can cause silent loss.

**Retry table**: On callback failure, insert the failed message into a retry table with the payload and a retry count. A background job retries delivery with backoff until success or a max-retries threshold.

**Idempotent producer with retries** (case-13): Set `enable.idempotence=true`, `retries > 0`. Kafka retries internally and deduplicates by sequence number, so a message is eventually delivered exactly once without your application managing retries. This handles transient broker issues but not full broker outages — the client still gets 202 before confirmation.

The right choice depends on your consistency requirements. For financial systems, the outbox pattern is the gold standard. For eventually-consistent event-driven systems, idempotent producer with retries is usually sufficient.

---

### Q6: What is `LINGER_MS_CONFIG`, and when would you increase it in a production async producer?

**Answer:**

`LINGER_MS_CONFIG` is how long the Kafka producer waits to accumulate additional messages before sending a batch to the broker. Default is 0, meaning: send immediately when there's anything in the buffer.

The trade-off:
- `LINGER_MS = 0` → lowest latency per message, but each message potentially goes in its own network request (poor throughput at high volume)
- `LINGER_MS = 5-50ms` → slightly higher latency per message, but messages that arrive within that window get batched together into one network request (much better throughput)

When to increase it: high-volume async pipelines where you're publishing thousands of events per second (telemetry, clickstream, log aggregation). The latency trade-off (a few milliseconds) is invisible to the business, but the throughput improvement is dramatic — network round-trips are expensive, and batching amortizes that cost.

When to keep it at 0: any scenario where a human is waiting for a response, or where downstream latency SLAs are tight (< 10ms end-to-end).

`LINGER_MS` works hand-in-hand with `BATCH_SIZE_CONFIG` (default 16KB). The producer sends a batch when either condition is met: the batch fills to `BATCH_SIZE` bytes, or `LINGER_MS` expires — whichever comes first.

---

### Q7: How does async sending affect message ordering guarantees?

**Answer:**

This is a common production gotcha. Kafka guarantees ordering **within a partition for a single producer** — messages sent by the same producer to the same partition arrive at consumers in the order they were written to the broker.

With async sends and `RETRIES_CONFIG > 0`, ordering can break: if message A and message B are both in-flight, and A's first attempt fails while B succeeds, and A is then retried — B appears in the partition before A's retry. Result: consumer sees B before A.

Solutions:

**`MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = 1`**: Only one in-flight request at a time per connection. Retries can't cause reordering because the next message isn't sent until the current one is confirmed. Downside: kills throughput.

**`enable.idempotence = true` (case-13)**: This is the modern answer. The producer assigns sequence numbers to messages. The broker tracks them and reorders if necessary. You get ordering guarantees even with retries and `max.in.flight = 5`. This is now the recommended default for any production producer.

For case-03 specifically, we set `RETRIES = 0`, so there are no retries and no reordering risk. This is only for learning — don't do this in production.

---

### Q8: In the `thenAccept` + `exceptionally` pattern, why does `exceptionally` require a return value even though we don't use it?

**Answer:**

Because `exceptionally` is a **recovery operator**, not just a side-effect operator. Its contract is: "if the future failed, provide a fallback value so the chain can continue." It returns `CompletableFuture<T>` where `T` matches the original type.

When we call `future.thenAccept(...)`, the result is `CompletableFuture<Void>`. Then calling `.exceptionally(...)` on that means `T = Void`, and `Void` can only be instantiated as `null`. So `return null` is the only valid implementation.

This feels awkward, and it is — `exceptionally` was designed for recovery, not for "I just want to log failures." The more semantically correct method is `whenComplete()` on the original future, which is a pure side-effect handler. The split-handler pattern (`thenAccept` + `exceptionally`) is useful when you genuinely need different control flow, but `whenComplete` is cleaner when you just need both paths handled.

Java 12+ added `exceptionallyAsync()` and `handle()` for more flexibility, but the fundamental return-type contract remains.

---

## 📊 Quick Reference Card

| Term | Definition |
|------|------------|
| `CompletableFuture` | Represents an async result — may already be done or will complete later |
| `whenComplete()` | Fires on both success and failure; receives result + throwable |
| `thenAccept()` | Fires on success only; receives the result |
| `exceptionally()` | Fires on failure only; receives the throwable; must return a value |
| HTTP 202 Accepted | "Request accepted, processing not yet complete" — correct status for async |
| Kafka I/O thread | Internal Kafka thread that sends network requests and fires callbacks |
| `LINGER_MS` | How long the producer waits to batch messages before sending |
| Outbox pattern | Write to DB + Kafka outbox atomically; separate process publishes to Kafka |
| Idempotent producer | Safe retries without duplicates; solves both ordering and at-least-once issues |

---

## ✅ Self-Assessment

After studying this Q&A, you should be able to:

- Explain what `kafkaTemplate.send()` returns and why that enables async patterns
- Distinguish `whenComplete`, `thenAccept`, and `exceptionally` — when to use each
- Justify returning HTTP 202 instead of 200 for async Kafka sends
- Describe which thread runs the callback and why slow work in callbacks is dangerous
- Explain the silent failure problem and three production patterns to mitigate it
- Describe `LINGER_MS` and when to increase it for throughput
- Explain how async retries can break message ordering and how idempotent producer solves it
- Explain why `exceptionally` requires a return value
