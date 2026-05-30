# Case 02: Q&A Document

## 🎓 Theory Section

### Core Concepts Covered

| Concept | Description |
|---------|-------------|
| **Synchronous send** | Producer blocks the calling thread until broker ACK is received |
| **`Future.get()`** | Java concurrency primitive — blocks until the async result is available |
| **`RecordMetadata`** | Broker-returned confirmation: partition, offset, timestamp |
| **`TimeoutException`** | `.get(N, unit)` expired — broker was too slow or unreachable |
| **`ExecutionException`** | Broker actively rejected — wraps the real Kafka error |
| **`InterruptedException`** | The waiting thread was interrupted — must restore interrupt flag |
| **ACKS config** | How many replicas must confirm before the producer gets an ACK |
| **REQUEST_TIMEOUT_MS** | Kafka-level deadline — different from the application-level `.get()` timeout |

### The Sync vs Async Decision

| Dimension | Async (case-01) | Sync (case-02) |
|-----------|----------------|----------------|
| **Throughput** | High — fire-and-forget, no waiting | Lower — one ACK per blocking call |
| **Latency** | Low for the producer | Higher — adds broker round-trip per send |
| **Delivery guarantee** | Best-effort at call time | Confirmed before returning |
| **Error handling** | Must use callbacks or listeners | Exceptions thrown at call site |
| **Use cases** | Logs, metrics, events, telemetry | Payments, orders, audit trails |

---

## 📝 Interview Q&A

### Q1: What is the difference between `kafkaTemplate.send()` and `kafkaTemplate.send().get()`?

**Answer:**

`kafkaTemplate.send()` returns a `CompletableFuture<SendResult>` immediately. The message is queued internally and sent to Kafka asynchronously — the calling thread moves on without waiting.

`kafkaTemplate.send().get()` **blocks the calling thread** until one of three things happens:
- The broker confirms the write → `SendResult` (with `RecordMetadata`) is returned
- The application timeout expires → `TimeoutException`
- The broker rejects the message → `ExecutionException`

The practical difference: with `.get()` you know the message is on Kafka before you continue. Without it, you don't.

---

### Q2: Why is `.get()` with no timeout dangerous in production?

**Answer:** Because it can block indefinitely.

If Kafka is overloaded, experiencing a leader election, or simply unreachable, the thread calling `.get()` will hang forever (or until the JVM/framework kills it). In a web server, HTTP request threads are a finite pool. Even a few threads hanging on a dead Kafka connection can exhaust the thread pool and take down the entire application — not just Kafka-related endpoints.

The safe pattern is always `.get(timeout, TimeUnit.SECONDS)`. If it times out, the application can return a 503, trigger a retry, or alert — rather than silently hanging.

---

### Q3: What three exceptions can `.get()` throw, and what does each mean?

**Answer:**

| Exception | When | What to do |
|-----------|------|------------|
| `ExecutionException` | Broker actively rejected the message | Unwrap with `.getCause()` to get the real Kafka error. Do NOT retry blindly — check if it's retryable first. |
| `TimeoutException` | Application deadline expired | Message state is **unknown** — it may or may not have been written. Log, alert, consider idempotency. |
| `InterruptedException` | Calling thread was interrupted externally | **Must call `Thread.currentThread().interrupt()`** to restore the interrupt flag before propagating. |

The `TimeoutException` case is the trickiest: you don't know if Kafka wrote the message or not. This is why idempotent producers (case-13) exist.

---

### Q4: What is `RecordMetadata` and why does it matter?

**Answer:** `RecordMetadata` is the confirmation object the broker sends back after writing a message. It contains:

- `partition()` — which partition the message landed in
- `offset()` — the exact position of the message within that partition (monotonically increasing, unique per partition)
- `timestamp()` — when the broker wrote it (in epoch ms)
- `topic()` — the topic name (useful in multi-topic sends)

Why it matters: the offset is essentially a receipt. If downstream systems fail, you can seek back to that exact offset and reprocess from there. Logging the offset alongside your business transaction ID is a common pattern in financial systems.

With async (case-01) you can also get `RecordMetadata` — but via callback, not inline. Sync makes it available right at the call site.

---

### Q5: What is `ACKS_CONFIG` and what values does it accept?

**Answer:** `ACKS_CONFIG` tells the broker how many replicas must acknowledge a write before the producer considers it "done".

| Value | Meaning | Risk |
|-------|---------|------|
| `"0"` | Don't wait for any ACK — fire and forget at the network level | Message can be lost if broker crashes immediately after receiving |
| `"1"` | Leader partition must write to its log | If leader crashes before replication, message is lost |
| `"all"` (or `"-1"`) | All in-sync replicas must acknowledge | Safest — survives leader failure as long as at least one replica is alive |

**For sync sends, `ACKS_CONFIG = "1"` or `"all"` makes sense** — you're already blocking for a confirmation, so you want that confirmation to mean something. Using `"0"` with sync would be contradictory: you'd block the thread but the ACK would come immediately with no real guarantee.

In this case (single-node local Kafka), `"1"` and `"all"` are identical since replication factor is 1.

---

### Q6: What is the difference between `REQUEST_TIMEOUT_MS_CONFIG` and the timeout passed to `.get()`?

**Answer:** They operate at different layers and are independent of each other.

`REQUEST_TIMEOUT_MS_CONFIG` (set in `ProducerConfig`) is **Kafka's own deadline** — how long the Kafka client library waits for a response from the broker on a single network request attempt. If the broker doesn't respond in this window, Kafka internally marks the attempt as failed.

The timeout in `.get(N, TimeUnit.SECONDS)` is **your application's deadline** — how long your calling thread is willing to wait for the `Future` to resolve (which includes any retries Kafka may do internally).

Think of it like this:
- `REQUEST_TIMEOUT_MS` = "how long does Kafka try each attempt?"
- `.get(N, unit)` = "how long is our app willing to wait overall?"

If `RETRIES > 0`, Kafka might make multiple attempts within your `.get()` timeout window. In this case we set `RETRIES=0`, so there's exactly one attempt, and both timeouts apply to the same single try.

---

### Q7: We set `RETRIES_CONFIG = 0` — why? Isn't that bad practice?

**Answer:** For a learning environment it's the right call. With retries enabled, failures are silently retried by Kafka — you'd never see the `ExecutionException` in the failure scenarios. `RETRIES=0` means: fail fast and loudly, so you can observe what goes wrong.

In production, the recommendation depends on context:
- For idempotent producers (case-13), retries are safe and recommended — Kafka deduplicates by sequence number.
- For non-idempotent producers, retries can cause **duplicate messages** (the broker wrote the message, but the ACK was lost in transit, so the producer retries and writes it again). This is a real production gotcha.

The general production guidance: use idempotent producers with `enable.idempotence=true` (which forces `ACKS_CONFIG="all"` and `RETRIES > 0` automatically). That's case-13's territory.

---

### Q8: Why does `InterruptedException` require `Thread.currentThread().interrupt()`?

**Answer:** Java's thread interruption model uses a flag on the thread. When `InterruptedException` is thrown and caught, the flag is **cleared** — the interruption signal is consumed.

If you catch `InterruptedException` and just rethrow or wrap it without restoring the flag, any code upstream (a framework, a thread pool) that checks `Thread.isInterrupted()` will see `false` and assume no interruption occurred. This breaks cooperative cancellation.

The pattern is:
```java
} catch (InterruptedException e) {
    Thread.currentThread().interrupt(); // restore the flag
    throw new RuntimeException("...", e); // then propagate
}
```

This is a standard Java concurrency rule — every `InterruptedException` handler must either re-throw it directly or restore the flag before doing anything else.

---

## 📊 Quick Reference Card

| Term | Definition |
|------|------------|
| `CompletableFuture.get()` | Blocks the calling thread until result is available |
| `CompletableFuture.get(N, unit)` | Blocks with a bounded deadline — throws `TimeoutException` if expired |
| `RecordMetadata` | Broker's write confirmation: partition + offset + timestamp |
| `ExecutionException` | Wraps the actual Kafka exception on broker rejection |
| `ACKS_CONFIG` | Durability guarantee level: 0 / 1 / all |
| `REQUEST_TIMEOUT_MS` | Kafka-internal network timeout per attempt |
| `RETRIES_CONFIG` | Number of retry attempts on transient failure |
| Idempotent producer | Producer that can retry without creating duplicates (case-13) |

---

## ✅ Self-Assessment

After studying this Q&A, you should be able to:

- Explain the exact difference between async `.send()` and sync `.send().get()`
- Describe all three exceptions `.get()` can throw and what each requires
- Justify why `.get()` with no timeout is dangerous in production
- Read `RecordMetadata` and explain what partition + offset tell you
- Explain `ACKS_CONFIG` values and choose the right one for a use case
- Distinguish `REQUEST_TIMEOUT_MS_CONFIG` from the `.get()` timeout
- Explain why `RETRIES=0` is correct for a learning environment
- Correctly handle `InterruptedException` without swallowing the interrupt flag
