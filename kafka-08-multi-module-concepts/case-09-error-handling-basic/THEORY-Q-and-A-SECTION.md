# Case 09: Q&A Document

## 🎓 Theory Section

### Core Concepts Covered

| Concept | Description |
|---------|-------------|
| **`DefaultErrorHandler`** | Spring Kafka 2.8+ built-in handler — replaces `SeekToCurrentErrorHandler` |
| **`FixedBackOff(interval, retries)`** | Retries with a fixed delay; `interval=0` means immediate retries |
| **Retryable exception** | Any exception NOT in the non-retryable list — handler retries up to maxAttempts |
| **Non-retryable exception** | Registered via `addNotRetryableExceptions()` — skips straight to recovery |
| **Recovery action** | Runs after all retries exhausted or on non-retryable exception |
| **Default recovery** | Log at ERROR level + commit offset (move past the record) |
| **`setRetryListeners()`** | Hooks into attempt lifecycle: `onNextAttempt`, `recovered`, `recoveryFailed` |

### The Error Handling Decision Tree

```
Exception thrown in @KafkaListener
         │
         ▼
Is it in notRetryableExceptions list?
    YES → go straight to recovery (no retries)
    NO  → retry up to (maxAttempts - 1) more times
              │
              ▼
         All retries exhausted?
              YES → recovery action
                      default: log + commitSync(offset + 1)
                      case-11:  publish to DLT + commitSync(offset + 1)
```

---

## 📝 Interview Q&A

### Q1: What is `DefaultErrorHandler` and what did it replace?

**Answer:**

`DefaultErrorHandler` was introduced in Spring Kafka 2.8 as the single unified error handler, replacing two older handlers that had overlapping responsibilities:

`SeekToCurrentErrorHandler` (the old retry handler): on exception, it seeked the consumer back to the failed record's offset and re-delivered it. This meant the consumer physically re-polled the same record. Effective but had a quirk — it worked by repositioning the consumer in the partition log.

`RecoveringBatchErrorHandler` (the old batch error handler): similar concept for batch listeners.

`DefaultErrorHandler` consolidates both. It no longer needs to seek back to the record — Spring Kafka now buffers the failed record in memory and re-invokes the listener directly without re-polling. This is more efficient (no unnecessary round-trips to the broker) and handles both record and batch listeners uniformly.

In new Spring Kafka code, `DefaultErrorHandler` is the one to use. If you see `SeekToCurrentErrorHandler` in documentation or older codebases, treat it as legacy.

---

### Q2: How does `FixedBackOff(intervalMs, maxRetries)` translate to total delivery attempts?

**Answer:**

`FixedBackOff` counts **retries**, not total attempts. The constructor takes `(long interval, long maxAttempts)` where `maxAttempts` is the number of *additional* attempts after the first failure.

So `FixedBackOff(0L, 2)` means:
- Attempt 1: original delivery → fails
- Wait 0ms
- Attempt 2: retry 1 → fails
- Wait 0ms
- Attempt 3: retry 2 → fails
- BackOff exhausted → recovery action runs

Total: **3 delivery attempts** (1 original + 2 retries).

This is why in the config we write `new FixedBackOff(0L, maxAttempts - 1)` when `maxAttempts = 3` — we want 3 total attempts, so we pass `maxAttempts - 1 = 2` retries to FixedBackOff.

The confusion is common. To remember: FixedBackOff's second argument is "how many MORE times to try after the first failure", not "how many times total".

---

### Q3: What happens to the consumer and its partition during retry attempts?

**Answer:**

During retries, the consumer is **not re-polling from Kafka**. Spring Kafka buffers the failed `ConsumerRecord` in memory and re-invokes the `@KafkaListener` method directly. The underlying Kafka consumer is paused (not polling new records) while retries are in progress.

This has important implications:

**Heartbeats still fire**: the Kafka consumer's heartbeat thread runs independently of the message processing thread. The consumer stays in the group during retries — no rebalance is triggered.

**The partition is effectively blocked**: no new messages from this partition are consumed while retries are happening. If you're retrying 3 times with 5-second backoff, messages that arrived after the failing one wait at least 15 seconds. This is why retry intervals must be tuned carefully — too long and you build lag.

**Other partitions are unaffected**: if you have concurrency > 1, other consumer threads continue processing their partitions normally. Only the thread retrying the failed record is blocked.

**`max.poll.interval.ms` must accommodate retry time**: if your total retry duration (retries × backoff interval) approaches `max.poll.interval.ms`, Kafka will remove the consumer from the group. Rule of thumb: total retry time should be under `max.poll.interval.ms / 2`.

---

### Q4: What is the difference between retryable and non-retryable exceptions, and how does the handler determine which is which?

**Answer:**

`DefaultErrorHandler` maintains an internal list of exception types that should NOT be retried. You add to it with `addNotRetryableExceptions(ExceptionClass.class)`. The handler checks using `instanceof` — subclasses of registered types are also non-retryable.

**Non-retryable** (immediate recovery): exceptions where retrying will never produce a different result. Examples: `FatalProcessingException` (your custom type for "this data is permanently invalid"), `NullPointerException` (a null field in the payload won't become non-null on retry), `DeserializationException` (if the bytes couldn't be parsed once, they never will), schema validation failures.

**Retryable** (default): everything else. Examples: `TransientProcessingException` (downstream service temporarily unavailable), `ConnectException` (database connection blip), timeout exceptions.

The reasoning behind classification: retrying a non-retryable exception wastes `maxAttempts` delivery attempts — CPU, time, and most importantly, it delays processing of subsequent messages on the same partition for the full retry duration. A transient DB connection error might resolve in 100ms, so 3 retries with 50ms backoff makes sense. A message with a null required field will fail every single time — there's no point waiting.

Production discipline: classify aggressively. When in doubt, classify as non-retryable and send to DLT (case-11) for human inspection.

---

### Q5: What is the default recovery action, and what are its limitations?

**Answer:**

The default recovery action (no custom `DeadLetterPublishingRecoverer` configured) is:
1. Log the failed record at `ERROR` level with the exception details
2. Call `commitSync` for `offset + 1` on the failed record's partition — moves past it
3. Resume consuming from the next record

This is the "log and skip" pattern. It's simple and ensures the consumer never blocks permanently on a bad record. But its limitations are significant:

**Silent data loss**: the failed record is gone from the perspective of this consumer group. If the record contained a payment event, an order placement, or any business-critical data — that data is now lost unless you have other mechanisms (source system retry, audit log).

**No actionability**: the only artifact is a log line. In production, a log line might get lost in noise, not alert on-call, or not contain enough context to reproduce the failure.

**No replay path**: there's no way to re-process the failed record after a code fix without running a separate replay job against the original topic (which may have been retained or may not).

The production upgrade: always configure a `DeadLetterPublishingRecoverer` (case-11) as the recovery action. It publishes the failed record — with the original payload, original headers, plus new headers containing exception class, message, stack trace, original topic/partition/offset — to a DLT topic. From there it can be monitored, alerted on, and replayed.

---

### Q6: Why is `AckMode.RECORD` appropriate here rather than `AckMode.BATCH`?

**Answer:**

`DefaultErrorHandler` needs precise control over which offsets get committed at which point during the retry/recovery cycle. `AckMode.RECORD` — commit after each individual record — gives it that precision.

With `AckMode.BATCH`, Spring would try to commit the entire batch's offsets together after the batch is processed. But `DefaultErrorHandler` needs to commit individual records at different times: successfully processed records should commit immediately, failed records should only commit after recovery (after all retries exhausted). Batch-level commit timing conflicts with per-record retry timing.

`AckMode.RECORD` ensures:
- A successfully processed record commits its offset immediately after returning from the listener
- A failed record's offset is committed by the error handler's recovery action after retries exhausted

Spring Kafka's documentation explicitly recommends `AckMode.RECORD` when using `DefaultErrorHandler` with record-level retry semantics. For batch listeners with batch-level error handling, there are separate `BatchErrorHandler` variants.

---

### Q7: What is `setRetryListeners()` and what are two production uses for it?

**Answer:**

`setRetryListeners(RetryListener...)` registers callbacks that fire at key points in the retry lifecycle:

```java
handler.setRetryListeners((record, ex, deliveryAttempt) -> {
    // fires before each delivery attempt (including the first)
    // deliveryAttempt: 1 = original attempt, 2 = first retry, etc.
});
```

There's also a `recovered(record, ex)` callback via the `CommonErrorHandler` interface that fires when recovery runs.

Two production uses:

**Metrics**: increment a counter for each retry attempt, tagged with exception type and topic. This feeds dashboards and alerts. "Retry rate spike on payments-topic with ConnectException → DB is flapping." Without retry listener metrics, you only see the final recovery event, not how many retries were consumed along the way.

**Distributed tracing**: on each retry attempt, propagate the original trace ID (extracted from Kafka record headers or MDC) into the retry attempt's trace span. Without this, each retry attempt appears as an independent operation in your tracing system — you can't correlate them as "3 attempts of the same delivery." The retry listener gives you the delivery attempt number and the original record to extract the trace context from.

---

## 📊 Quick Reference Card

| Term | Definition |
|------|------------|
| `DefaultErrorHandler` | Spring Kafka 2.8+ unified error handler for record and batch listeners |
| `FixedBackOff(interval, retries)` | Retry policy: fixed delay between attempts; `retries` = additional attempts after first failure |
| `addNotRetryableExceptions()` | Registers exception types that skip retries and go straight to recovery |
| Recovery action | Runs after all retries exhausted or non-retryable exception; default = log + commit past |
| `setRetryListeners()` | Hooks into retry lifecycle for metrics, tracing, alerting |
| `AckMode.RECORD` | Commit after each individual record — required for per-record retry semantics |
| `DeadLetterPublishingRecoverer` | Custom recovery action that publishes failed record to a DLT (case-11) |

---

## ✅ Self-Assessment

After studying this Q&A, you should be able to:

- Explain what `DefaultErrorHandler` replaced and why it's better
- Calculate total delivery attempts from `FixedBackOff(interval, retries)` arguments
- Describe what happens to the consumer and its partition during retries
- Classify exceptions as retryable vs non-retryable and justify each classification
- Describe the default recovery action and its two main limitations
- Explain why `AckMode.RECORD` is correct for `DefaultErrorHandler`
- Describe two production uses of `setRetryListeners()`
