# Case 12: Q&A Document

## 🎓 Theory Section

### Core Concepts Covered

| Concept | Description |
|---------|-------------|
| **Global `DefaultErrorHandler`** | Single `@Bean` wired into every container factory — uniform retry policy and exception classification across the entire application |
| **Factory builder pattern** | `buildFactory(groupId)` private helper — DRY construction of multiple identical factories, differing only by consumer group |
| **Shared DLT** | One DLT topic receives failed records from multiple source topics; routing identity preserved via `kafka_dlt-original-topic` header |
| **`kafka_dlt-original-topic` routing** | DLT consumer branches on this header to apply topic-specific handling without needing separate DLT consumers |
| **Cross-cutting retry listener** | `setRetryListeners()` on the global handler logs retry attempts for every topic in one place |
| **Non-retryable classification** | `addNotRetryableExceptions()` called once — covers all listeners simultaneously |
| **Global vs per-factory trade-off** | Global = operational simplicity + uniformity; per-factory = independent tuning flexibility |

### The Global Handler Architecture

```
                         KafkaConfig
                              │
              ┌───────────────┼───────────────┐
              │               │               │
    globalRecoverer()         │         globalErrorHandler()
    routes ALL failures       │         retries + non-retryable
    to case-12-global.DLT     │         classification
              │               │               │
              └───────────────┼───────────────┘
                              │
                    buildFactory(groupId)
                              │
              ┌───────────────┼───────────────┐
              │               │               │
     ordersFactory    paymentsFactory  notificationsFactory
              │               │               │
       OrdersConsumer  PaymentsConsumer  NotificationsConsumer
```

**One change to `globalErrorHandler()` → three listeners updated simultaneously.**

### Shared DLT Header Routing

```
case-12-payments-topic  ──fails──►  globalRecoverer  ──publishes──►  case-12-global.DLT
                                                                              │
                                    kafka_dlt-original-topic: case-12-payments-topic
                                    kafka_dlt-original-offset: 4
                                    kafka_dlt-exception-fqcn: java.lang.RuntimeException
                                                                              │
                                                                    GlobalDltConsumer
                                                                              │
                                                          if "payments" → page finance on-call
                                                          if "orders"   → alert order team
                                                          if "notif..."  → low priority log
```

---

## 📝 Interview Q&A

### Q1: What problem does a global error handler solve, and when did it become necessary?

**Answer:**

In case-11, the `DefaultErrorHandler` bean was created inside `kafkaListenerContainerFactory()` — tightly coupled to one factory. That's fine for a single listener. The moment you have two or more topics each needing the same retry policy, you face a choice:

**Option A — Copy-paste**: create `errorHandler()`, `errorHandlerPayments()`, `errorHandlerNotifications()` — identical configuration in three places. Now changing the retry multiplier requires editing three methods. Forgetting to update one means silently inconsistent behaviour: payments retry for 5s but notifications only retry for 2s because someone forgot.

**Option B — Global handler**: define `DefaultErrorHandler` as a standalone `@Bean`, inject it via `factory.setCommonErrorHandler(globalErrorHandler())` in every factory. One `@Bean`, one place to change, zero inconsistency risk.

The global handler pattern becomes necessary the moment you have N > 1 listeners that should share error handling behaviour. In a real service with 5-10 topic consumers, this is essentially always the right starting point.

The case-12 architecture goes one step further with the `buildFactory(groupId)` helper — even the factory construction itself is deduplicated. Adding a fourth listener takes three lines: new `@Bean`, `return buildFactory("case-12-invoices-group")`, and a `@KafkaListener` on the consumer class.

---

### Q2: What is the factory builder pattern here, and what are its trade-offs?

**Answer:**

`buildFactory(String groupId)` is a private method that takes only what varies between factories (the consumer group ID) and returns a fully configured `ConcurrentKafkaListenerContainerFactory`. All shared config — deserializer, ack mode, concurrency, error handler — lives in one place.

```java
private ConcurrentKafkaListenerContainerFactory<String, Object> buildFactory(String groupId) {
    // ... all common config ...
    factory.setCommonErrorHandler(globalErrorHandler());  // one line, applies to all
    return factory;
}

@Bean public ConcurrentKafkaListenerContainerFactory<String, Object> ordersContainerFactory()       { return buildFactory(ordersGroupId); }
@Bean public ConcurrentKafkaListenerContainerFactory<String, Object> paymentsContainerFactory()     { return buildFactory(paymentsGroupId); }
@Bean public ConcurrentKafkaListenerContainerFactory<String, Object> notificationsContainerFactory(){ return buildFactory(notificationsGroupId); }
```

**Benefits:**
- Adding a new listener: one new `@Bean` method, one `buildFactory()` call, done
- Config changes propagate to all consumers automatically
- Impossible to have a factory that accidentally skips the error handler

**Trade-offs:**
- All consumers share identical config — you can't give payments a longer retry budget without breaking out of the pattern
- If one topic genuinely needs different deserialization (e.g. a different value type), the helper needs a parameter or an override — it starts to get messy
- Less visible to a reader who's debugging — they have to find the `buildFactory` method to understand the factory config

The pattern is correct when all topics legitimately share the same operational contract. It's the wrong choice when different topics have different SLAs — at that point, explicit separate factory methods with clearly named handlers are better even if more verbose.

---

### Q3: How does a shared DLT consumer distinguish which source topic a failed record came from?

**Answer:**

`DeadLetterPublishingRecoverer` automatically adds `kafka_dlt-original-topic` as a header to every DLT record. The value is the original topic name as a UTF-8 byte array.

In the DLT consumer, you extract it:

```java
var header = record.headers().lastHeader("kafka_dlt-original-topic");
String sourceTopic = new String(header.value(), StandardCharsets.UTF_8);
```

Then branch:

```java
if (sourceTopic.contains("payments"))       { /* URGENT — page finance on-call */ }
else if (sourceTopic.contains("orders"))    { /* alert order team */ }
else if (sourceTopic.contains("notif"))     { /* low priority */ }
```

This is the key advantage of the shared DLT over per-topic DLTs: you get cross-topic failure correlation in one consumer. If `orders` AND `payments` DLT records start arriving simultaneously, that's a strong signal of a shared infrastructure issue (DB down, schema change) rather than two independent bugs. With separate DLT consumers for each topic, you'd only discover the correlation when a human looked at multiple dashboards.

The trade-off: the shared DLT consumer becomes more complex over time as it accretes per-topic branching logic. If the handling for each topic is substantial (write to different tables, call different APIs), separate DLT consumers per topic may be cleaner despite the operational overhead.

---

### Q4: What is a `RetryListener` and what can you do with it in production?

**Answer:**

`RetryListener` is an interface with a single method: `onNextAttempt(ConsumerRecord<?, ?> record, Exception ex, int deliveryAttempt)`. You register it on `DefaultErrorHandler` via `setRetryListeners()`. It fires before each retry attempt.

In case-12:
```java
handler.setRetryListeners((record, ex, attempt) ->
    log.warn(">>> [GLOBAL-HANDLER] attempt={} topic={} offset={} ex={}",
             attempt, record.topic(), record.offset(), ex.getClass().getSimpleName())
);
```

Basic use — but in production you can do much more:

**Metrics**: emit a `retry_attempt_total` counter tagged with `topic` and `exception_type`. A dashboard showing "payments topic: avg 2.3 retry attempts per message" spots degraded services before they cause full failures.

**Alerting threshold**: if `deliveryAttempt >= 3`, send a Slack alert: "payments consumer is consistently failing — intervention may be needed before budget exhausted."

**Audit trail**: persist each retry attempt to a database with timestamp, topic, offset, and exception. After an incident, you can reconstruct the exact failure timeline.

**Adaptive backoff**: you could theoretically modify backoff behaviour based on exception type — though in practice it's cleaner to handle this via separate non-retryable exception registration.

**Circuit breaker coordination**: after N retries on the same topic, set a flag that causes new messages from that topic to fast-fail to DLT — preventing retry storms from blocking the consumer thread.

---

### Q5: When should you NOT use a global error handler and stick with per-factory handlers?

**Answer:**

Global = uniformity. If you genuinely need non-uniformity, per-factory is correct.

**Different retry budgets by topic SLA**: `payments-topic` processes financial transactions — you want 10 retries with up to 30s total because a transient DB hiccup shouldn't lose a payment. `notifications-topic` sends marketing emails — 2 retries, 3s budget, failures are low-cost. A global handler would give both topics the same budget, which is either too aggressive for payments or too lenient for notifications.

**Different exception classifications**: `OutOfStockException` is non-retryable on the inventory topic but should be retried on the orders topic (inventory might replenish). A global `addNotRetryableExceptions(OutOfStockException.class)` would affect both — wrong.

**Different DLT destinations**: if payments must go to `payments.DLT` (monitored by finance, with strict retention) and notifications can go to `notifications.DLT` (monitored by marketing, shorter retention), a shared global recoverer with a single DLT destination can't express this cleanly.

**Different concurrency per topic**: high-throughput `events-topic` needs concurrency=8, low-volume `config-topic` needs concurrency=1. These live in the factory, not the error handler, but they force separate factory methods anyway — at that point you might as well own the error handler per factory for clarity.

The rule: start with a global handler. Split to per-factory exactly when you find a genuine divergence in requirements, not speculatively.

---

### Q6: What happens if you wire the same `DefaultErrorHandler` bean into the DLT factory?

**Answer:**

Infinite routing loop — the classic DLT-of-DLT problem.

Sequence:
1. `GlobalDltConsumer.consumeFromDlt()` is called with a failed record.
2. The DLT consumer's processing fails (e.g., the persistence layer it writes to throws).
3. The container's `DefaultErrorHandler` catches the exception.
4. `DeadLetterPublishingRecoverer` publishes the record to... `case-12-global.DLT`.
5. `GlobalDltConsumer` is called again — step 2 repeats.

The loop continues until one of:
- The consumer is killed manually
- The exception happens to succeed on a later attempt (if retries are enabled)
- Kafka topic name length limits are hit if Spring creates `case-12-global.DLT.DLT.DLT...`

In practice, `case-12-global.DLT` is the correct DLT destination because `globalRecoverer()` hardcodes it — so records just pile up on the same topic, growing DLT lag infinitely.

The fix is exactly what the code shows: **no `setCommonErrorHandler()` call on `dltContainerFactory()`**. Spring's default behaviour for a container with no error handler is `LoggingErrorHandler` — logs at ERROR and commits past the record. DLT processing failures become observable (ERROR log) but don't cascade.

This is the correct contract for a DLT consumer: it is the last safety net. There is no net below it.

---

### Q7: How would you migrate from per-factory handlers (case-11 style) to a global handler in a live production service?

**Answer:**

This is a real migration scenario — a service that grew from 1 topic to 5 topics, each with their own copy-pasted `DefaultErrorHandler`.

**Step 1 — Audit divergence.** Before merging, list every factory's error handler config:
- What's the retry policy? (usually identical — good)
- What non-retryable exceptions are registered? (may differ — problem)
- What's the recoverer destination? (per-topic DLTs vs shared — key decision)

If the retry policies are already identical across all factories, the migration is safe. If some have diverged, align them first (separate PR, observe in production).

**Step 2 — Extract the global handler.** Create `globalErrorHandler()` and `globalRecoverer()` as standalone `@Bean` methods with the agreed-upon config. Don't wire it yet.

**Step 3 — Replace one factory first.** Pick the lowest-risk topic (not payments). Replace its inline handler with `factory.setCommonErrorHandler(globalErrorHandler())`. Deploy and monitor DLT lag and retry metrics for 24h. No regressions → proceed.

**Step 4 — Roll out topic by topic.** Replace one factory per deploy cycle. If a regression appears, you know exactly which topic was just migrated.

**Step 5 — Delete the orphaned per-topic handler beans.** After all factories are migrated, delete the old `errorHandlerOrders()`, `errorHandlerPayments()` etc. This is the point where the config is truly global and the maintenance benefit is realised.

**Step 6 — Extract `buildFactory()`.** Once all factories share the same handler, the factory methods themselves are near-identical. Extract the builder helper to eliminate the remaining duplication.

The key production rule: never change retry policy and change factory wiring in the same deploy. One variable at a time.

---

### Q8: How does the `kafka_dlt-original-offset` header enable surgical replay from a shared DLT?

**Answer:**

`kafka_dlt-original-offset` (a long value encoded as UTF-8 bytes) tells you the exact position of the failed record in the source topic. Combined with `kafka_dlt-original-topic` and `kafka_dlt-original-partition`, you have a precise coordinate: `(topic, partition, offset)`.

For replay from a shared DLT, you need this coordinate for two reasons:

**Filtering by source**: the DLT may contain failed records from orders, payments, and notifications. You only want to replay orders records after fixing the orders bug. Filter by `kafka_dlt-original-topic` header.

**Deduplication**: if the bug caused 1,000 messages to fail but the producer sent each once, you replay exactly 1,000 records. If the same message failed, was re-sent, and failed again, the DLT may contain duplicates. Use `original-topic + original-partition + original-offset` as a dedup key — the same offset won't appear twice in a correct system.

**Targeted re-inspection**: when an incident report says "order ORD-ABC123 was not processed on 2024-01-15 between 14:00-15:00", you can scan the DLT for records with matching `original-offset` ranges (use `kafka_dlt-original-timestamp` header) and find exactly which record was lost.

**Replay implementation pattern:**
```java
// Consumer that reads from DLT and republishes to source topic
ConsumerRecord<String, GenericMessage> dltRecord = ...;
String sourceTopic  = extractHeader(dltRecord, "kafka_dlt-original-topic");
String sourceOffset = extractHeader(dltRecord, "kafka_dlt-original-offset");

log.info("Replaying original-offset={} back to {}", sourceOffset, sourceTopic);
kafkaTemplate.send(sourceTopic, dltRecord.key(), dltRecord.value()).get();
```

The original record's key is preserved by `DeadLetterPublishingRecoverer`, so partition routing via key hashing is identical to the original send — the replayed record lands on the same partition as the original, preserving ordering guarantees.

---

## 📊 Quick Reference Card

| Term | Definition |
|------|------------|
| Global `DefaultErrorHandler` | Single `@Bean` shared by all container factories |
| `buildFactory(groupId)` | Private helper that constructs a factory — only group ID varies |
| `setCommonErrorHandler()` | Method on container factory that wires in the error handler |
| Shared DLT | One topic receives failures from multiple source topics |
| `kafka_dlt-original-topic` | Header that identifies which source topic a DLT record came from |
| `RetryListener` | Hook called before each retry — used for metrics, alerting, audit |
| Global vs per-factory | Global = uniformity; per-factory = independent SLA tuning |
| DLT factory: no handler | Prevents DLT → DLT infinite routing loop |

---

## ✅ Self-Assessment

After studying this Q&A, you should be able to:

- Explain why a global handler is better than copy-pasted per-factory handlers
- Describe the `buildFactory()` pattern and its trade-offs
- Explain how the shared DLT consumer branches by source topic using `kafka_dlt-original-topic`
- List three production uses for `RetryListener` beyond simple logging
- Describe three scenarios where per-factory handlers are the correct choice
- Explain what happens when the DLT factory has an error handler (infinite loop path)
- Outline a safe migration path from per-factory to global error handling in production
- Explain how `kafka_dlt-original-offset` enables surgical replay from a shared DLT
