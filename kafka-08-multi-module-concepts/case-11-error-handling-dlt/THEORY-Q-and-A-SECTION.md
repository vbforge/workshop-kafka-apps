# Case 11: Q&A Document

## 🎓 Theory Section

### Core Concepts Covered

| Concept | Description |
|---------|-------------|
| **Dead Letter Topic (DLT)** | A dedicated Kafka topic that receives records that failed all processing attempts |
| **`DeadLetterPublishingRecoverer`** | Spring Kafka's built-in recovery action that publishes to DLT with diagnostic headers |
| **`kafka_dlt-*` headers** | Metadata headers added to every DLT record: origin coordinates + exception details |
| **Destination function** | `BiFunction<ConsumerRecord, Exception, TopicPartition>` — controls routing |
| **DLT consumer** | Separate `@KafkaListener` group that reads DLT records for alerting, storage, replay |
| **`@DltHandler`** | Alternative: co-locate DLT handling with the main listener on the same class |
| **Infinite loop prevention** | DLT container factory must have NO error handler |

### What `DeadLetterPublishingRecoverer` Adds to the Failed Record

```
Original record (on case-11-topic):
  key:   ORD-ABC123
  value: { orderId: "ORD-ABC123", amount: -50.0, ... }

DLT record (on case-11-topic.DLT):
  key:   ORD-ABC123       ← original key preserved
  value: { orderId: "ORD-ABC123", amount: -50.0, ... }  ← original value preserved
  headers:
    kafka_dlt-original-topic:          case-11-topic
    kafka_dlt-original-partition:      0
    kafka_dlt-original-offset:         7
    kafka_dlt-original-timestamp:      1736949000000
    kafka_dlt-original-consumer-group: case-11-consumer-group
    kafka_dlt-exception-fqcn:          com.vbforge.case11.exception.NonRetryableOrderException
    kafka_dlt-exception-message:       Invalid order data — orderId=ORD-ABC123
    kafka_dlt-exception-stacktrace:    [full stack trace bytes]
```

---

## 📝 Interview Q&A

### Q1: What is a Dead Letter Topic and why is it better than "log and skip"?

**Answer:**

A Dead Letter Topic is a dedicated Kafka topic that receives records that could not be processed successfully after all retry attempts. It's the Kafka equivalent of a dead letter queue in traditional messaging systems.

The "log and skip" alternative (default `DefaultErrorHandler` recovery): logs the failed record at ERROR level and commits the offset. The record is permanently gone from any processing path. If the failure was caused by a bug in your code, you have no way to re-process those records after deploying a fix — they're silently lost.

DLT is better for four reasons:

**Preservation**: the original record bytes and all its context are stored in Kafka with the same retention policy as any other topic. Nothing is lost.

**Observability**: DLT lag is a measurable metric. An on-call alert "DLT lag > 0 on payments-topic.DLT" fires immediately when a message fails. Log lines can get lost in noise.

**Replay path**: after a code fix is deployed, DLT records can be replayed to the original topic (or directly to a retry topic) and processed correctly. Without DLT, there's no replay path.

**Diagnostic headers**: every DLT record carries the exact exception, stack trace, original partition, and offset — complete forensic information for post-mortems and replay targeting.

---

### Q2: What headers does `DeadLetterPublishingRecoverer` add, and what is each used for in practice?

**Answer:**

`kafka_dlt-original-topic` → which topic the message originally came from. Needed when a single DLT receives records from multiple source topics (a common pattern to reduce topic count).

`kafka_dlt-original-partition` → which partition. Together with `original-offset`, uniquely identifies the original record in the source topic. Used to seek back for audit or replay.

`kafka_dlt-original-offset` → exact position. This is the forensic receipt — "we failed to process the record at exactly offset 7 of partition 0 of case-11-topic." Enables surgical re-inspection or seek-based replay.

`kafka_dlt-original-timestamp` → when the original record was produced. Useful for age-based DLT analysis: "this order has been failing for 3 hours."

`kafka_dlt-original-consumer-group` → which consumer group failed. When multiple groups consume the same source topic, this identifies which one had the problem.

`kafka_dlt-exception-fqcn` → the fully qualified class name of the exception. Enables DLT consumers to categorise failures without parsing the message: `NonRetryableOrderException` vs `ConnectException` get different handling.

`kafka_dlt-exception-message` → the exception's `.getMessage()`. The human-readable reason.

`kafka_dlt-exception-stacktrace` → full stack trace as UTF-8 bytes. Can be large. Often not logged in full — stored to a database for detailed post-mortems.

---

### Q3: Why must the DLT consumer factory have NO error handler configured?

**Answer:**

If the DLT consumer's processing fails (e.g. the database it writes to is down), and the DLT container factory has a `DefaultErrorHandler` with `DeadLetterPublishingRecoverer`, the failed DLT record would be published to `case-11-topic.DLT.DLT`. If that consumer also has a recoverer, it publishes to `case-11-topic.DLT.DLT.DLT`. This chain continues until topic names exceed Kafka's topic name length limit — a runaway failure cascade.

The correct DLT consumer contract: **if DLT processing fails, log and move on.** The record is preserved on the DLT — a human can inspect and re-process it manually. The DLT is the last safety net; there is no next net.

In practice, DLT consumers should be designed to be robust and simple. They typically do one thing: persist the record to a database or alert system. If that fails, it's an infrastructure problem (DB down, alerting system down) that requires operator intervention — not another automated retry loop.

Spring's default handler (no `setCommonErrorHandler` call) logs the exception at ERROR level and commits the offset. This is exactly the right behaviour for a DLT consumer.

---

### Q4: What is the destination function in `DeadLetterPublishingRecoverer` and when would you customise it?

**Answer:**

The destination function is a `BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition>` that determines where the failed record gets published. It receives the original failed record and the exception, and returns the target `TopicPartition`.

The default behaviour (no custom function): route to `<original-topic>.DLT`, partition 0.

You'd customise it for:

**Exception-based routing**: route `NonRetryableOrderException` failures to a `orders.DLT.schema-errors` topic (for schema team review) and `ConnectException` failures to `orders.DLT.infra-errors` (for ops team). Different teams monitor different DLTs.

**Preserving partition affinity**: route to the same partition number as the original record. This keeps relative ordering within the DLT consistent with the source topic — useful when DLT replay needs to preserve per-key ordering.

```java
(record, exception) -> new TopicPartition(
    record.topic() + ".DLT",
    record.partition()  // preserve partition affinity
)
```

**Multiple source topics, single DLT**: when many topics share one DLT, embed the source topic in the DLT record's key rather than routing by topic name.

**Environment-based routing**: in a multi-environment setup, route to `prod.DLT` or `staging.DLT` based on a record header indicating environment.

---

### Q5: What is `@DltHandler` and how does it differ from a separate `@KafkaListener` on the DLT topic?

**Answer:**

`@DltHandler` is a Spring Kafka annotation that co-locates DLT handling with the main `@KafkaListener` on the same class. When Spring Kafka sees both annotations, it automatically wires DLT records from `<topic>.DLT` to the `@DltHandler` method without requiring a separate topic string, group ID, or container factory configuration.

```java
@KafkaListener(topics = "orders")
public void processOrder(OrderMessage order) { ... }

@DltHandler
public void handleOrderDlt(OrderMessage order,
        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
        @Header("kafka_dlt-original-offset") long originalOffset) {
    log.error("DLT: order={} from {} at offset {}", order.getOrderId(), topic, originalOffset);
}
```

Differences vs separate `@KafkaListener` on the DLT topic:

**Co-location**: `@DltHandler` keeps the main processing logic and failure handling in the same class — good for small services where the DLT handling is trivial.

**Auto-wiring**: Spring infers the DLT topic name and routes automatically. With a separate `@KafkaListener`, you must specify `topics = "${kafka.topic.dlt}"` explicitly.

**Less flexibility**: `@DltHandler` uses the same container factory configuration as the main listener by default, which can create issues (same group ID, same error handler). A separate `@KafkaListener` with a dedicated `dltContainerFactory` gives full control — different group, no error handler, different concurrency.

In case-11 we use a separate class for clarity. In a real codebase with a simple DLT action (log + alert), `@DltHandler` is often the cleaner choice.

---

### Q6: How do you replay records from a DLT back to the original topic after a code fix?

**Answer:**

There is no built-in Spring Kafka "replay" mechanism — you implement it as a separate operation. The approaches from simplest to most robust:

**Manual replay via console tools**: for small DLTs in development:
```bash
kafka-console-consumer --topic orders.DLT --from-beginning | \
kafka-console-producer --topic orders
```
Simple but loses the original key and headers.

**Admin replay endpoint**: write a REST endpoint on the DLT consumer service that reads from the DLT (using a raw `KafkaConsumer`) and re-publishes each record to the original topic using a `KafkaTemplate`, preserving the original key. Trigger it manually after a fix deployment.

**Replay topic pattern**: instead of re-publishing to the original topic (which could cause ordering issues with live traffic), publish to an `orders.REPLAY` topic that the main consumer also subscribes to. The replay topic is processed at lower priority and keeps live traffic unaffected.

**Kafka Streams re-routing**: write a small Streams job that reads `orders.DLT`, filters by `kafka_dlt-original-topic` header (in case the DLT is shared), and forwards to the original topic. Can be automated as a post-deploy step.

Key consideration when replaying: the original record on the DLT has the `kafka_dlt-original-offset` header. If you need to verify you're replaying the right record, compare that offset against the source topic. Also ensure your consumer logic is idempotent — the same order might have been partially processed before failing.

---

### Q7: What production observability should you build around a DLT?

**Answer:**

DLT observability is the difference between knowing about a failure in seconds and discovering it when a customer complains two days later.

**Lag alert**: monitor `case-11-topic.DLT` consumer group lag. Lag > 0 = at least one failed message is waiting. Set a Prometheus/Micrometer alert: `kafka_consumer_fetch_manager_records_lag > 0 AND topic contains ".DLT"`. Page on-call immediately.

**Exception classification metrics**: the DLT consumer should emit a counter tagged with `kafka_dlt-exception-fqcn`. Dashboard: "Which exception types are causing DLT routing?" `NonRetryableOrderException` spikes = bad data in production. `ConnectException` spikes = infrastructure problem.

**Volume trending**: track DLT records per minute over time. A sudden spike indicates a new deployment broke something. A slow steady increase indicates data quality degradation.

**Age of oldest DLT record**: how long has the oldest unprocessed DLT record been sitting there? Records aging past an SLA (e.g. 1 hour for payment failures) should trigger escalation.

**DLT content sampling**: periodically log or store a sample of DLT record payloads for pattern analysis — "80% of DLT records have amount=-1, it's a producer bug."

**Replay audit trail**: when replaying, log the DLT record's `original-offset`, the re-published offset, and the result. This creates a traceable chain: original failure → DLT → replay → success/failure.

---

## 📊 Quick Reference Card

| Term | Definition |
|------|------------|
| DLT | Dead Letter Topic — receives records that failed all processing attempts |
| `DeadLetterPublishingRecoverer` | Spring recovery action that publishes to DLT with `kafka_dlt-*` headers |
| Destination function | `(record, exception) -> TopicPartition` — where failed records are routed |
| `kafka_dlt-original-offset` | Original record's partition offset — enables surgical audit and replay |
| `kafka_dlt-exception-fqcn` | Exception class name — enables programmatic failure categorisation |
| `@DltHandler` | Co-locates DLT handling with main listener; auto-routes DLT records to the method |
| DLT lag | Number of unprocessed DLT records — primary production alert metric |
| No error handler on DLT factory | Prevents DLT→DLT-of-DLT infinite routing loop |

---

## ✅ Self-Assessment

After studying this Q&A, you should be able to:

- Explain why DLT is better than "log and skip" — preservation, observability, replay, diagnostics
- List the `kafka_dlt-*` headers and describe what each is used for in production
- Explain why the DLT consumer factory must have no error handler configured
- Customise the destination function for exception-based DLT routing
- Compare `@DltHandler` vs a separate `@KafkaListener` and choose the appropriate one
- Describe three approaches to replaying DLT records after a code fix
- Define four production observability signals to build around a DLT
