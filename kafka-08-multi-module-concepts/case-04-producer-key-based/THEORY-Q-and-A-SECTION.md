# Case 04: Q&A Document

## 🎓 Theory Section

### Core Concepts Covered

| Concept | Description |
|---------|-------------|
| **Partition** | An ordered, append-only log — the unit of parallelism and ordering in Kafka |
| **Message key** | Optional bytes attached to a record; used by the partitioner to decide the target partition |
| **DefaultPartitioner** | Kafka's built-in partitioner: `murmur2(keyBytes) % numPartitions` |
| **Custom partitioner** | Implements `org.apache.kafka.clients.producer.Partitioner` — explicit routing logic |
| **Null key** | No key → sticky partitioning (batches go to one partition, then rotate) |
| **Ordering guarantee** | Kafka guarantees ordering WITHIN a partition only, not across partitions |
| **`ConsumerRecord<K,V>`** | Wraps the full Kafka record: key, value, partition, offset, timestamp, headers |
| **Sticky partitioning** | Kafka ≥ 2.4 batches null-key messages to one partition before rotating |

### The Partition Routing Decision Tree

```
Message sent
    │
    ├─ Has key? ──YES──► DefaultPartitioner: murmur2(key) % numPartitions
    │                OR  CustomPartitioner: your logic
    │
    └─ No key ──────────► Sticky: fill current batch partition, then rotate
```

---

## 📝 Interview Q&A

### Q1: Why does Kafka use the message key for partitioning, and what guarantee does it provide?

**Answer:**

Kafka's only ordering guarantee is: messages within a single partition are consumed in the order they were written. There is no cross-partition ordering guarantee.

This creates a problem for event-driven systems: if you have 10 events about "user-42" (profile update, login, purchase, logout...) and they scatter across 4 partitions, a consumer might process "logout" before "login" because they came from different partitions processed by different threads.

The key solves this by ensuring all events for the same entity land in the same partition. The invariant is: `partition(key) = murmur2(key.getBytes()) % numPartitions`. Because this is a pure function, it's deterministic — same key always produces the same partition, regardless of which broker handles the request, regardless of producer restart. This makes partition routing reproducible and stateless.

In production this is used for: user events keyed on userId, order events keyed on orderId, IoT sensor readings keyed on deviceId, financial transactions keyed on accountId.

---

### Q2: What algorithm does Kafka's DefaultPartitioner use, and what are its properties?

**Answer:**

The DefaultPartitioner computes `murmur2(keyBytes) % numPartitions` where murmur2 is a non-cryptographic hash function known for good distribution uniformity and speed.

Properties:
- **Deterministic**: same key always produces the same hash, so same partition.
- **Uniform distribution**: murmur2 distributes keys evenly across the hash space, meaning partitions should be roughly equally loaded given varied keys.
- **Opaque**: you can't predict the partition from the key string without running the hash. If you need to know which partition a key maps to, you must either run the hash or observe the broker response.
- **Partition-count dependent**: the modulo operation means the result changes if you change `numPartitions`. This is the partition count change gotcha.

One nuance: in Kafka 3.x, the default partitioner was changed from `DefaultPartitioner` to `UniformStickyPartitioner` for null-key messages, and the null-key behavior changed from round-robin to sticky. For keyed messages, the behavior is unchanged.

---

### Q3: What is the production danger of increasing the partition count on a topic with keyed messages?

**Answer:**

This is one of the most common production mistakes with Kafka.

The routing function is `murmur2(key) % numPartitions`. If you have `numPartitions = 4` and user-42 hashes to partition 2, after you scale to `numPartitions = 8`, user-42 might hash to partition 6. The same key now routes to a different partition.

The consequences:
- Messages for user-42 that were in partition 2 (historical) now mix with new messages in partition 6 (current). A consumer reading partition 6 sees new user-42 events without context of prior events in partition 2.
- Ordering is broken: the consumer on partition 2 gets old events, the consumer on partition 6 gets new events. There's no single timeline for user-42 anymore.

This is irreversible — you can't "rehash" historical messages into new partitions after the fact.

The production mitigation: plan partition counts ahead of time (over-partition rather than under-partition), use compacted topics + snapshots so consumers can rebuild state from scratch, or implement a migration where you write to a new topic with the new partition count and replay history.

---

### Q4: What is sticky partitioning, and how does it differ from round-robin for null-key messages?

**Answer:**

Before Kafka 2.4, null-key messages were distributed with round-robin: message 1 → partition 0, message 2 → partition 1, message 3 → partition 2, message 4 → partition 0, and so on. Each individual message went to the next partition in rotation.

The problem: this created very small batches. Kafka's performance depends heavily on batching — sending 1000 messages in one network request is far more efficient than 1000 separate requests. Round-robin prevented batching because consecutive messages went to different partitions.

Sticky partitioning (Kafka 2.4+) changes this: the producer picks one partition and sends all messages to it until the batch is full or `linger.ms` expires. Then it rotates to the next partition. Messages still spread across partitions over time, but within a batch window they all go to one partition, enabling real batching.

The observable effect in case-04: if you send 6 null-key messages quickly (within the linger window), several might land on the same partition before it rotates. If you send them with a delay, you'll see more distribution.

---

### Q5: How do you implement a custom partitioner, and what interface must it implement?

**Answer:**

Implement `org.apache.kafka.clients.producer.Partitioner` — a pure Kafka API interface (not Spring-specific). Three methods:

`int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster)` — the routing logic. Called for every message. Returns the target partition index (0-based). You have access to the `Cluster` object to check `cluster.partitionCountForTopic(topic)` — always use this rather than hardcoding partition count, so your partitioner adapts if partitions are added.

`void close()` — called when the producer is closed. Release any resources (connections, file handles).

`void configure(Map<String, ?> configs)` — called once at producer initialization with the full producer config map. Use this to read custom partitioner config from producer properties.

To activate it: `ProducerConfig.PARTITIONER_CLASS_CONFIG → YourPartitioner.class`. Kafka instantiates it via reflection, so it must have a no-arg constructor.

Two important guards to always implement:
- Null key check: if keyBytes is null, decide a default behavior (partition 0, or throw, or hash the value) rather than NPE-ing.
- `Math.abs()` on hash codes: `String.hashCode()` can return `Integer.MIN_VALUE`, whose absolute value overflows back to negative in Java. Always `Math.abs(hash) % numPartitions`.

---

### Q6: What is `ConsumerRecord<K, V>` and when should you use it over the value type directly?

**Answer:**

`ConsumerRecord<K, V>` is Kafka's representation of a full record as received by the consumer. It wraps the payload along with all record metadata:

- `key()` — the message key (type K)
- `value()` — the deserialized payload (type V)
- `partition()` — which partition this record came from
- `offset()` — the record's position in the partition log
- `timestamp()` — when the record was written to the broker
- `headers()` — optional key-value metadata attached to the record (useful for tracing, routing hints)
- `topic()` — the topic name

Use `ConsumerRecord` when you need any of that metadata — audit logging, tracing (log offset with transaction ID), dead-letter handling (re-publish with original metadata), or filtering based on headers.

Use the value type directly (e.g. `@KafkaListener` on `MyMessageObject`) when you only care about the payload. Spring extracts `.value()` for you, keeping the consumer method clean.

For production services it's common to use `ConsumerRecord` as a habit, since you often discover you need the offset or headers only after you've already shipped the service. The overhead is zero — it's just a wrapper.

---

### Q7: If two different keys hash to the same partition, what happens?

**Answer:**

They co-exist in the same partition, and their messages interleave in arrival order. This is completely normal and expected — a partition is not owned by a single key.

The consequence: if consumer A is processing messages from partition 2, and both key="user-42" and key="user-77" hash to partition 2, consumer A processes all events for both users. Messages from user-42 are in relative order with each other (because they're in the same partition). Messages from user-77 are in relative order with each other. But user-42 and user-77 messages can interleave arbitrarily.

This is fine for most use cases — you process each message independently. It only becomes a concern if your consumer logic has cross-key dependencies (e.g. "don't process user-77's order until user-42's payment clears") — which is an application-level concern Kafka doesn't solve.

Hash collisions in partitioning are NOT a problem in the same way they are in hash maps. In hash maps, collisions degrade performance. In Kafka, "collision" (same partition) just means more messages in one partition — the log handles it trivially.

Basically:
In Kafka, **all messages with the same key** (e.g., `key-42`) are guaranteed to be **strictly ordered** within their partition. This means:

- If `key-42` produces messages **M1, M2, M3**, they will appear in the partition in that exact order: **M1 → M2 → M3**.
- Messages from **other keys** (e.g., `key-77`) can appear **between** them (e.g., **M1 → key-77's message → M2 → M3**), but the relative order of `key-42`'s messages is preserved.

So, **yes**: a message from `key-42` will always appear *after* the previous message from `key-42`, even if messages from other keys are interleaved in between.

---

### Q8: In our `KafkaConfig`, we have two `KafkaTemplate` beans. How does Spring know which one to inject where?

**Answer:**

Without `@Qualifier`, Spring would fail with `NoUniqueBeanDefinitionException: expected single matching bean but found 2`. You must tell Spring which bean to inject.

Two mechanisms:

`@Qualifier("beanName")` on the injection point — matches by the `@Bean` method name (which becomes the bean name by default). In our case: `@Qualifier("kafkaTemplate")` and `@Qualifier("customKafkaTemplate")`.

`@Primary` on one of the `@Bean` methods — marks one bean as the default when no qualifier is specified. If you use both `kafkaTemplate` and `customKafkaTemplate` throughout the codebase, and `kafkaTemplate` is the common case, annotate it with `@Primary` so unqualified injection points get the default without needing `@Qualifier`.

In our `ProducerService` we inject both in the constructor and qualify both explicitly. This is the safest approach when both beans are actively used in the same class — explicit is better than relying on `@Primary` resolution.

Note: `@RequiredArgsConstructor` from Lombok generates a constructor from all final fields, but it can't add `@Qualifier` annotations. That's why `ProducerService` uses a manual constructor instead of relying on Lombok here.

---

## 📊 Quick Reference Card

| Term | Definition |
|------|------------|
| Partition | Ordered append-only log; unit of parallelism in Kafka |
| murmur2 | Non-cryptographic hash used by Kafka's DefaultPartitioner |
| `PARTITIONER_CLASS_CONFIG` | ProducerConfig key to set a custom partitioner class |
| Sticky partitioning | Kafka 2.4+: batch null-key messages to one partition before rotating |
| `ConsumerRecord<K,V>` | Full Kafka record wrapper: key, value, partition, offset, headers |
| `@Qualifier` | Spring annotation to disambiguate between multiple beans of the same type |
| Partition count change | Changing numPartitions breaks key → partition routing for existing keys |
| `cluster.partitionCountForTopic()` | Always use this in custom partitioners — never hardcode partition count |

---

## ✅ Self-Assessment

After studying this Q&A, you should be able to:

- Explain why Kafka uses keys for partitioning and what ordering guarantee it provides
- Describe the murmur2 algorithm and its properties (deterministic, uniform, opaque)
- Explain the production danger of increasing partition count and why it can't be undone
- Describe sticky partitioning and how it improves on round-robin for null-key messages
- Implement a custom `Partitioner` from scratch (interface, three methods, null guard, abs guard)
- Choose between `ConsumerRecord<K,V>` and value-type-only listener methods
- Explain what happens when two different keys hash to the same partition
- Explain `@Qualifier` vs `@Primary` and when to use each
