# Case 07: Q&A Document

## 🎓 Theory Section

### Core Concepts Covered

| Concept | Description |
|---------|-------------|
| **Consumer group** | A named set of consumers sharing a `group.id`; Kafka treats them as one logical consumer |
| **Partition assignment** | Kafka distributes a topic's partitions across all consumers in a group |
| **Fan-out** | Multiple groups on the same topic — each group gets every message independently |
| **Independent offsets** | Each group maintains its own committed offset per partition |
| **Group coordinator** | A broker elected to manage group membership, heartbeats, and rebalances |
| **Rebalance** | Redistribution of partitions when group membership changes |
| **`ConsumerRebalanceListener`** | Callback interface invoked before/after partition assignment changes |

### Group Semantics: Competing vs Fan-Out

```
Topic: order-events  (3 partitions)
                    │
        ┌───────────┼───────────┐
        │           │           │
  analytics-group  audit-group  notify-group
  (gets ALL msgs) (gets ALL msgs) (gets ALL msgs)
        │
  ┌─────┴─────┐
 C-1          C-2         C-3
(partition 0) (partition 1) (partition 2)
  ↑ within one group, partitions are SPLIT across consumers
```

---

## 📝 Interview Q&A

### Q1: What is a Kafka consumer group and what problem does it solve?

**Answer:**

A consumer group is a set of consumer instances that share a `group.id` and collectively consume a topic together. Kafka treats the entire group as a single logical consumer.

It solves two distinct problems depending on how you use it:

**Horizontal scaling** (one group, many consumers): a topic with 12 partitions and a group with 4 consumers → each consumer owns 3 partitions. You process 4x faster than a single consumer. Add more consumers (up to 12) to scale further. This is the competing-consumers pattern — work is divided.

**Fan-out** (multiple groups, same topic): an order event needs to update analytics, write an audit log, and trigger a notification. Three separate groups, each with its own `group.id`, each receives every message independently. The groups don't know about each other. This is the publish-subscribe pattern — every subscriber gets everything.

The key insight: the `group.id` string is what determines which pattern you're implementing. Same `group.id` = competing. Different `group.id` = fan-out.

---

### Q2: How does Kafka assign partitions to consumers within a group?

**Answer:**

When consumers with the same `group.id` connect to Kafka, the group's **coordinator broker** runs a partition assignment algorithm. The default is `RangeAssignor`:

1. Sort partitions numerically: [0, 1, 2, 3, 4]
2. Sort consumers alphabetically by their client ID
3. Divide partitions evenly: with 5 partitions and 2 consumers → consumer-A gets [0, 1, 2], consumer-B gets [3, 4]

Other assignors:
- `RoundRobinAssignor`: distributes one partition at a time in round-robin order — more even spread across multiple topics
- `StickyAssignor`: tries to preserve existing assignments during rebalance, minimising movement — reduces the rebalance cost
- `CooperativeStickyAssignor`: same as Sticky but uses incremental cooperative rebalancing (Kafka 2.4+) — consumers only release the partitions that are being moved, not all of them. This eliminates the stop-the-world pause of classic rebalancing.

In Spring Kafka you configure this via `ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG`.

---

### Q3: What triggers a consumer group rebalance, and what is its impact?

**Answer:**

Triggers:
- A consumer joins the group (new instance starts, `subscribe()` called)
- A consumer leaves gracefully (`close()` called, app shuts down)
- A consumer is presumed dead: no heartbeat within `session.timeout.ms` (default 10s)
- A consumer doesn't call `poll()` within `max.poll.interval.ms` (default 5min)
- The topic's partition count changes
- A `ConsumerRebalanceListener` calls `unsubscribe()`

Impact (classic eager rebalance):
All consumers in the group stop consuming — a "stop-the-world" pause. Every consumer releases all its partitions. The group coordinator reassigns all partitions from scratch. Consumers resume from their last committed offsets on their new partitions. The pause duration depends on the number of consumers, network latency, and whether any `onPartitionsRevoked` callbacks do slow work (like committing offsets).

This is why frequent restarts/deployments of Kafka consumer services are expensive in production — each rolling deploy triggers multiple rebalances. The `CooperativeStickyAssignor` reduces this by only moving the partitions that actually need to change.

---

### Q4: Why do we need three separate `ConcurrentKafkaListenerContainerFactory` beans — one per group?

**Answer:**

Because `group.id` is baked into the `ConsumerFactory` configuration at construction time, and `ConsumerFactory` is what the container factory uses to create consumers.

If you used a single factory with `group.id = "analytics-group"` for all three `@KafkaListener` methods, all three listeners would join the `analytics-group` group. Kafka would split the partitions across them — you'd get competing consumption (each event processed by one of the three listeners, not all three). Fan-out would be completely broken.

The factory-per-group pattern makes the intent explicit and enforces the correct behaviour:
```
analyticsContainerFactory → ConsumerFactory(group.id="analytics-group")
auditContainerFactory     → ConsumerFactory(group.id="audit-group")
notifyContainerFactory    → ConsumerFactory(group.id="notify-group")
```

In our `KafkaConfig`, the `buildConsumerFactory(groupId)` helper avoids repeating the full config map — it parameterises only `group.id` while keeping everything else consistent across groups.

---

### Q5: If one consumer group is far behind (high lag), does it affect other groups?

**Answer:**

No. Consumer group lag is entirely independent per group. Here's why:

Kafka is a log — it stores messages for a configurable retention period regardless of consumer state. A consumer group's "lag" is just the difference between the latest offset on a partition and the group's last committed offset on that partition. It's a number maintained in Kafka's `__consumer_offsets` internal topic. It has zero effect on the broker's behaviour toward other groups.

Group A being 10,000 messages behind does not:
- Slow down Group B's consumption
- Prevent Group B from committing its offsets
- Cause any rebalance in Group B
- Affect the broker's handling of producers

The only shared resource is broker I/O — if Group A is catching up aggressively (fetching millions of records at high throughput), it shares network bandwidth and disk I/O with other consumers. But in normal operation, lag in one group is completely invisible to others.

This independence is one of Kafka's most powerful properties — it's why you can add a new consumer group at any time, set `auto.offset.reset=earliest`, and replay the entire history without affecting any live consumer group.

---

### Q6: When should you add a new consumer group vs adding more consumers to an existing group?

**Answer:**

**Add a new consumer group** when you have a new use case that needs to process the same events independently — different business logic, different downstream system, different SLA. Each new group gets its own complete copy of all messages, its own offset tracking, its own independent lag. Examples: add an audit group, add a machine-learning feature pipeline, add a cold-archive writer.

**Add more consumers to an existing group** when the existing use case is falling behind — the lag is growing and you need more processing throughput for the same logic. Each additional consumer reduces the per-consumer partition load (up to the partition count ceiling). Examples: scale the payments-processing group from 3 to 6 consumers because order volume doubled.

The decision test: "Is this the same processing logic applied to all messages, or new logic?" Same logic → scale the group. New logic → new group.

One practical note: creating a new group in production means you need to decide `auto.offset.reset`. If the topic has 30 days of retention and you need to process historical data, use `earliest`. If you only care about new events from now on, use `latest`. This is a one-time decision per group — after the first commit, `auto.offset.reset` no longer applies.

---

### Q7: What is `ConsumerRebalanceListener` and when do you use it?

**Answer:**

`ConsumerRebalanceListener` is an interface with two callbacks:

`onPartitionsRevoked(Collection<TopicPartition> partitions)` — called **before** the consumer gives up its partitions during a rebalance. This is your window to commit any pending offsets for those partitions before they're reassigned to another consumer. If you don't commit here, the next consumer to pick up the partition will reprocess from the last committed offset.

`onPartitionsAssigned(Collection<TopicPartition> partitions)` — called **after** new partitions are assigned to this consumer. Use this to load any state associated with the partitions (e.g. initialise a local cache, load checkpoints from a database for a stateful processor).

With `@KafkaListener`, you can register a `ConsumerRebalanceListener` via the container factory or via a `@KafkaListener` attribute. With manual poll (case-06), you pass it to `consumer.subscribe(topics, listener)`.

In practice, most applications don't need a custom `ConsumerRebalanceListener` — `AckMode.BATCH` handles commit-before-revoke automatically. You need it when you're managing your own offset storage (database checkpoints), doing stateful stream processing (local aggregations per partition), or need to log/metric rebalance events for observability.

---

## 📊 Quick Reference Card

| Term | Definition |
|------|------------|
| `group.id` | The string that defines group membership — same group.id = competing consumers |
| Fan-out | Multiple groups on same topic — each group receives all messages independently |
| Rebalance | Stop-the-world reassignment of partitions across group members |
| `session.timeout.ms` | Time before Kafka declares a consumer dead due to missing heartbeats |
| `max.poll.interval.ms` | Max time between `poll()` calls before consumer is ejected from the group |
| `CooperativeStickyAssignor` | Incremental rebalance — only moves the partitions that need to change |
| `ConsumerRebalanceListener` | Callbacks invoked before/after partition assignment changes |
| `__consumer_offsets` | Kafka's internal topic where committed offsets are stored |

---

## ✅ Self-Assessment

After studying this Q&A, you should be able to:

- Explain the two problems consumer groups solve (scaling vs fan-out)
- Describe how Kafka assigns partitions to consumers and name three assignors
- List four triggers for a consumer group rebalance and explain the stop-the-world impact
- Explain why three separate `ContainerFactory` beans are required for three groups
- Confirm that one group's lag has no effect on other groups and explain why
- Decide when to add a new group vs scale an existing group
- Explain `ConsumerRebalanceListener` and two use cases that require it
