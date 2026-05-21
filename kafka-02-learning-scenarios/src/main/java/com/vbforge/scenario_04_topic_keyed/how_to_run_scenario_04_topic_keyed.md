# Scenario 04 — Keyed Messages & Per-Key Ordering

**What this scenario demonstrates:**
- Message keys control which partition a message goes to: `hash(key) % numPartitions`
- Same key always hashes to the same partition → same consumer always processes it
- Ordering is guaranteed **per key**, not across all messages
- `OrderProofProducer` makes this observable with a fixed step-by-step workflow

---

## Prerequisites

- Docker running
- Kafka started from project root:
  ```bash
  docker-compose up -d
  docker-compose ps
  ```

---

## Topic Setup

`topic-keyed` needs **3 partitions** — same reason as scenario_03.
With 3 users and 3 partitions, each user typically lands on a distinct partition.

```bash
docker exec -it kafka-learning-broker \
  kafka-topics --create \
  --topic topic-keyed \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:19092
```

Verify:
```bash
docker exec -it kafka-learning-broker \
  kafka-topics --describe --topic topic-keyed --bootstrap-server localhost:19092
```

---

## Part 1 — Key Routing (KeyedProducer)

### Step 1: Start 2 consumers

Terminal 1:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_04_topic_keyed.KeyedConsumer" -Dexec.args="1"
```

Terminal 2:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_04_topic_keyed.KeyedConsumer" -Dexec.args="2"
```

Wait for both to print `Subscribed — waiting for partition assignment...`

### Step 2: Run KeyedProducer

Terminal 3:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_04_topic_keyed.KeyedProducer"
```

Sends 30 random user events across `user-123`, `user-456`, `user-789`. Exits automatically.

### What to observe

- Each consumer's output shows only a **subset of user IDs** in its key summary
- A user ID that appears in Consumer-1 never appears in Consumer-2 (and vice versa)
- Partition numbers in the producer output are consistent per key:
  `user-123` always → partition X, `user-456` always → partition Y

This is the routing guarantee: `hash("user-123") % 3` always produces the same result.

---

## Part 2 — Order Proof (OrderProofProducer)

Stops consumers if still running from Part 1, then restart fresh:

Terminal 1:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_04_topic_keyed.KeyedConsumer" -Dexec.args="1"
```

Terminal 2:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_04_topic_keyed.KeyedConsumer" -Dexec.args="2"
```

Terminal 3:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_04_topic_keyed.OrderProofProducer"
```

Sends a fixed workflow per user:
```
Step 1: Login
Step 2: Browse
Step 3: Add to Cart
Step 4: Checkout
Step 5: Logout
```

### What to observe

- In the consumer output, find all messages for a single user (e.g. `user-123`)
- Steps always appear in order: 1 → 2 → 3 → 4 → 5, regardless of how other users'
  messages interleave between them
- Messages from different users **do** interleave at the topic level (across partitions)
  but **never** within the same user's partition

---

## Key Concept: How routing works

```
hash("user-123") % 3  →  partition 0
hash("user-456") % 3  →  partition 1
hash("user-789") % 3  →  partition 2
```

Kafka uses `murmur2` hash of the key bytes. You don't control the result — Kafka
decides which partition each key maps to. What you control is that the mapping
is **stable and deterministic**: the same key always produces the same partition.

**Consequence:** if you add partitions to a topic after data has been written,
existing keys will hash to different partitions. Never add partitions to a keyed
topic in production without planning for this.

---

## Experiments

### Experiment 1 — Start 3rd consumer mid-stream

1. Run Part 1 (2 consumers + KeyedProducer)
2. While consumers are running, start Consumer-3:
   ```bash
   mvn exec:java -Dexec.mainClass="com.vbforge.scenario_04_topic_keyed.KeyedConsumer" -Dexec.args="3"
   ```
3. Run KeyedProducer again
4. Observe: Kafka rebalances — Consumer-3 takes ownership of one partition.
   After rebalance, that partition's key is now processed exclusively by Consumer-3.

### Experiment 2 — Verify key stickiness across producer runs

1. Run KeyedProducer once, note which partition each user went to
2. Stop the producer
3. Run KeyedProducer again
4. Same users → same partitions every time

### Experiment 3 — Single consumer handles all keys

1. Start only Consumer-1 (no Consumer-2)
2. Run KeyedProducer
3. Consumer-1 receives all messages from all 3 partitions — all keys appear in its summary

---

## Monitor partition assignment

```bash
docker exec -it kafka-learning-broker \
  kafka-consumer-groups --bootstrap-server localhost:19092 \
  --group consumer-group-keyed --describe
```

---

## Kafka CLI Reference

```bash
# Read all messages from beginning (useful to verify order)
docker exec -it kafka-learning-broker \
  kafka-console-consumer \
  --topic topic-keyed \
  --from-beginning \
  --property print.key=true \
  --bootstrap-server localhost:19092

# The --property print.key=true flag shows the message key alongside the value
# Output format: <key>   <value>
```

---

## Cleanup

```bash
docker exec -it kafka-learning-broker \
  kafka-topics --delete --topic topic-keyed --bootstrap-server localhost:19092

docker-compose down -v
```

---

## What You Observed

| Concept | Where you saw it |
|---|---|
| Key → partition routing | Producer output: same key, same partition every send |
| Per-key consumer affinity | Consumer key summary: each consumer owns a fixed subset of keys |
| Per-key ordering | `OrderProofProducer`: steps 1→2→3→4→5 always in order per user |
| Cross-key interleaving | Steps from different users interleave, but each user's order is intact |
| Rebalancing with keyed topics | Experiment 1: new consumer takes over a partition, inherits its keys |

