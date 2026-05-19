# Scenario 02 — Demo App (Quick Intro)

**What this scenario demonstrates:**
- Minimum viable producer → send one message, read confirmation
- Multi-message producer → watch round-robin partition distribution
- Keyed producer → same key always lands on the same partition
- Basic consumer with graceful shutdown (ShutdownHook + `wakeup()`)
- Broadcast vs load-balance behavior with two consumer groups

---

## Prerequisites

- Docker running
- Kafka started from project root:
  ```bash
  docker-compose up -d
  docker-compose ps   # confirm kafka-learning-broker is healthy
  ```

- To check which topics are already exist:
  ```bash
  winpty docker exec -it kafka-learning-broker kafka-topics --bootstrap-server localhost:9092 --list
  ```
- expected output:
  ```
  __consumer_offsets
  topic-simple
  topic-test-connectivity-kafka
  ```

---

## Topic Setup

`topic-demo` needs **3 partitions** so the partition distribution experiments are meaningful. With `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` in docker-compose,
auto-create gives us only 1 partition. Create it manually once:

```bash
winpty docker exec -it kafka-learning-broker kafka-topics --create --topic topic-demo --partitions 3 --replication-factor 1 --bootstrap-server localhost:19092
```

Verify:
```bash
winpty docker exec -it kafka-learning-broker kafka-topics --describe --topic topic-demo --bootstrap-server localhost:19092
```

---

## How to Run — Base Flow

Open terminals in IntelliJ (`Alt+F4` or the Terminal tab). One terminal per process.

### Terminal 1 — Start Consumer

```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_02_demo_app.MyConsumer"
```

Expected:
```
=== Quick Demo Consumer ===
Subscribed to topic: topic-demo | group: consumer-group-topic-demo
Listening for messages... (Ctrl+C to stop)
```

### Terminal 2 — Choose a Producer

**Option A — Single message:**
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_02_demo_app.MyProducer"
```

**Option B — Multiple messages (observe partition distribution):**
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_02_demo_app.MultiMessageProducer"
```

**Option C — Keyed messages (observe same key → same partition):**
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_02_demo_app.KeyedMessageProducer"
```

Producers exit automatically after sending.

### Stop Consumer

Press `Ctrl+C` in Terminal 1.

> **Why terminal and not the IDE Stop button?**
> Stop button sends SIGKILL — JVM dies instantly, shutdown hook never runs.
> `Ctrl+C` sends SIGINT — hook fires, `wakeup()` interrupts `poll()`, consumer closes cleanly.

---

## Experiments

### Experiment 1 — Broadcast vs Load Balance

**Goal:** understand how group ID controls message delivery.

**Broadcast (all consumers get all messages):**

Terminal 1:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_02_demo_app.MyConsumer"
```
Terminal 2:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_02_demo_app.MyConsumer2"
```
Terminal 3:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_02_demo_app.MultiMessageProducer"
```

`MyConsumer` uses `consumer-group-topic-demo`.
`MyConsumer2` uses `consumer-group-topic-demo-2`.
Different group IDs → Kafka treats them as independent subscribers → both get every message.

**Load balance (messages shared between consumers):**

In `MyConsumer2.java`, change the group ID from `CONSUMER_GROUP_DEMO_2` to `CONSUMER_GROUP_DEMO`
(same as `MyConsumer`). Rerun both consumers + producer.

Now both consumers share partitions — each message goes to only one consumer.
With 3 partitions and 2 consumers, the split will be approximately 2:1.

---

### Experiment 2 — Send from CLI, receive in Java

Start `MyConsumer` in Terminal 1, then in Terminal 2:

```bash
winpty docker exec -it kafka-learning-broker kafka-console-producer --topic topic-demo --bootstrap-server localhost:19092
```

Type any text and press Enter. Watch Terminal 1 receive it.

---

### Experiment 3 — Java producer, CLI consumer

Start the CLI consumer first:

```bash
winpty docker exec -it kafka-learning-broker \
  kafka-console-consumer \
  --topic topic-demo \
  --from-beginning \
  --bootstrap-server localhost:19092
```

Then run any Java producer. Watch messages appear in the CLI consumer.

---

### Experiment 4 — Kafka persists messages across consumer restarts

1. Start `MyConsumer` → Terminal 1
2. Run `MultiMessageProducer` → 10 messages sent
3. Stop consumer → `Ctrl+C`
4. Run `MultiMessageProducer` again → 10 more messages sent (consumer is down)
5. Start `MyConsumer` again

Consumer receives all 20 messages — the 10 sent while it was offline are still in the log.
This demonstrates Kafka's persistent storage model (not a transient message queue).

---

## Kafka CLI — Useful Commands

```bash
# List all topics
docker exec -it kafka-learning-broker kafka-topics --list --bootstrap-server localhost:19092

# Describe topic-demo (partitions, leader, replicas)
docker exec -it kafka-learning-broker kafka-topics --describe --topic topic-demo --bootstrap-server localhost:19092

# Read all messages from the beginning
docker exec -it kafka-learning-broker kafka-console-consumer --topic topic-demo --from-beginning --bootstrap-server localhost:19092

# Check consumer group offsets and lag
docker exec -it kafka-learning-broker kafka-consumer-groups --bootstrap-server localhost:19092 --group consumer-group-topic-demo --describe
```

---

## Cleanup

```bash
# Delete topic (reset for fresh experiment)
winpty docker exec -it kafka-learning-broker kafka-topics --delete --topic topic-demo --bootstrap-server localhost:19092

# Stop Kafka, keep data
docker-compose down

# Stop Kafka, wipe everything
docker-compose down -v
```

---

## What You Observed

| Concept | Where you saw it |
|---|---|
| Synchronous send | `MyProducer` — `.get()` blocks until broker confirms |
| Round-robin partitioning | `MultiMessageProducer` — key=null, messages spread across partitions |
| Key-based routing | `KeyedMessageProducer` — same key always → same partition |
| Broadcast delivery | Experiment 1 (different group IDs) |
| Load-balanced delivery | Experiment 1 variant (same group ID) |
| Graceful shutdown | `Ctrl+C` → hook → `wakeup()` → clean close |
| Message persistence | Experiment 4 — messages survive consumer downtime |


