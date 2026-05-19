# Scenario 03 — Load Balancing with Consumer Groups

**What this scenario demonstrates:**
- Multiple consumers sharing work within the same consumer group
- Kafka automatically assigns partitions — you don't control which consumer gets which
- Rebalancing: Kafka redistributes partitions when consumers join or leave
- The ceiling rule: consumers > partitions → extra consumers sit idle

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

`topic-load-balance` needs **3 partitions** — this is what makes load balancing
observable. One partition per consumer in the base experiment.

Create it once:
```bash
docker exec -it kafka-learning-broker kafka-topics --create --topic topic-load-balance --partitions 3 --replication-factor 1 --bootstrap-server localhost:19092
```

Verify:
```bash
docker exec -it kafka-learning-broker kafka-topics --describe --topic topic-load-balance --bootstrap-server localhost:19092
```

---

## How to Run — Base Flow

You need **4 terminals** open in IntelliJ.

### Terminals 1, 2, 3 — Start Consumers (start these first)

Each consumer gets a number argument — this is just a readable label in your output.
Kafka assigns actual partitions; the number has no effect on that.

Terminal 1:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_03_load_balancing.LoadBalancingConsumer" -Dexec.args="1"
```

Terminal 2:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_03_load_balancing.LoadBalancingConsumer" -Dexec.args="2"
```

Terminal 3:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_03_load_balancing.LoadBalancingConsumer" -Dexec.args="3"
```

Wait until all three print `Subscribed — waiting for partition assignment...`
before starting the producer. Kafka needs a moment to complete the initial rebalance.

### Terminal 4 — Start Producer

```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_03_load_balancing.LoadBalancingProducer"
```

Sends 30 messages with `key=null` (round-robin distribution), then exits.

**What to observe:**
- Each consumer terminal receives messages from different partitions
- Distribution is approximately 10 messages per consumer (not always exactly equal —
  Kafka batches sends, so round-robin operates at batch level, not per-message)
- The `partition:` field in each consumer's log shows which partitions it owns

---

## Experiments

### Experiment 1 — Rebalancing on consumer leave

1. Start all 3 consumers + run the producer once
2. Press `Ctrl+C` in Terminal 2 (stop Consumer-2)
3. Watch Terminals 1 and 3 — you'll see a rebalance log line, then both consumers
   start receiving messages from partitions they didn't own before
4. Run the producer again
5. Now only 2 consumers handle all 3 partitions (one gets 2 partitions, one gets 1)

This is automatic — you did nothing in code to make it happen.

---

### Experiment 2 — The idle consumer (consumers > partitions)

1. Start all 3 consumers
2. Start a **4th consumer** in a new terminal:
   ```bash
   mvn exec:java -Dexec.mainClass="com.vbforge.scenario_03_load_balancing.LoadBalancingConsumer" -Dexec.args="4"
   ```
3. Run the producer

Consumer-4 receives **zero messages**. It joined the group but there are only 3
partitions — already fully assigned to consumers 1, 2, 3. Consumer-4 is on standby.

4. Now stop Consumer-1 (`Ctrl+C` in Terminal 1)
5. Kafka rebalances — Consumer-4 takes over Consumer-1's partition immediately

This is why you provision consumers equal to partition count, not more.

---

### Experiment 3 — Rebalancing on consumer join

1. Start only Consumer-1
2. Run the producer — Consumer-1 handles all 3 partitions alone
3. While Consumer-1 is still running, start Consumer-2 in Terminal 2
4. Watch the rebalance: Consumer-1 gives up one or two partitions to Consumer-2
5. Run the producer again — work is now split between them

---

### Monitor — Check partition assignment and lag

Run this any time while consumers are active:
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_03_load_balancing.LoadBalancingGroupMonitor"
```

It prints the exact Docker CLI commands to run. Use them to see real-time partition
assignment and consumer lag.

Direct command:
```bash
docker exec -it kafka-learning-broker \
  kafka-consumer-groups --bootstrap-server localhost:19092 \
  --group consumer-group-load-balance --describe
```

Sample output:
```
TOPIC               PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID
topic-load-balance  0          10              10              0    Consumer-1-xxx
topic-load-balance  1          10              10              0    Consumer-2-xxx
topic-load-balance  2          10              10              0    Consumer-3-xxx
```

`LAG = 0` means fully caught up. Rising LAG means consumers are slower than the producer.

---

## Stop Consumers

`Ctrl+C` in each terminal. You'll see final stats per consumer:
```
[Consumer-1] FINAL STATISTICS:
   Messages processed: 10
   Total runtime:      18234 ms
   Avg throughput:     0.55 msgs/sec
[Consumer-1] finished.
```

---

## Kafka CLI Reference

```bash
# List all topics
docker exec -it kafka-learning-broker \
  kafka-topics --list --bootstrap-server localhost:19092

# Describe topic (confirm 3 partitions)
docker exec -it kafka-learning-broker \
  kafka-topics --describe --topic topic-load-balance --bootstrap-server localhost:19092

# Watch messages live
docker exec -it kafka-learning-broker \
  kafka-console-consumer --topic topic-load-balance \
  --from-beginning --bootstrap-server localhost:19092

# Check consumer group
docker exec -it kafka-learning-broker \
  kafka-consumer-groups --bootstrap-server localhost:19092 \
  --group consumer-group-load-balance --describe
```

---

## Cleanup

```bash
# Delete topic
docker exec -it kafka-learning-broker \
  kafka-topics --delete --topic topic-load-balance --bootstrap-server localhost:19092

# Stop Kafka, keep data
docker-compose down

# Stop Kafka, wipe everything
docker-compose down -v
```

---

## What You Observed

| Concept | Where you saw it |
|---|---|
| Partition assignment | Each consumer owns specific partitions after subscribe |
| Automatic rebalancing | Experiment 1 — consumer leaves, others absorb its partitions |
| Idle consumer | Experiment 2 — 4th consumer gets no partitions (3 partitions, already full) |
| Rebalance on join | Experiment 3 — new consumer causes redistribution |
| Lag monitoring | `--describe` command shows offset lag per partition |

