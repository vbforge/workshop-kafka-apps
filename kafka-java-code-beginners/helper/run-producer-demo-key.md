## We are working here with ProducerDemoKeys

### Concept: Key-Based Partition Routing

**How it works:** Kafka uses the formula `partition = hash(key) % numPartitions` to determine which partition a message goes to. This guarantees that messages with the **same key always go to the same partition**.

**Why it matters:**
- Maintains order for messages from the same entity (e.g., user ID, order ID)
- Enables stateful processing per key
- Allows efficient caching and aggregation

---

### 1. Start Kafka in Docker

```bash
cd kafka-java-code-beginners
docker-compose up -d
```

### 2. Wait for Kafka to be ready (30 seconds)

```bash
docker-compose logs -f kafka | grep "started"
# Press Ctrl+C when you see it's ready
```

### 3. Verify Kafka is running

```bash
docker ps | grep kafka-java-broker
```

### 4. Create the topic with 3 partitions

```bash
docker exec -it kafka-java-broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic demo_topic_example \
  --partitions 3 \
  --replication-factor 1
```

### 5. Run ProducerDemoKeys

#### Option A: In IntelliJ
1. Open `ProducerDemoKeys.java`
2. Click the green arrow next to `main()` method
3. Select "Run ProducerDemoKeys.main()"

#### Option B: Via command line (Maven)
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.producer.ProducerDemoKeys"
```

### 6. Expected Output

```
🚀 Starting Kafka Producer with Keys (Docker version)
📚 Demonstrating key-based partition routing
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📦 BATCH 1 of 2
   ✅ Key: id_1   → Partition: 0 | Offset: 0 | Batch: 1
   ✅ Key: id_2   → Partition: 1 | Offset: 1 | Batch: 1
   ✅ Key: id_3   → Partition: 2 | Offset: 2 | Batch: 1
   ✅ Key: id_4   → Partition: 0 | Offset: 3 | Batch: 1
   ✅ Key: id_5   → Partition: 1 | Offset: 4 | Batch: 1
   ✅ Key: id_6   → Partition: 2 | Offset: 5 | Batch: 1
   ✅ Key: id_7   → Partition: 0 | Offset: 6 | Batch: 1
   ✅ Key: id_8   → Partition: 1 | Offset: 7 | Batch: 1
   ✅ Key: id_9   → Partition: 2 | Offset: 8 | Batch: 1
   ✅ Key: id_10  → Partition: 0 | Offset: 9 | Batch: 1
   ✅ Batch 1 completed
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📦 BATCH 2 of 2
   ✅ Key: id_1   → Partition: 0 | Offset: 10 | Batch: 2
   ✅ Key: id_2   → Partition: 1 | Offset: 11 | Batch: 2
   ✅ Key: id_3   → Partition: 2 | Offset: 12 | Batch: 2
   ... (same partition mapping as batch 1)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🏁 Producer finished
🎯 Key insight: Same key → Same partition across all batches!
```

### 7. Verify Key-Based Partitioning (3 different ways)

#### Method A: Using Console Consumer with Key/Partition Print

```bash
docker exec -it kafka-java-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic demo_topic_example \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true
```

**Expected output:**
```
Key:id_1    Partition:0    Hello from id_1 - batch 1
Key:id_2    Partition:1    Hello from id_2 - batch 1
Key:id_1    Partition:0    Hello from id_1 - batch 2  (same partition!)
Key:id_2    Partition:1    Hello from id_2 - batch 2  (same partition!)
```

#### Method B: Using Conduktor UI

1. Open http://localhost:8085
2. Navigate to "Topics" → "demo_topic_example"
3. Click "Browse Messages"
4. Filter by partition to see that each key always appears in the same partition

#### Method C: Check Partition Distribution

```bash
# Get offset information per partition
docker exec -it kafka-java-broker kafka-run-class \
  --class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic demo_topic_example
```

**Expected output (shows messages evenly distributed across partitions):**
```
demo_topic_example:0:7
demo_topic_example:1:7
demo_topic_example:2:6
```

### 8. Visual Understanding of Key Routing

```
┌─────────────────────────────────────────────────────────────────┐
│                        KAFKA TOPIC                              │
│                   demo_topic_example                            │
│                                                                 │
│  Partition 0          Partition 1          Partition 2          │
│  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐      │
│  │ id_1 (msg1) │      │ id_2 (msg1) │      │ id_3 (msg1) │      │
│  │ id_4 (msg1) │      │ id_5 (msg1) │      │ id_6 (msg1) │      │
│  │ id_7 (msg1) │      │ id_8 (msg1) │      │ id_9 (msg1) │      │
│  │ id_10(msg1) │      │             │      │             │      │
│  │             │      │             │      │             │      │
│  │ id_1 (msg2) │      │ id_2 (msg2) │      │ id_3 (msg2) │      │
│  │ id_4 (msg2) │      │ id_5 (msg2) │      │ id_6 (msg2) │      │
│  └─────────────┘      └─────────────┘      └─────────────┘      │
│                                                                 │
│  Same key ALWAYS → Same partition                               │
└─────────────────────────────────────────────────────────────────┘
```

---

## Verification Checklist

| Test | Command | Expected Result |
|------|---------|-----------------|
| Kafka running | `docker ps` | Container `kafka-java-broker` is up |
| Topic exists with 3 partitions | `docker exec -it kafka-java-broker kafka-topics --bootstrap-server localhost:9092 --describe --topic demo_topic_example` | Shows partitions 0,1,2 |
| Producer runs successfully | Producer logs | Shows 20 messages sent, all with ✅ |
| Same key → same partition | Check logs for id_1 across batches | Both batches show same partition number |
| Different keys may differ | Compare id_1 and id_2 partitions | Can be different (hash distribution) |

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `UNKNOWN_TOPIC_OR_PARTITION` | Topic not created → run step 4 to create it |
| All messages go to same partition | Check if topic has multiple partitions. If only 1 partition, all keys go there |
| Key not showing in logs | Verify `print.key=true` in consumer command |
| Producer shows ❌ for same key | Check if partition count changed. Delete and recreate topic with same partition count |
| `Connection refused` | Kafka not started → `docker-compose up -d` |

---

## Key Learning Outcomes

| Concept | Demonstrated |
|---------|--------------|
| Key hashing | `partition = hash(key) % partitions` |
| Deterministic routing | Same key always → same partition |
| Partition stability | Even across different batches/sessions |
| Ordering guarantee | Within a partition, messages are ordered by key |

---

## Experiment: Test What Happens When You Change Partition Count

```bash
# 1. Delete existing topic
docker exec -it kafka-java-broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --delete --topic demo_topic_example

# 2. Recreate with DIFFERENT partition count (e.g., 4)
docker exec -it kafka-java-broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --topic demo_topic_example \
  --partitions 4 --replication-factor 1

# 3. Run ProducerDemoKeys again
# OBSERVE: Same keys now go to DIFFERENT partitions!
# Reason: partition = hash(key) % 4 (was % 3 before)
```

---



