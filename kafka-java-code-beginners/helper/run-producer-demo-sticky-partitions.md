## We are working here with ProducerDemoWithCallbackSwitchPartitions

### Concept: Sticky Partitioning

**What is Sticky Partitioning?**

Traditional round-robin partitioning sends each message to a different partition. Sticky partitioning batches multiple messages to the **same partition** until the batch is full, then "sticks" to a new partition.

**Why Sticky Partitioning?**
- ✅ **Higher throughput** - Fewer partition metadata requests
- ✅ **Better batching** - Larger batches = better compression
- ✅ **Lower latency** - Reduced network overhead
- ✅ **Default behavior** - Kafka 3.0+ uses sticky partitioner

**How it works:**
```
Traditional Round-Robin:          Sticky Partitioner:
Partition 0: A C E G I            Partition 0: A A A A A (batch 1)
Partition 1: B D F H J            Partition 1: B B B B B (batch 2)
Partition 2: (empty)              Partition 2: C C C C C (batch 3)

Each message → different partition  Batch of messages → same partition
```

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

### 5. Run ProducerDemoWithCallbackSwitchPartitions

#### Option A: In IntelliJ
1. Open `ProducerDemoWithCallbackSwitchPartitions.java`
2. Click the green arrow next to `main()` method
3. Select "Run ProducerDemoWithCallbackSwitchPartitions.main()"

#### Option B: Via command line (Maven)
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.producer.ProducerDemoWithCallbackSwitchPartitions"
```

---

### 6. Expected Output

```
🚀 Starting Kafka Producer with Sticky Partitioning (Docker version)
📦 Demonstrating how Kafka batches messages to partitions
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚙️  Configuration: batch.size = 400 bytes (small for demo)
📊 Sending 10 batches × 30 messages = 300 total messages
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📦 BATCH 1 of 10 starting...
   📝 Msg #  1 | Partition: 0 | Offset: 30 | Batch: 1
   📝 Msg #  2 | Partition: 0 | Offset: 31 | Batch: 1
   📝 Msg #  3 | Partition: 0 | Offset: 32 | Batch: 1
   ... (messages 4-27 omitted for brevity) ...
   📝 Msg # 28 | Partition: 0 | Offset: 57 | Batch: 1
   📝 Msg # 29 | Partition: 0 | Offset: 58 | Batch: 1
   📝 Msg # 30 | Partition: 0 | Offset: 59 | Batch: 1
   ✅ Batch 1 sent, waiting 500ms...

📦 BATCH 2 of 10 starting...
   🔄 PARTITION SWITCH: 0 → 1 at message #31
   📝 Msg # 31 | Partition: 1 | Offset: 30 | Batch: 2
   📝 Msg # 32 | Partition: 1 | Offset: 31 | Batch: 2
   📝 Msg # 33 | Partition: 1 | Offset: 32 | Batch: 2
   ... (messages 34-57 omitted for brevity) ...
   📝 Msg # 58 | Partition: 1 | Offset: 57 | Batch: 2
   📝 Msg # 59 | Partition: 1 | Offset: 58 | Batch: 2
   📝 Msg # 60 | Partition: 1 | Offset: 59 | Batch: 2
   ✅ Batch 2 sent, waiting 500ms...

📦 BATCH 3 of 10 starting...
   🔄 PARTITION SWITCH: 1 → 2 at message #61
   📝 Msg # 61 | Partition: 2 | Offset: 30 | Batch: 3
   ... (pattern continues) ...
```

---

### 7. Understanding Sticky Partitioning

#### What You'll Observe:

| Observation | Explanation |
|-------------|-------------|
| **First batch all in partition 0** | Sticky partitioner chooses a partition and "sticks" to it |
| **Partition switch at batch 2** | Batch 1 filled the buffer → new batch goes to different partition |
| **Each batch stays in ONE partition** | Messages in same batch go to same partition |
| **Offsets reset per partition** | Each partition has its own offset sequence (0,1,2...) |

#### Visual Representation:

```
Time ──────────────────────────────────────────────────────────►

Batch 1 (Messages 1-30)     Batch 2 (31-60)      Batch 3 (61-90)
┌─────────────────────┐     ┌─────────────────┐   ┌─────────────────┐
│ Partition 0         │     │ Partition 1     │   │ Partition 2     │
│ ┌─────────────────┐ │     │ ┌─────────────┐ │   │ ┌─────────────┐ │
│ │ Msg 1  Offset 0 │ │     │ │ Msg 31 Off 0│ │   │ │ Msg 61 Off 0│ │
│ │ Msg 2  Offset 1 │ │     │ │ Msg 32 Off 1│ │   │ │ Msg 62 Off 1│ │
│ │ Msg 3  Offset 2 │ │     │ │ Msg 33 Off 2│ │   │ │ Msg 63 Off 2│ │
│ │ ...             │ │     │ │ ...         │ │   │ │ ...         │ │
│ │ Msg 30 Offset 29│ │     │ │ Msg 60 Off 29│ │   │ │ Msg 90 Off 29│ │
│ └─────────────────┘ │     │ └─────────────┘ │   │ └─────────────┘ │
└─────────────────────┘     └─────────────────┘   └─────────────────┘

      Same partition!            Different partition!    Different partition!
```

---

### 8. Verify with Console Consumer

To see which partition each message went to:

```bash
docker exec -it kafka-java-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic demo_topic_example \
  --from-beginning \
  --property print.partition=true
```

**Expected output:**
```
Partition:0    Message #1 (batch:1, item:1)
Partition:0    Message #2 (batch:1, item:2)
...
Partition:0    Message #30 (batch:1, item:30)
Partition:1    Message #31 (batch:2, item:1)
Partition:1    Message #32 (batch:2, item:2)
...
```

---

### 9. Check Partition Distribution

```bash
# Get offset information per partition
docker exec -it kafka-java-broker kafka-run-class \
  --class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic demo_topic_example
```

**Expected output (with 300 messages across 3 partitions):**
```
demo_topic_example:0:100   (Batch 1,4,7,10)
demo_topic_example:1:100   (Batch 2,5,8)
demo_topic_example:2:100   (Batch 3,6,9)
```

---

### 10. Experiment: Remove batch.size setting

```java
// Comment out this line:
// properties.setProperty("batch.size", "400");
```

**What happens:** With default 16KB batch size, you'll see fewer partition switches because each batch can hold more messages.

---

## Verification Checklist

| Test | Command | Expected Result |
|------|---------|-----------------|
| Kafka running | `docker ps` | Container `kafka-java-broker` is up |
| Topic has 3 partitions | Describe topic | Shows partitions 0,1,2 |
| Producer runs | Producer logs | 300 messages sent |
| First batch in one partition | Log output | All messages 1-30 in same partition |
| Partition switches observed | Log output | "🔄 PARTITION SWITCH" messages appear |
| Even distribution | Check offsets | Each partition has ~100 messages |

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| No partition switches | Batch size too large - reduce to 400 bytes or send more messages |
| All messages in one partition | Remove linger.ms=0 or increase batch size significantly |
| Partition switches too often | Increase batch.size to 16384 (default) |
| Offsets not sequential within partition | Normal if multiple producers running concurrently |

---

## Key Takeaways

| Concept | What It Means | Why It Matters |
|---------|--------------|----------------|
| **Sticky Partitioning** | Batches stick to same partition | Higher throughput |
| **Batch Size** | 400 bytes triggers frequent switches | Control batching behavior |
| **Partition Switch** | New batch → new partition | Even distribution over time |
| **Sequential Offsets** | Within partition, offsets increment | Ordering guarantee |

---

## Comparison: Sticky vs Round-Robin

| Aspect | Round-Robin (Old) | Sticky Partitioning (New) |
|--------|-------------------|---------------------------|
| **Metadata requests** | Per message | Per batch |
| **Batch efficiency** | Poor (messages scattered) | Excellent (messages grouped) |
| **Compression ratio** | Low | High |
| **Throughput** | Lower | Higher (2-3x) |
| **Default in Kafka** | Before 2.4 | 3.0+ |

---

## Next Steps

After understanding sticky partitioning, try:
1. **ProducerDemoKeys** - See how keys override sticky partitioning
2. **Increase batch.size** to 16384 - Observe fewer partition switches
3. **Remove batch.size** (use default) - See default behavior
4. **ConsumerDemoCooperative** - See how consumers handle partitions

---

## Why This Matters in Production

Sticky partitioning is the **default behavior** in modern Kafka for good reason:

- **Real-time analytics** - Batching improves throughput for high-volume streams
- **Microservices** - Reduces network overhead between services
- **IoT data** - Groups sensor readings efficiently
- **Log aggregation** - Larger batches mean better compression

**Remember:** Even with sticky partitioning, Kafka still guarantees **ordering within a partition** and **no data loss** with appropriate acks settings!

---

## Key Learning Points

1. **Sticky Partitioning is default** in Kafka 3.0+ - no configuration needed
2. **Batch size controls switching frequency** - smaller batch = more switches
3. **Same batch = same partition** - messages stay together for efficiency
4. **Over time, distribution is even** - across many batches, partitions balance
5. **Keys override sticky partitioning** - if you need specific partition routing

---


