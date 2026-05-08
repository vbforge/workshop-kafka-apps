## Test All Scenarios

### Table of contents

* [Scenario 1: Basic Producer-Consumer Flow](#scenario-1-basic-producer-consumer-flow)
* [Scenario 2: Key-Based Partitioning (ProducerDemoKeys)](#scenario-2-key-based-partitioning-producerdemokeys)
* [Scenario 3: Producer with Callbacks (ProducerDemoWithCallback)](#scenario-3-producer-with-callbacks-producerdemowithcallback)
* [Scenario 4: Sticky Partitioning (ProducerDemoWithCallbackSwitchPartitions)](#scenario-4-sticky-partitioning-producerdemowithcallbackswitchpartitions)
* [Scenario 5: Cooperative Rebalancing (ConsumerDemoCooperative)](#scenario-5-cooperative-rebalancing-consumerdemocooperative)
* [Scenario 6: Graceful Shutdown (ConsumerDemoWithShutdown)](#scenario-6-graceful-shutdown-consumerdemowithshutdown)
* [Quick Reference: All Test Commands](#quick-reference-all-test-commands)
* [Partition Distribution Formula](#partition-distribution-formula)


### Scenario 1: Basic Producer-Consumer Flow

**Step 1: Create topic**
```bash
docker exec -it kafka-java-broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic demo_topic_example \
  --partitions 3 \
  --replication-factor 1
```

**Step 2: Start Consumer (in Terminal 1)**
```bash
docker exec -it kafka-java-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic demo_topic_example \
  --from-beginning
```

**Step 3: Run your Java Producer**
```bash
In IntelliJ/Eclipse: Run ProducerDemo.main()
# Or via Maven:
mvn exec:java -Dexec.mainClass="com.vbforge.producer.ProducerDemo"
```

**Expected Result:** Consumer terminal shows `Hello World from Docker Kafka Producer!`

---

### Scenario 2: Key-Based Partitioning (ProducerDemoKeys)

**Concept:** Same key → Same partition every time. Kafka uses `partition = hash(key) % numPartitions`

**Step 1: Run ProducerDemoKeys**
```bash
In IntelliJ: Run ProducerDemoKeys.main()
# Or via Maven:
mvn exec:java -Dexec.mainClass="com.vbforge.producer.ProducerDemoKeys"
```

**Step 2: Observe the logs**
Look for partition assignments. Notice that:
- `id_1` always goes to same partition (e.g., partition 0)
- `id_2` always goes to same partition (e.g., partition 1)
- Same key in Batch 1 and Batch 2 → IDENTICAL partition

**Expected log output:**
```
📦 BATCH 1 of 2
   ✅ Key: id_1   → Partition: 0 | Offset: 0 | Batch: 1
   ✅ Key: id_2   → Partition: 1 | Offset: 1 | Batch: 1
   ...
📦 BATCH 2 of 2
   ✅ Key: id_1   → Partition: 0 | Offset: 10 | Batch: 2  (SAME partition!)
   ✅ Key: id_2   → Partition: 1 | Offset: 11 | Batch: 2  (SAME partition!)
```

**Step 3: Verify with console consumer (shows key and partition)**
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
Key:id_3    Partition:2    Hello from id_3 - batch 1
Key:id_1    Partition:0    Hello from id_1 - batch 2   (same partition!)
Key:id_2    Partition:1    Hello from id_2 - batch 2   (same partition!)
```

**Step 4: Verify topic has 3 partitions**
```bash
docker exec -it kafka-java-broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic demo_topic_example
```

**Expected output shows:**
```
Topic: demo_topic_example    PartitionCount: 3
    Partition: 0    Leader: 1
    Partition: 1    Leader: 1
    Partition: 2    Leader: 1
```

**Step 5: Check partition distribution (offsets per partition)**
```bash
docker exec -it kafka-java-broker kafka-run-class \
  --class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic demo_topic_example
```

**Expected output (20 messages distributed across 3 partitions):**
```
demo_topic_example:0:7   (id_1, id_4, id_7, id_10 → 7 messages)
demo_topic_example:1:7   (id_2, id_5, id_8 → 7 messages)
demo_topic_example:2:6   (id_3, id_6, id_9 → 6 messages)
```

**Key Insight:**
- Messages with `id_1` (and id_4, id_7, id_10) have hash % 3 = 0 → all in partition 0
- Messages with `id_2` (and id_5, id_8) have hash % 3 = 1 → all in partition 1
- Messages with `id_3` (and id_6, id_9) have hash % 3 = 2 → all in partition 2

**Why This Matters:**
- **Ordering:** All messages for a specific key are processed in order
- **Stateful processing:** Can maintain state per key (e.g., user session)
- **Scalability:** Different keys can be processed in parallel across partitions

---

### Scenario 3: Producer with Callbacks (ProducerDemoWithCallback)

**Concept:** Asynchronous callbacks that execute when Kafka acknowledges your message, providing metadata and error handling.

**Step 1: Run ProducerDemoWithCallback**
```bash
# In IntelliJ: Run ProducerDemoWithCallback.main()
# Or via Maven:
mvn exec:java -Dexec.mainClass="com.vbforge.producer.ProducerDemoWithCallback"
```

**Step 2: Observe the asynchronous nature**
Notice that "Message sent asynchronously" appears BEFORE the callback logs. The main thread continues while Kafka processes messages.

**Step 3: Observe metadata logging**
Each message's metadata is logged when Kafka acknowledges it:

**Expected output:**
```
📤 Message #1 sent asynchronously (waiting for callback)...
📤 Message #2 sent asynchronously (waiting for callback)...
📤 Message #3 sent asynchronously (waiting for callback)...
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Message #1 sent successfully!
   Topic:     demo_topic_example
   Partition: 2
   Offset:    0
   Timestamp: 1733456789123
   Serialized key size:   0 bytes
   Serialized value size: 42 bytes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✅ Message #2 sent successfully!
   Topic:     demo_topic_example
   Partition: 2
   Offset:    1
   Timestamp: 1733456790123
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

**Step 4: Understand what each metadata field means**

| Field | Description | Why It Matters |
|-------|-------------|----------------|
| **Topic** | Destination topic | Confirms correct routing |
| **Partition** | Which partition (0, 1, or 2) | Know where your data lives for ordering |
| **Offset** | Position within partition | Used for seeking, debugging, exactly-once |
| **Timestamp** | When Kafka received the message | Time-based processing, windows |
| **Serialized size** | Size in bytes | Monitor message size, detect anomalies |

**Step 5: Verify messages with console consumer**
```bash
docker exec -it kafka-java-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic demo_topic_example \
  --from-beginning
```

**Step 6: Test error handling (optional)**

Simulate a failure by stopping Kafka mid-send:
```bash
# Terminal 1: Run producer
mvn exec:java -Dexec.mainClass="com.vbforge.producer.ProducerDemoWithCallback"

# Terminal 2: Stop Kafka while producer is running
docker-compose stop kafka
```

**Expected error output:**
```
❌ Message #5 failed to send!
org.apache.kafka.common.errors.TimeoutException: Failed to send message
```

**Key Insights:**
1. **Asynchronous execution:** Callbacks run in background threads, not blocking main thread
2. **Per-message metadata:** Each message's partition and offset are unique
3. **Error isolation:** One failing message doesn't block others
4. **Always flush:** Without `flush()`, program may exit before callbacks execute

**Why use callbacks in production?**
- ✅ Audit logging - record exactly where each message was stored
- ✅ Dead Letter Queue - route failed messages for later processing
- ✅ Metrics collection - track success/failure rates per partition
- ✅ Exactly-once semantics - store (partition, offset) as unique ID
- ✅ Performance monitoring - measure time between send and ack

---

### Scenario 4: Sticky Partitioning (ProducerDemoWithCallbackSwitchPartitions)

**Concept:** Kafka batches multiple messages to the SAME partition until the batch fills, then "sticks" to a new partition. This improves throughput by reducing metadata requests.

**Step 1: Run ProducerDemoWithCallbackSwitchPartitions**
```bash
# In IntelliJ: Run ProducerDemoWithCallbackSwitchPartitions.main()
# Or via Maven:
mvn exec:java -Dexec.mainClass="com.vbforge.producer.ProducerDemoWithCallbackSwitchPartitions"
```

**Step 2: Observe partition switching behavior**

The producer sends 300 messages (10 batches × 30 messages) with `batch.size=400` bytes.

**Expected output:**
```
📦 BATCH 1 of 10 starting...
   📝 Msg #  1 | Partition: 0 | Offset: 30 | Batch: 1
   📝 Msg #  2 | Partition: 0 | Offset: 31 | Batch: 1
   ... (all messages 1-30 in partition 0) ...
   ✅ Batch 1 completed

📦 BATCH 2 of 10 starting...
   🔄 PARTITION SWITCH: 0 → 1 at message #31
   📝 Msg # 31 | Partition: 1 | Offset: 30 | Batch: 2
   ... (all messages 31-60 in partition 1) ...
   ✅ Batch 2 completed

📦 BATCH 3 of 10 starting...
   🔄 PARTITION SWITCH: 1 → 2 at message #61
   ... (all messages 61-90 in partition 2) ...
```

**Step 3: Understand what's happening**

| Batch | Partition | Why |
|-------|-----------|-----|
| Batch 1 | Partition 0 | Sticky partitioner chooses first partition |
| Batch 2 | Partition 1 | Batch 1 filled buffer → switches to new partition |
| Batch 3 | Partition 2 | Batch 2 filled buffer → switches again |
| Batch 4 | Partition 0 | Cycles back after all partitions used |

**Step 4: Verify with console consumer**
```bash
docker exec -it kafka-java-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic demo_topic_example \
  --from-beginning \
  --property print.partition=true | head -40
```

**Expected output shows:**
```
Partition:0    Message #1 (batch:1, item:1)
Partition:0    Message #2 (batch:1, item:2)
...
Partition:0    Message #30 (batch:1, item:30)
Partition:1    Message #31 (batch:2, item:1)  ← Switch!
Partition:1    Message #32 (batch:2, item:2)
```

**Step 5: Check distribution across partitions**
```bash
docker exec -it kafka-java-broker kafka-run-class \
  --class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 \
  --topic demo_topic_example
```

**Expected result (roughly even distribution):**
```
demo_topic_example:0:100   (about 100 messages)
demo_topic_example:1:100   (about 100 messages)
demo_topic_example:2:100   (about 100 messages)
```

**Key Insights:**

| Observation | Explanation |
|-------------|-------------|
| **Batch 1 all in partition 0** | Kafka "sticks" to one partition for efficiency |
| **Partition switches at batch boundaries** | When batch fills, next batch goes to different partition |
| **Offsets reset per partition** | Each partition has its own offset sequence (0,1,2...) |
| **Even distribution over time** | Over many batches, messages spread across partitions |

**Why Sticky Partitioning is Better:**

| Aspect | Round-Robin (Old) | Sticky Partitioning (Kafka 3.0+) |
|--------|-------------------|-----------------------------------|
| **Network overhead** | High (per-message metadata) | Low (per-batch metadata) |
| **Batch compression** | Poor (small batches) | Excellent (large batches) |
| **Throughput** | Lower | 2-3x higher |
| **Default behavior** | No | Yes |

**Experiment #1: Increase batch size**
```
// Change batch.size to default 16KB
properties.setProperty("batch.size", "16384");
```
**Result:** Fewer partition switches because each batch can hold more messages.

**Experiment #2: Add keys to override sticky partitioning**
```java
// Keys force specific partitions
ProducerRecord<String, String> producerRecord = 
    new ProducerRecord<>("demo_topic_example", "key_" + (i % 3), message);
```
**Result:** Messages with same key go to same partition, overriding sticky behavior.

---

### Scenario 5: Cooperative Rebalancing (ConsumerDemoCooperative)

**Concept:** Uses Cooperative Sticky Assignor for incremental rebalancing. Only affected partitions move when consumers join or leave.

**Step 1: Run first consumer instance (Terminal 1)**
```bash
# In IntelliJ (with Allow multiple instances enabled)
# Run ConsumerDemoCooperative.main() - Instance 1
```

**Expected output:**
```
🚀 Starting Kafka Consumer with Cooperative Sticky Assignor
✅ Subscribed to topic: demo_topic_example
💓 Heartbeat: No messages received, still polling...
```
**Assignment:** All 3 partitions `[0, 1, 2]`

**Step 2: Run second consumer instance (Terminal 2)**
```bash
# Run another instance - keep first running
```

**Expected rebalancing output (first consumer logs):**
```
⚖️ Rebalancing started (Cooperative - incremental)
   Previously owned partitions: [0, 1, 2]
   New assignment: [0, 1]  ← Kept most partitions!
```

**Second consumer logs:**
```
✅ Subscribed to topic: demo_topic_example
   New assignment: [2]  ← Received only one partition
```

**Key observation:** First consumer kept [0,1] - only partition 2 moved!

**Step 3: Run third consumer instance (Terminal 3)**
```bash
# Run third instance
```

**Expected distribution:**
```
Consumer 1 → [0]  (lost only partition 1)
Consumer 2 → [1]  (lost partition 2, gained 1)
Consumer 3 → [2]  (new consumer)
```

**Step 4: Stop second consumer (Ctrl+C in Terminal 2)**
Watch incremental rebalancing:
```
⚖️ Only partitions from stopped consumer are reassigned
Consumer 1 → [0, 1]  (gained partition 1)
Consumer 3 → [2]     (unchanged!)
```

**Notice:** Consumer 3 kept its partition! No full rebalance.

---

#### What Makes This Different from Eager Rebalancing?

| Event | Eager (Range/RoundRobin) | Cooperative Sticky |
|-------|--------------------------|---------------------|
| **2nd consumer joins** | All consumers STOP, all partitions reassigned | Only partition 2 moves |
| **3rd consumer joins** | All consumers STOP again | Only partitions 1 and 2 move |
| **Consumer leaves** | All consumers STOP | Only that consumer's partitions move |
| **Downtime per rebalance** | Several seconds | Milliseconds |
| **State preservation** | Lost | Preserved |

**Visual representation:**
```
Eager Rebalancing (Stop-the-World):
Timeline: [Consumer 1 Running] [STOP] [REASSIGN ALL] [START Consumers 1,2]
           ↑───────────────↑      ↑      ↑────────────↑      ↑
                 Work            Pause      Work resumes

Cooperative Sticky (Incremental):
Timeline: [Consumer 1 Running] [Continue Work]
                ↑                       ↑
           Consumer 2 joins → Only partition 2 moves (Consumer 1 keeps working!)
```

**Step 5: Verify with consumer group CLI**
```bash
docker exec -it kafka-java-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group my-java-application
```

Shows which consumer has which partition.

**Step 6: Test with messages**
```bash
# Send messages with keys to see routing to specific consumers
docker exec -it kafka-java-broker kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic demo_topic_example \
  --property parse.key=true \
  --property key.separator=:
```

**Key Insights:**

| Observation | Explanation |
|-------------|-------------|
| First consumer kept [0,1] | Cooperative assignor preserves existing assignments |
| Second consumer got only [2] | Only unassigned partitions move |
| No full rebalance | Incremental = minimal disruption |
| Consumers continue processing | No "stop-the-world" pause |

**Why This Matters:**
- **Large deployments** (100+ consumers) rebalance in seconds vs minutes
- **Stateful processing** (Kafka Streams) preserves local state
- **Rolling restarts** don't pause entire application
- **Auto-scaling** adds/removes consumers with minimal impact

**Experiment: Compare with eager assignor**

```
// Change assignment strategy to Range (eager)
properties.setProperty("partition.assignment.strategy", 
    "org.apache.kafka.clients.consumer.RangeAssignor");
```

**Observe difference:** Every consumer rebalance now causes ALL consumers to stop!

#### Key Learning Points

1. **Cooperative Sticky is default** in modern Kafka clients
2. **Incremental rebalancing** = only affected partitions move
3. **Sticky** = keep previous assignments when possible
4. **No stop-the-world** = consumers continue processing
5. **Essential for large consumer groups** (100+ consumers)

---

### Scenario 6: Graceful Shutdown (ConsumerDemoWithShutdown)

**Concept:** Properly shuts down consumer with offset commit and clean group departure.

**Step 1: Start consumer**
```bash
# In IntelliJ: Run ConsumerDemoWithShutdown.main()
# Or via Maven:
mvn exec:java -Dexec.mainClass="com.vbforge.consumer.ConsumerDemoWithShutdown"
```

**Expected output:**
```
🚀 Starting Kafka Consumer with Graceful Shutdown
✅ Subscribed to topic: demo_topic_example
💡 Press Ctrl+C to gracefully shut down
```

**Step 2: Send test messages**
```bash
# In another terminal
docker exec -it kafka-java-broker kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic demo_topic_example
```

Type a few messages and watch consumer receive them.

**Step 3: Graceful shutdown (Press Ctrl+C in consumer terminal)**

**Expected shutdown output:**
```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
⚠️ SHUTDOWN SIGNAL DETECTED (Ctrl+C)
   Calling consumer.wakeup() to interrupt poll()...
   Waiting for main thread to finish processing...
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
👋 WakeupException caught - initiating graceful shutdown
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🔒 Closing consumer...
   - Committing final offsets
   - Leaving consumer group
   - Closing network connections
✅ Consumer closed successfully
🏁 Consumer finished - graceful shutdown complete!
```

**Step 4: Verify offsets were committed**

Check consumer group:
```bash
docker exec -it kafka-java-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe \
  --group my-java-application
```

**Expected output shows CURRENT-OFFSET = LOG-END-OFFSET (no lag)**

**Step 5: Test multiple consumer instances with shutdown**

**Terminal 1:**
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.consumer.ConsumerDemoWithShutdown"
```

**Terminal 2:**
```bash
# Run second instance
```

**Observe partition assignment:**
- First consumer: gets [0, 1, 2] initially
- Second consumer joins: rebalancing, first consumer keeps [0, 1], second gets [2]

**Step 6: Shutdown second consumer (Ctrl+C in Terminal 2)**

**Observe in Terminal 1:**
```
⚠️ Rebalancing triggered (consumer left)
   Previously owned partitions: [0, 1]
   New assignment: [0, 1, 2]  ← Gained partition 2
   Consumer continues processing without interruption!
```

**Key observation:** The remaining consumer KEPT its partitions [0,1] and only GAINED partition 2. No full rebalance!


#### What Happens During Graceful Shutdown?

| Phase | Action | Why |
|-------|--------|-----|
| 1. Signal | Ctrl+C → shutdown hook triggered | Catches termination signal |
| 2. Interrupt | `consumer.wakeup()` called | Breaks out of poll() |
| 3. Exception | `WakeupException` thrown | Expected, not an error |
| 4. Cleanup | `consumer.close()` called | Commits offsets, leaves group |
| 5. Rebalance | Consumer group rebalances | Partitions reassigned |

**Without Graceful Shutdown (Forceful Kill):**
```
Process killed → No offset commit → Messages will be re-processed on restart
Consumer group → Stale member (waiting for timeout → slow rebalance)
```

**With Graceful Shutdown:**
```
Process ends → Offsets committed → No duplicate processing
Clean leave → Immediate rebalance → Fast partition reassignment
```

#### Testing Scenarios Summary

| Test | Command | Expected |
|------|---------|----------|
| Single consumer | Run ConsumerDemoWithShutdown | Processes all messages |
| Graceful shutdown | Press Ctrl+C | Shows shutdown sequence |
| Check offsets | Describe consumer group | All offsets committed |
| Two consumers | Run 2 instances | Partitions split |
| Shutdown one | Ctrl+C on one | Remaining gets all partitions |
| No duplicate processing | Restart consumer | Doesn't reprocess old messages |

---

## Quick Reference: All Test Commands

| Scenario | Command to Run |
|----------|----------------|
| Basic Producer | `ProducerDemo.main()` |
| Sync Producer | `ProducerDemoSync.main()` |
| Key-Based Producer | `ProducerDemoKeys.main()` |
| Producer with Callback | `ProducerDemoWithCallback.main()` |
| Sticky Partitioner | `ProducerDemoWithCallbackSwitchPartitions.main()` |
| Simple Consumer | `ConsumerDemo.main()` |
| Consumer with Shutdown | `ConsumerDemoWithShutdown.main()` |
| Cooperative Consumer | `ConsumerDemoCooperative.main()` |
| Console Consumer (Docker) | `docker exec -it kafka-java-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic demo_topic_example --from-beginning --property print.key=true --property print.partition=true` |
| Console Producer (Docker) | `docker exec -it kafka-java-broker kafka-console-producer --bootstrap-server localhost:9092 --topic demo_topic_example` |

---

## Partition Distribution Formula

```
partition = Math.abs(Utils.murmur2(key.getBytes())) % numPartitions

Example with 3 partitions:
- id_1: hash % 3 = 0 → Partition 0
- id_2: hash % 3 = 1 → Partition 1  
- id_3: hash % 3 = 2 → Partition 2
- id_4: hash % 3 = 0 → Partition 0 (same as id_1!)
- id_5: hash % 3 = 1 → Partition 1 (same as id_2!)
```

**Note:** Different keys can map to the same partition! That's why `id_1` and `id_4` both go to partition 0.

---