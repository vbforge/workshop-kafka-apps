## Test All Scenarios

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
