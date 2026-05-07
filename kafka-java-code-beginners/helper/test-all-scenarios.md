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
# In IntelliJ/Eclipse: Run ProducerDemo.main()
# Or via Maven:
mvn exec:java -Dexec.mainClass="com.vbforge.producer.ProducerDemo"
```

**Expected Result:** Consumer terminal shows `Hello World from Docker Kafka Producer!`

---

### Scenario 2: Key-Based Partitioning (ProducerDemoKeys)

**Concept:** Same key → Same partition every time. Kafka uses `partition = hash(key) % numPartitions`

**Step 1: Run ProducerDemoKeys**
```bash
# In IntelliJ: Run ProducerDemoKeys.main()
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