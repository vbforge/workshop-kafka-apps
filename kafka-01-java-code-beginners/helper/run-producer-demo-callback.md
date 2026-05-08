## We are working here with ProducerDemoWithCallback

### Concept: Asynchronous Callbacks with Metadata

**What are callbacks?** 
A callback is a function that executes when Kafka acknowledges your message (or when an error occurs). It runs **asynchronously** in a separate thread, so it doesn't block your main application.

**Why use callbacks?**
- ✅ Know exactly where your message was stored (partition + offset)
- ✅ Detect failures without blocking the producer
- ✅ Implement per-message retry logic
- ✅ Audit trail for compliance
- ✅ Real-time metrics (success/failure rates)

**Callback execution flow:**
```
Main Thread                    Callback Thread
│                               │
├─ send(message) ───────────────►
│                               │
├─ continue work (non-blocking) │
│                               │
│                          Kafka acknowledges
│                               │
│           ◄────── callback runs
│                               │
└─ callback logs metadata       └─ execution complete
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

### 4. Create the topic (optional - auto-creation is enabled)

```bash
docker exec -it kafka-java-broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic demo_topic_example \
  --partitions 3 \
  --replication-factor 1
```

### 5. Run ProducerDemoWithCallback

#### Option A: In IntelliJ
1. Open `ProducerDemoWithCallback.java`
2. Click the green arrow next to `main()` method
3. Select "Run ProducerDemoWithCallback.main()"

#### Option B: Via command line (Maven)
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.producer.ProducerDemoWithCallback"
```

### 6. Expected Output

```
🚀 Starting Kafka Producer with Callback (Docker version)
📊 Demonstrating asynchronous metadata logging
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
✅ Message #3 sent successfully!
   Topic:     demo_topic_example
   Partition: 2
   Offset:    2
   Timestamp: 1733456791123
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
...
🔄 Flushing producer...
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
🏁 Producer finished
📊 Summary: 10 successful, 0 failed
🎯 Key insight: Callbacks are ASYNCHRONOUS - they run after Kafka acknowledges!
```

### 7. Verify Messages with Console Consumer

```bash
docker exec -it kafka-java-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic demo_topic_example \
  --from-beginning
```

**Expected output:**
```
Message #1 sent at 1733456789123
Message #2 sent at 1733456790123
Message #3 sent at 1733456791123
...
```

### 8. Understanding the Output

| Field | Description | Why It Matters |
|-------|-------------|----------------|
| **Topic** | Which topic the message was written to | Confirms correct routing |
| **Partition** | Which partition (0, 1, or 2) | Know where your data lives |
| **Offset** | Position within the partition | Used for seeking, debugging |
| **Timestamp** | When Kafka received the message | Time-based processing |
| **Serialized size** | Size in bytes | Monitor message size |

### 9. Key Observations

#### Observation 1: Callbacks Are Asynchronous
Notice that "Message sent asynchronously" logs appear BEFORE the callback logs. The main thread continues while Kafka processes messages in the background.

#### Observation 2: All Messages May Go to Same Partition
With default settings, Kafka uses **sticky partitioning** - it batches messages to the same partition for efficiency.

#### Observation 3: Offsets Are Sequential Per Partition
Even across different send calls, offsets increase sequentially within each partition.

---

## Verification Checklist

| Test | Command | Expected Result |
|------|---------|-----------------|
| Kafka running | `docker ps` | Container `kafka-java-broker` is up |
| Producer runs | Producer logs | 10 messages sent |
| Callbacks execute | Producer logs | 10 "✅ Message sent successfully!" entries |
| Metadata logged | Callback output | Shows topic, partition, offset, timestamp |
| Messages in Kafka | Console consumer | Shows all 10 messages |

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| No callback logs appear | Callbacks run asynchronously - wait a few seconds after flush() |
| Error callbacks appear | Check Kafka connection, topic existence, network |
| Offsets not sequential | Normal if multiple producers or previous runs - delete topic and recreate |
| Messages go to different partitions | Sticky partitioner may switch after batch is full |
| Callback order mismatched | Callbacks execute in order Kafka acknowledges, not send order |

---

## Advanced: Simulating a Failure

To see error callbacks in action:

**Option 1: Stop Kafka mid-send**
```bash
# While producer is running, in another terminal:
docker-compose stop kafka
# Observe error callbacks
# Restart: docker-compose start kafka
```

**Option 2: Use invalid topic name**
```java
// Temporarily change topic to one that doesn't exist
ProducerRecord<String, String> producerRecord = 
    new ProducerRecord<>("non_existent_topic", message);
```

**Expected error output:**
```
❌ Message #5 failed to send!
org.apache.kafka.common.errors.TimeoutException: Topic non_existent_topic not present in metadata after 60000 ms
```

---

## Why This Matters for Production

| Production Use Case | How Callbacks Help |
|---------------------|---------------------|
| **Audit Logging** | Record exactly when/where each message was stored |
| **Dead Letter Queue** | Send failed messages to DLQ for later processing |
| **Metrics Collection** | Track success/failure rates per partition |
| **Exactly-Once Semantics** | Store (partition, offset) as unique identifier |
| **Performance Monitoring** | Measure time between send and acknowledgment |

---

## Comparison: With vs Without Callbacks

| Aspect | Without Callback | With Callback |
|--------|------------------|---------------|
| Know if message succeeded? | ❌ No | ✅ Yes |
| Know partition/offset? | ❌ No | ✅ Yes |
| Detect specific failures? | ❌ No | ✅ Yes |
| Performance impact | Lower | Minimal (async) |
| Code complexity | Simple | Slightly more |

---

## Experiment: Remove flush() and see what happens

```java
// Comment out these lines:
// producer.flush();
// Thread.sleep(500);
```

**Observation:** The program may exit before callbacks execute!
**Why?** Callbacks run in background threads. If main thread exits, JVM shuts down and callbacks never run.

**Lesson:** Always call `flush()` and wait briefly before closing!

---

