## We are working here with ConsumerDemo

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

### 4. Create the topic (if not exists)
```bash
docker exec -it kafka-java-broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic demo_topic_example \
  --partitions 3 \
  --replication-factor 1
```

### 5. Run your Java Consumer

####  In IntelliJ:
1. Open `ConsumerDemo.java`
2. Click the green arrow next to `main()` method
3. Select "Run ConsumerDemo.main()"

### 6. In another terminal, send messages to see consumption

#### Option A: Using Java Producer
Run `ProducerDemo.main()` or `ProducerDemoSync.main()` in your IDE

#### Option B: Using console producer (Docker)
```bash
docker exec -it kafka-java-broker kafka-console-producer --bootstrap-server localhost:9092 --topic demo_topic_example
```
Then type messages and press Enter:
```
> Hello from consumer test!
> Another message
> Ctrl+C to stop
```

#### Option C: Using CLI producer with keys (easy to send messages by separating with `:` for keys and values)
```bash
docker exec -it kafka-java-broker kafka-console-producer --bootstrap-server localhost:9092 --topic demo_topic_example --property parse.key=true --property key.separator=:
```
Then type:
```
key1:message with key1
key2:message with key2
```

### 7. Observe consumer output

Expected output when messages arrive:
```
🚀 Starting Kafka Consumer (Docker version)
✅ Subscribed to topic: demo_topic_example
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📨 Received message:
   Key:       null
   Value:     Hello from consumer test!
   Topic:     demo_topic_example
   Partition: 2
   Offset:    42
   Timestamp: 1733456789123
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
```

### 8. Graceful shutdown

Press `Ctrl+C` in the consumer terminal:
```
⚠️ Shutdown signal received. Stopping consumer...
👋 Consumer wakeup called - shutting down gracefully
🔒 Closing consumer...
✅ Consumer closed successfully
🏁 Consumer finished
```

---

## Verification Checklist

| Test | Command | Expected Result |
|------|---------|-----------------|
| Kafka running | `docker ps` | Container `kafka-java-broker` is up |
| Topic exists | `docker exec -it kafka-java-broker kafka-topics --bootstrap-server localhost:9092 --list` | `demo_topic_example` in list |
| Consumer subscribed | Consumer logs | "Subscribed to topic" message |
| Message received | Consumer logs | Shows message with partition, offset |
| Consumer group active | `docker exec -it kafka-java-broker kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group my-java-application` | Shows consumer and offset |

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `Connection refused: localhost:9092` | Kafka not started → `docker-compose up -d` |
| Consumer doesn't receive messages | 1. Verify topic exists<br>2. Send new messages (consumer reads from current offset)<br>3. Check `auto.offset.reset: earliest` |
| `No current assignment for partition` | Consumer needs to complete rebalancing (takes 1-2 seconds) |
| Consumer shows no logs | Change log level to INFO in logback.xml or add `log.info()` statements |
| Multiple consumers in same group | Each gets different partitions (rebalancing happens automatically) |

---

## Testing Multiple Consumers in Same Group

To test consumer group rebalancing:

* > easy to do: just copy ConsumerDemo class and rename it to ConsumerDemo2
* > then run both ConsumerDemo and ConsumerDemo2
* > or do by different terminals:

**Terminal 1:**
```
# Run first consumer instance
  mvn exec:java -Dexec.mainClass="com.vbforge.consumer.ConsumerDemo"
```

**Terminal 2:**
```
# Run second consumer instance (same group ID)
  mvn exec:java -Dexec.mainClass="com.vbforge.consumer.ConsumerDemo"
```

**Expected behavior:**
- First consumer gets some partitions (e.g., 0, 1)
- Second consumer gets remaining partitions (e.g., 2)
- Messages are distributed across both consumers

---

## Key Configuration Explained

| Property | Value | Meaning |
|----------|-------|---------|
| `bootstrap.servers` | `localhost:9092` | Connect to Docker Kafka |
| `group.id` | `my-java-application` | Consumer group identifier |
| `auto.offset.reset` | `earliest` | Start from beginning if no offset |
| `enable.auto.commit` | `true` | Auto-commit offsets periodically |
| `auto.commit.interval.ms` | `1000` | Commit every 1 second |
| `key.deserializer` | `StringDeserializer` | Convert bytes to String for keys |
| `value.deserializer` | `StringDeserializer` | Convert bytes to String for values |

---