# Scenario 01 — Simple Producer & Consumer

**What this scenario demonstrates:**
- Basic message production and consumption
- Async send with callbacks (success path + failure path)
- Graceful consumer shutdown via ShutdownHook + `consumer.wakeup()`
- Metrics: throughput, timing, success/failure counts

---

## Prerequisites

- Docker running
- Kafka started from project root: `docker-compose up -d`
- Verify it's up: `docker-compose ps`

---

## How to Run

Open **two separate IntelliJ embedded terminal tabs** (not Run Configurations — see note below).

### Terminal 1 — Start Consumer first

```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_01_simple.SimpleConsumer"
```

Expected output:
```
Starting SimpleConsumer
Bootstrap Servers: localhost:9092
Subscribed to topic: simple-topic
Waiting for messages... (Press Ctrl+C to stop)
```

Consumer runs indefinitely, polling for messages.

### Terminal 2 — Run Producer

```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_01_simple.SimpleProducer"
```

Expected output (per message):
```
Message #1 sent successfully (375ms)
   Value: Message #1 from Docker Kafka - Timestamp: 1778506363202
   Topic: simple-topic
   Partition: 0
   Offset: 0
   Timestamp: 1778506363518
...
FINAL STATISTICS:
   Expected messages: 10
   Successfully sent: 10
   Failed: 0
   Total time: 5106 ms
   Throughput: 1.96 msgs/sec
ALL MESSAGES SENT SUCCESSFULLY!
```

Producer exits automatically after sending all messages.

### Terminal 1 — Stop Consumer

Press `Ctrl+C` in the consumer terminal.

Expected shutdown sequence:
```
Shutdown signal received — calling consumer.wakeup()
WakeupException received — consumer is shutting down
Consumer closed
═══════════════════════════════════════════
FINAL STATISTICS:
   Messages processed: 10
   Total runtime: 18432 ms
   Avg throughput: 0.54 msgs/sec
═══════════════════════════════════════════
SimpleConsumer finished!
```

---

## Why terminal and not the IntelliJ Run button?

| Stop method | Signal sent | Shutdown hook | Stats printed |
|---|---|---|---|
| `Ctrl+C` in terminal | SIGINT | ✅ fires | ✅ yes |
| IntelliJ Stop button | SIGKILL | ❌ skipped | ❌ no |

The Stop button kills the JVM instantly — shutdown hooks never execute.
Always use `Ctrl+C` to stop the consumer so the hook demonstrates correctly.

---

## Overriding Kafka address (optional)

By default connects to `localhost:9092`. Override via environment variable:

```bash
KAFKA_BOOTSTRAP_SERVERS=myhost:9092 mvn exec:java -Dexec.mainClass="com.vbforge.scenario_01_simple.SimpleConsumer"
```

---

## Useful Kafka CLI commands (inside Docker) for my OS-system

```bash
# each call need to start with 'winpty' prefix for execution inside Docker

# List topics
winpty docker exec -it kafka-learning-broker kafka-topics --list --bootstrap-server localhost:19092

# Describe topic
winpty docker exec -it kafka-learning-broker kafka-topics --describe --topic simple-topic --bootstrap-server localhost:19092

# Read all messages from beginning
winpty docker exec -it kafka-learning-broker kafka-console-consumer --topic simple-topic --bootstrap-server localhost:19092 --from-beginning

# Check consumer group status
winpty docker exec -it kafka-learning-broker kafka-consumer-groups --bootstrap-server localhost:19092 --group consumer-group-simple --describe
```

---

## Cleanup

```bash
# Stop Kafka, keep data
docker-compose down

# Stop Kafka, wipe all data (clean slate)
docker-compose down -v
```

---

## Observations to make

- Messages arrive in order (offset 0, 1, 2...) — single partition guarantees this
- Callback fires **after broker acknowledgement**, not after `producer.send()` returns
- `wakeup()` interrupts `poll()` mid-wait — consumer doesn't wait out the full 1000ms timeout
- `consumer.close()` in `finally` commits pending offsets before exit

---
