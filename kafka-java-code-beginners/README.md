# Kafka Java Code Beginners

* > A comprehensive demonstration of Apache Kafka producers and consumers using Java, showcasing various patterns including simple messaging, callbacks, key-based partitioning, graceful shutdown, and cooperative rebalancing.
* > configured with Kafka in Docker (+ Conduktor UI)
---

## Table of contents

  * [Project Overview](#project-overview)
  * [Prerequisites](#prerequisites)
  * [Project Structure](#project-structure)
  * [Start Kafka from Docker](#start-kafka-from-docker)
  * [Project Components Descriptions (and flows for testing)](#project-components-descriptions-and-flows-for-testing)
    * [`Producers`](#producers)
      * [1. **ProducerDemo**](#1-producerdemo)
      * [2. **ProducerDemoSync**](#2-producerdemosync)
      * [3. **ProducerDemoWithCallback**](#3-producerdemowithcallback)
      * [4. **ProducerDemoWithCallbackSwitchPartitions**](#4-producerdemowithcallbackswitchpartitions)
      * [5. **ProducerDemoKeys**](#5-producerdemokeys)
    * [`Consumers`](#consumers)
      * [1. **ConsumerDemo**](#1-consumerdemo)
      * [2. **ConsumerDemoWithShutdown**](#2-consumerdemowithshutdown)
      * [3. **ConsumerDemoCooperative**](#3-consumerdemocooperative)
  * [Key Concepts Demonstrated](#key-concepts-demonstrated)
  * [Check Tests All scenarios:](#check-tests-all-scenarios)
  * [Complete Testing Script (One-liners) for Windows (`kafka-docker-test.bat`):](#complete-testing-script-one-liners-for-windows-kafka-docker-testbat)
  * [Use Conduktor UI for Visual Testing](#use-conduktor-ui-for-visual-testing)
  * [Summary:](#summary)
  * [License](#license)
  * [Author](#author)

---

## Project Overview

This project contains examples of:
- **Producers**: Simple producer, producer with callbacks, key-based producers, sticky partitioning
- **Consumers**: Simple consumer, consumer with graceful shutdown, consumer with cooperative sticky assignor

---

## Prerequisites

- Java 8 or higher
- docker-compose.yml (for run Apache Kafka by Docker)
- Maven or Gradle (for dependency management)
- SLF4J logging framework

---

## Project Structure

```
kafka-java-code-beginners/
├── src/main/java/com/vbforge/
│   ├── producer/
│   │   ├── ProducerDemo.java
│   │   ├── ProducerDemoSync.java
│   │   ├── ProducerDemoWithCallback.java
│   │   ├── ProducerDemoWithCallbackSwitchPartitions.java
│   │   └── ProducerDemoKeys.java
│   └── consumer/
│       ├── ConsumerDemo.java
│       ├── ConsumerDemoWithShutdown.java
│       └── ConsumerDemoCooperative.java
├── helper/
│   ├── image-1-comparison-eager-vs-cooperative
│   ├── image-2-graceful-shutdown-flow-diagram
│   ├── run-producer-demo-producer-demo-sync.md
│   ├── run-consumer-demo.md
│   ├── run-producer-demo-key.md
│   ├── run-producer-demo-callback.md
│   ├── run-producer-demo-sticky-partitions.md
│   ├── run-consumer-demo-cooperative.md
│   ├── run-consumer-demo-shutdown.md
│   ├── kafka-docker-test.bat
│   └── test-all-scenarios.md
├── docker-compose.yml
├── pom.xml
└── README.md
```

---

## Start Kafka from Docker

```bash
# Navigate to your project folder
cd kafka-java-code-beginners

# Start Kafka and Conduktor
docker-compose up -d

# Wait for Kafka to be ready (watch logs)
docker-compose logs -f kafka
# Press Ctrl+C when you see "started" or wait 30 seconds

# Verify Kafka is running
docker ps
```

---

## Project Components Descriptions (and flows for testing)

### `Producers`

#### 1. **ProducerDemo**
* [run flow description](helper/run-producer-demo-producer-demo-sync.md)
* The simplest producer example that sends a single "hello world" message.

**Key Features:**
- Basic producer configuration (bootstrap.servers, serializers)
- Asynchronous send with flush() and close()
- Demonstrates minimal producer setup
- Great starting point for understanding Kafka producer basics

**Configuration:**
- Bootstrap server: `localhost:9092` (Docker Kafka)
- Topic: `demo_topic_example`
- Key serializer: StringSerializer
- Value serializer: StringSerializer

**What you'll learn:**
- How to create a Kafka producer
- How to send messages to a topic
- Why flush() and close() are important

---

#### 2. **ProducerDemoSync**
* [run flow description](helper/run-producer-demo-producer-demo-sync.md)
* Synchronous producer that waits for send confirmation.

**Key Features:**
- Uses `producer.send(record).get()` for synchronous delivery
- Blocks until Kafka acknowledges the message
- Returns RecordMetadata with partition, offset, timestamp
- Throws exceptions on failure (no silent failures)

**Compared to Async:**

| Aspect | Async (default) | Sync (this demo) |
|--------|-----------------|------------------|
| Performance | Higher (non-blocking) | Lower (blocking) |
| Error handling | Callback-based | Try-catch |
| Use case | High throughput | Critical messages |
| Simplicity | More complex | Simpler to understand |

**What you'll learn:**
- The difference between sync and async sends
- How to get metadata synchronously
- Exception handling with Kafka producer

---

#### 3. **ProducerDemoWithCallback**
* [run flow description](helper/run-producer-demo-callback.md)
* Producer that logs metadata (topic, partition, offset) via callback.

**Key Features:**
- Metadata logging on success
- Error handling via callback
- 1-second delay between messages
- Messages go to the same partition (default sticky behavior)
- Demonstrates asynchronous callback execution

**Callback Benefits:**
- ✅ Know exactly where your message was stored (partition + offset)
- ✅ Detect failures without blocking the producer
- ✅ Implement per-message retry logic
- ✅ Audit trail for compliance

**What you'll learn:**
- How callbacks work asynchronously
- What metadata Kafka provides
- How to handle success and failure cases

---

#### 4. **ProducerDemoWithCallbackSwitchPartitions**
* [run flow description](helper/run-producer-demo-sticky-partitions.md)
* Demonstrates sticky partitioning by sending messages in batches.

**Key Features:**
- Sends 300 messages total (10 batches × 30 messages)
- Custom batch size: 400 bytes (smaller than default 16KB)
- Shows how Kafka's sticky partitioner switches partitions
- 500ms delay between batches for visibility

**Sticky Partitioning Explained:**

| Traditional Round-Robin | Sticky Partitioning |
|------------------------|---------------------|
| Each message → different partition | Batch of messages → same partition |
| Many metadata requests | Fewer metadata requests |
| Lower throughput | 2-3x higher throughput |
| Pre-Kafka 2.4 default | Kafka 3.0+ default |

**What you'll learn:**
- How Kafka batches messages for efficiency
- When and why partition switches occur
- How batch.size affects partitioning behavior
- The performance benefits of sticky partitioning

---

#### 5. **ProducerDemoKeys**
* [run flow description](helper/run-producer-demo-key.md)
* Demonstrates key-based message routing to partitions.

**Key Features:**
- Sends 20 messages (2 rounds × 10 messages)
- Uses keys: "id_1" through "id_10"
- **Same key always goes to the same partition**
- Logs which partition each key maps to
- Shows consistency across multiple batches

**The Key Formula:**
> partition = hash(key) % numPartitions

**Key Benefits:**
- Guarantees order for messages with the same key
- Enables stateful processing per entity (user, order, session)
- Allows efficient caching and aggregation
- Deterministic routing for predictable behavior

**What you'll learn:**
- How Kafka uses keys to determine partitions
- Why same key = same partition (always!)
- How to use keys for ordered message processing
- Partition distribution with different keys

---

### `Consumers`

#### 1. **ConsumerDemo**
* [run flow description](helper/run-consumer-demo.md)
* Basic consumer that continuously polls for messages with graceful shutdown.

**Key Features:**
- Continuous polling loop with Duration.ofMillis(1000)
- Subscribes to topic with consumer group
- Auto-commit offsets enabled
- Graceful shutdown with shutdown hook
- Logs message details (key, value, partition, offset)

**Configuration:**
- Group ID: `my-java-application`
- Auto offset reset: `earliest` (start from beginning)
- Auto commit: `true` with 1000ms interval
- Poll duration: 1000ms

**Partition Assignment:**
- 1 consumer: Gets all 3 partitions
- 2 consumers: Partitions split (2+1)
- 3 consumers: Each gets 1 partition

**What you'll learn:**
- How to create a Kafka consumer
- How consumer groups work
- How to poll for messages continuously
- Basic message processing patterns

---

#### 2. **ConsumerDemoWithShutdown**
* [run flow description](helper/run-consumer-demo-shutdown.md)
* Consumer with enhanced graceful shutdown and wakeup handling.

**Key Features:**
- Shutdown hook to catch termination signals (Ctrl+C)
- Uses `consumer.wakeup()` for clean shutdown
- Proper resource cleanup with `consumer.close()`
- Handles `WakeupException` gracefully
- Commits final offsets before exit

**Graceful Shutdown Flow:**
```
1. Ctrl+C pressed → Shutdown hook triggered
2. consumer.wakeup() called → Interrupts poll()
3. WakeupException caught → Exit poll loop
4. finally block → consumer.close()
5. Offsets committed → Consumer leaves group
```

**Why Graceful Shutdown Matters:**

| Without Graceful Shutdown | With Graceful Shutdown |
|---------------------------|------------------------|
| Messages may be lost | ✅ All messages processed |
| Duplicate processing on restart | ✅ Offsets committed |
| Slow rebalancing (timeout) | ✅ Clean group departure |
| Resource leaks | ✅ Proper cleanup |

**What you'll learn:**
- How to handle shutdown signals properly
- Why consumer.wakeup() is the correct approach
- How to prevent data loss during shutdown
- Clean resource management patterns

---

#### 3. **ConsumerDemoCooperative**
* [run flow description](helper/run-consumer-demo-cooperative.md)
* Uses CooperativeStickyAssignor for incremental rebalancing.

**Key Features:**
- Strategy: `CooperativeStickyAssignor`
- **Incremental rebalancing**: Only affected partitions are reassigned
- Consumers keep their existing partitions during rebalancing
- More efficient than eager rebalancing (RangeAssignor/RoundRobinAssignor)
- Default strategy in modern Kafka clients (2.4+)

**Partition Assignment Examples:**

| Consumers | Eager Rebalancing | Cooperative Sticky |
|-----------|-------------------|---------------------|
| 1 → 2 | ALL consumers STOP, ALL partitions reassigned | Only partition 2 moves |
| 2 → 3 | ALL consumers STOP again | Only partitions 1 and 2 move |
| Consumer leaves | ALL consumers STOP | Only that consumer's partitions move |
| Downtime | Several seconds | Milliseconds |


**When to Use:**
- ✅ Large consumer groups (100+ consumers)
- ✅ Stateful processing (Kafka Streams)
- ✅ Rolling deployments
- ✅ Auto-scaling environments
- ✅ Any production application (recommended default)

**What you'll learn:**
- The difference between eager and incremental rebalancing
- How CooperativeStickyAssignor preserves assignments
- Why rebalancing is faster with cooperative strategy
- How to monitor rebalancing behavior

---

## Key Concepts Demonstrated

1. **Sticky Partitioning**: Kafka batches messages to the same partition until the batch is full
2. **Key-Based Partitioning**: Messages with the same key always go to the same partition
3. **Consumer Groups**: Multiple consumers in a group share partition consumption
4. **Graceful Shutdown**: Proper cleanup prevents data loss and aids rebalancing
5. **Cooperative Rebalancing**: Minimizes disruption by only reassigning necessary partitions
6. **Async Callbacks**: Non-blocking metadata logging and error handling
7. **Idempotent Producer**: Prevents duplicate messages (enable.idempotence=true)

---


## Check Tests All scenarios:
[Test all scenarios](helper/test-all-scenarios.md)

---

## Complete Testing Script (One-liners) for Windows (`kafka-docker-test.bat`):

[Testing Script](helper/kafka-docker-test.bat)

---

## Use Conduktor UI for Visual Testing

>Open browser: http://localhost:8085

**What you can see:**
- All topics and partitions
- Consumer group offsets and lag
- Browse messages (like console consumer but with GUI)
- Create/delete topics

>Easier than command line for testing!


### Stop Everything

```bash
# Stop Kafka and Conduktor
docker-compose down

# Remove all data (reset everything)
docker-compose down -v
```

---

## Summary:

| Scenario | How to Test                                               |
|----------|-----------------------------------------------------------|
| Basic producer-consumer | Run **ProducerDemo** + console consumer                   |
| Callbacks & metadata | Run **ProducerDemoWithCallback**                          |
| Sticky partitioning | Run **ProducerDemoWithCallbackSwitchPartitions**          |
| Key-based routing | Run **ProducerDemoKeys** + consumer with `print.key=true` |
| Simple consumer | Run **ConsumerDemo**                                      |
| Graceful shutdown | Run **ConsumerDemoWithShutdown** + Ctrl+C                 |
| Cooperative rebalancing | Run 3 instances of **ConsumerDemoCooperative**            |

**No more local Kafka installation needed!**

To remember: all Kafka CLI commands now need `docker exec -it kafka-java-broker` prefix.

---

## License

This project is part of a personal learning workshop.

---

## Author

**vbforge** — [GitHub](https://github.com/vbforge/workshop-kafka-apps)

---


