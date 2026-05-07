# Kafka Java Code Beginners

* > A comprehensive demonstration of Apache Kafka producers and consumers using Java, showcasing various patterns including simple messaging, callbacks, key-based partitioning, graceful shutdown, and cooperative rebalancing.
* > configured with Kafka in Docker (+ Conduktor UI)
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
com.vbforge
      └──producer
            ├── ProducerDemo.java                              # Basic producer
            ├── ProducerDemoSynch.java                         # Basic sync-producer (Synchronous send: wait for result)
            ├── ProducerDemoWithCallback.java                  # Producer with metadata logging
            ├── ProducerDemoWithCallbackSwitchPartitions.java  # Demonstrates sticky partitioning
            └── ProducerDemoKeys.java                          # Key-based message routing
      └──consumer
            ├── ConsumerDemo.java                              # Basic consumer
            ├── ConsumerDemoWithShutdown.java                  # Graceful shutdown handling
            └── ConsumerDemoCooperative.java                   # Cooperative rebalancing      

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

## Components Descriptions

### Producers

#### 1. ProducerDemo
* [run flow description](helper/run-producer-demo-producer-demo-sync.md)
* The simplest producer example that sends a single "hello world" message.

#### 2. ProducerDemoSync
* [run flow description](helper/run-producer-demo-producer-demo-sync.md)
* Synchronous producer that waits for send confirmation.

#### 3. ProducerDemoWithCallback
* [run flow description](helper/run-producer-demo-callback.md)
* Producer that logs metadata (topic, partition, offset) via callback.

#### 4. ProducerDemoWithCallbackSwitchPartitions
* Demonstrates sticky partitioning by sending messages in batches.

#### 5. ProducerDemoKeys
* [run flow description](helper/run-producer-demo-key.md)
* Demonstrates key-based message routing to partitions.

### Consumers

#### 1. ConsumerDemo
* [run flow description](helper/run-consumer-demo.md)
* Basic consumer that continuously polls for messages with graceful shutdown.

#### 2. ConsumerDemoWithShutdown
* Consumer with enhanced graceful shutdown and wakeup handling.

#### 3. ConsumerDemoCooperative
* Uses CooperativeStickyAssignor for incremental rebalancing.

---

## Key Concepts Demonstrated

1. **Sticky Partitioning**: Kafka batches messages to the same partition until the batch is full
2. **Key-Based Partitioning**: Messages with the same key always go to the same partition
3. **Consumer Groups**: Multiple consumers in a group share partition consumption
4. **Graceful Shutdown**: Proper cleanup prevents data loss and aids rebalancing
5. **Cooperative Rebalancing**: Minimizes disruption by only reassigning necessary partitions

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

| Scenario | How to Test |
|----------|-------------|
| Basic producer-consumer | Run `ProducerDemo` + console consumer |
| Callbacks & metadata | Run `ProducerDemoWithCallback` |
| Sticky partitioning | Run `ProducerDemoWithCallbackSwitchPartitions` |
| Key-based routing | Run `ProducerDemoKeys` + consumer with `print.key=true` |
| Simple consumer | Run `ConsumerDemo` |
| Graceful shutdown | Run `ConsumerDemoWithShutdown` + Ctrl+C |
| Cooperative rebalancing | Run 3 instances of `ConsumerDemoCooperative` |

**No more local Kafka installation needed!**

To remember: all Kafka CLI commands now need `docker exec -it kafka-java-broker` prefix.

---

## License

This project is part of a personal learning workshop.

---

## Author

**vbforge** — [GitHub](https://github.com/vbforge/workshop-kafka-apps)

---


