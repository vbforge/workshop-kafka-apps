# Kafka Learning Scenarios

* > This is a comprehensive, hands-on Java project designed to teach Apache Kafka through practical, real-world scenarios.
* > 5 progressive scenarios across app provided (using pure Java Kafka client): basic messaging → load balancing → keyed messages → manual offsets → complete e-commerce pipeline

---

## Required Software for this project
- **Java 17+**
- **Maven 3.6+**
- **Docker**
- **IDE**

---

## Key Concepts Covered

- 5+ complete scenarios covering beginner to advanced topics
- Self-contained examples with detailed comments
- Producer acks, idempotence, batching
- Consumer groups, partition assignment
- Offset management (auto/manual/batch/per-partition)
- At-least-once vs at-most-once delivery
- Shutdown hooks and graceful termination
- Real-world e-commerce pipeline design
- Production-ready code patterns
- Comprehensive documentation

---

## Setup Instructions

1) Clone or Create Project
2) Build the Project: `mvn clean install`
3) Verify Build: `mvn compile`
4) Test Kafka SetUp & Connection:
   - start docker;
   - run docker-compose from the root of the project: **docker-compose up -d**
   - run **DockerConnectivityTest**
   - expected console output:
     ```
     Successfully connected to Docker Kafka!
      ...
     Test Kafka Configuration Connectivity Completed!
     ```

   - stop and remove container: `docker-compose down -v`

---

## Project Structure

```
kafka-02-learning-scenarios/
├── docker-compose.yml                                         # Kafka broker + dependencies
├── pom.xml                                                    # Maven dependencies
├── README.md                                                  # This file
│
└── src/main/java/com/vbforge/
   │
   ├── config/                                                 # Shared configuration
   │   ├── Constants.java                                      # Topics, groups, timing constants
   │   ├── KafkaConfig.java                                    # Producer/consumer factory
   │   ├── Utility.java                                        # ObjectMapper, config validation
   │   └── DockerConnectivityTest.java                         # Connection sanity check
   │
   ├── scenario_01_simple/                                     # Basic Producer/Consumer
   │   ├── SimpleProducer.java                                 # Async producer with callbacks
   │   ├── SimpleConsumer.java                                 # Basic consumer with shutdown hook
   │   └── how_to_run_scenario_01_simple.md
   │
   ├── scenario_02_demo_app/                                   # Synchronous Sends
   │   ├── MyProducer.java                                     # Simple sync producer
   │   ├── MyConsumer.java                                     # Single consumer
   │   ├── MyConsumer2.java                                    # Second consumer (different group)
   │   ├── KeyedMessageProducer.java                           # Partition-by-key example
   │   ├── MultiMessageProducer.java                           # Batch sending
   │   └── how_to_run_scenario_02_demo_app.md
   │
   ├── scenario_03_load_balancing/                             # Consumer Groups
   │   ├── LoadBalancingProducer.java                          # Messages across partitions
   │   ├── LoadBalancingConsumer.java                          # Consumer that joins a group
   │   ├── LoadBalancingGroupMonitor.java                      # Describe group state
   │   └── how_to_run_scenario_03_load_balancing.md
   │
   ├── scenario_04_topic_keyed/                                # Message Ordering
   │   ├── KeyedProducer.java                                  # Same key → same partition
   │   ├── KeyedConsumer.java                                  # Preserves per-key ordering
   │   ├── OrderProofProducer.java                             # Demo: order events per orderId
   │   └── how_to_run_scenario_04_topic_keyed.md
   │
   ├── scenario_05_manual_offset_management/                   # Offset Control
   │   ├── OrderProducer.java                                  # 20 test orders
   │   ├── AutoCommitConsumer.java                             # DANGER: At-most-once delivery
   │   ├── ManualCommitConsumer.java                           # SAFE: At-least-once delivery
   │   ├── BatchCommitConsumer.java                            # Commit after N records
   │   ├── PerPartitionCommitConsumer.java                     # Track per-partition offsets
   │   ├── OrderProcessingException.java                       # Custom exception for demo
   │   └── how_to_run_scenario_05_manual_offset_management.md
   │
   └── scenario_06_e_commerce_orders_app/                      # Real-World Pipeline
       ├── Order.java                                          # Order domain object
       ├── OrderService.java                                   # Order producer with validation
       ├── PaymentService.java                                 # Processes payments
       ├── InventoryService.java                               # Updates inventory
       ├── NotificationService.java                            # Sends notifications
       ├── AnalyticsService.java                               # Real-time analytics
       └── how_to_run_ecommerce.md
```

---

## Configuration & Documentation

All Kafka configurations are centralized:
- `Constants.java` - Topics, groups, timing
- `KafkaConfig.java` - Producer/consumer factories
- `Utility.java` - startup sanity check, logs active Kafka settings, Singleton ObjectMapper
- Environment variable: `KAFKA_BOOTSTRAP_SERVERS` (default: localhost:9092)
- I use Kafka (KRaft Mode) only image, but might be useful full docker image with Kafka UI, DB [docker_compose_with_Conductor_UI](docker-compose_with_Conductor_UI.yml)

Each scenario includes a dedicated `how_to_run_*.md` file with:
- Step-by-step instructions
- Expected output
- Common pitfalls
- Kafka CLI verification commands

---

## Learning Path

| Scenario | Concept | Key Takeaway |
|----------|---------|---------------|
| 01 | Basic Producer/Consumer | Async callbacks, shutdown hooks, metrics |
| 02 | Synchronous Sends | Blocking sends, different consumer groups |
| 03 | Load Balancing | Partitions + consumer groups = horizontal scaling |
| 04 | Keyed Messages | Same key → same partition (ordering guarantee) |
| 05 | **Manual Offset** | **Auto-commit = data loss on crash** |
| 06 | E-Commerce Pipeline | Real-world: Orders → Payment → Inventory → Notifications |

---

## What we explore (6 scenarios):

1) **scenario_01_simple:**
    - Async `Producer class` with callbacks — success path, failure path, send metrics;
    - `Consumer class` runs infinitely, graceful shutdown via `Ctrl+C` (ShutdownHook + `consumer.wakeup()` pattern);
    - `WakeupException` as the correct interrupt mechanism for `poll()`;
    - [how to run](src/main/java/com/vbforge/scenario_01_simple/how_to_run_scenario_01_simple.md)


2) **scenario_02_demo_app:**
    - Three producer variants: single message (`MyProducer class`), multiple with `null` key — round-robin (`MultiMessageProducer class`), messages with explicit keys (`KeyedMessageProducer`);
    - Broadcast behaviour — `MyConsumer class` and `MyConsumer2 class` use different group IDs → both receive all messages;
    - Load-balance behaviour — switching both consumers to the same group ID → each message delivered to one consumer only;
    - Kafka persistence — messages survive consumer downtime, replayed on restart;
    - [how to run](src/main/java/com/vbforge/scenario_02_demo_app/how_to_run_scenario_02_demo_app.md)


3) **scenario_03_load_balancing:**
    - Single `LoadBalancingConsumer class` class run N times — no copy-paste consumers;
    - Consumer group: Kafka auto-assigns partitions across all running instances;
    - Rebalancing observed live: stop a consumer → others absorb its partitions;
    - Ceiling rule: consumers > partitions → excess consumers sit idle until a slot opens;
    - [how to run](src/main/java/com/vbforge/scenario_03_load_balancing/how_to_run_scenario_03_load_balancing.md)


4) **scenario_04_topic_keyed:**
    - `KeyedProducer class` — user ID as message key; same key always routes to the same partition via `hash(key) % numPartitions`;
    - `KeyedConsumer class` tracks per-key event counts — each consumer instance owns a fixed subset of keys, never overlap;
    - `OrderProofProducer class` — sends a fixed step-by-step workflow per user (Login→Browse→Checkout→Logout) with synchronous send to prove steps always arrive in order;
    - Per-key ordering guarantee explained: ordering is preserved within a partition, not across the whole topic;
    - [how to run](src/main/java/com/vbforge/scenario_04_topic_keyed/how_to_run_scenario_04_topic_keyed.md)


5) **scenario_05_manual_offset_management:**
    - Four consumer strategies compared side-by-side against the same `OrderProducer class`;
    - `AutoCommitConsumer class` — default Kafka behavior, at-most-once delivery; crash experiment demonstrates how committed offsets cause messages to be silently skipped on restart;
    - `ManualCommitConsumer class` — `commitSync()` called only after full batch succeeds, at-least-once guarantee; `OrderProcessingException` separates business failures (don't commit, redeliver) from unexpected runtime errors (stop consumer);
    - `BatchCommitConsumer class` — commits every N records for balanced throughput; final `commitSync()` in `finally` block flushes remaining records on graceful shutdown;
    - `PerPartitionCommitConsumer class` — commits each partition's offset independently after processing; isolates failures so one partition's problem doesn't block another's progress; `offset + 1` rule explained;
    - [how to run](src/main/java/com/vbforge/scenario_05_manual_offset_management/how_to_run_scenario_05_manual_offset_management.md)


6) **scenario_06_e_commerce_orders_app:**
    - descriptions not provided yet...

---