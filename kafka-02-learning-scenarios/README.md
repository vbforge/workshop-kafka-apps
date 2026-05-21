# Kafka Learning Scenarios

* > This is a comprehensive, hands-on Java project designed to teach Apache Kafka through practical, real-world scenarios.
* > 5 progressive scenarios across app provided (using pure Java Kafka client): basic messaging → load balancing → keyed messages → manual offsets → complete e-commerce pipeline

---

## Key Features
- 5+ complete scenarios covering beginner to advanced topics
- Self-contained examples with detailed comments
- Real-world e-commerce use case
- Production-ready code patterns
- Comprehensive documentation


---

## Test Kafka Connection
- start docker;
- run docker-compose from the root of the project: **docker-compose up -d**
- run **DockerConnectivityTest**
- expected console output:
```
[main] INFO com.vbforge.config.DockerConnectivityTest - Successfully connected to Docker Kafka!

...

[main] INFO com.vbforge.config.DockerConnectivityTest - Test Kafka Configuration Connectivity Completed!
```

- stop and remove container: `docker-compose down -v`

NOTES: 
1) during development and exploration of concerns, `Utility` and `Constants` could be updated with new topics and configurations;
2) for simplicity, I use Kafka (KRaft Mode) only image, but in some cases it might be useful to have a full complete docker image with Kafka UI, DB, etc.
3) in each package of concrete scenario provided a flow of `how_to_run_scenario_X` description;

---

## What we explore (6 scenarios):

1) **scenario_01_simple:**
    - Async `Producer` with callbacks — success path, failure path, send metrics;
    - `Consumer` runs infinitely, graceful shutdown via `Ctrl+C` (ShutdownHook + `consumer.wakeup()` pattern);
    - `WakeupException` as the correct interrupt mechanism for `poll()`;
    - [how to run](src/main/java/com/vbforge/scenario_01_simple/how_to_run_scenario_01_simple.md)


2) **scenario_02_demo_app:**
    - Three producer variants: single message (`MyProducer`), multiple with `null` key — round-robin (`MultiMessageProducer`), messages with explicit keys (`KeyedMessageProducer`);
    - Broadcast behaviour — `MyConsumer` and `MyConsumer2` use different group IDs → both receive all messages;
    - Load-balance behaviour — switching both consumers to the same group ID → each message delivered to one consumer only;
    - Kafka persistence — messages survive consumer downtime, replayed on restart;
    - [how to run](src/main/java/com/vbforge/scenario_02_demo_app/how_to_run_scenario_02_demo_app.md)


3) **scenario_03_load_balancing:**
    - Single `LoadBalancingConsumer` class run N times — no copy-paste consumers;
    - Consumer group: Kafka auto-assigns partitions across all running instances;
    - Rebalancing observed live: stop a consumer → others absorb its partitions;
    - Ceiling rule: consumers > partitions → excess consumers sit idle until a slot opens;
    - [how to run](src/main/java/com/vbforge/scenario_03_load_balancing/how_to_run_scenario_03_load_balancing.md)


4) **scenario_04_topic_keyed:**
    - `KeyedProducer` — user ID as message key; same key always routes to the same partition via `hash(key) % numPartitions`;
    - `KeyedConsumer` tracks per-key event counts — each consumer instance owns a fixed subset of keys, never overlap;
    - `OrderProofProducer` — sends a fixed step-by-step workflow per user (Login→Browse→Checkout→Logout) with synchronous send to prove steps always arrive in order;
    - Per-key ordering guarantee explained: ordering is preserved within a partition, not across the whole topic;
    - [how to run](src/main/java/com/vbforge/scenario_04_topic_keyed/how_to_run_scenario_04_topic_keyed.md)


5) **scenario_05_manual_offset_management:**
    - Four consumer strategies compared side-by-side against the same `OrderProducer`;
    - `AutoCommitConsumer` — default Kafka behavior, at-most-once delivery; crash experiment demonstrates how committed offsets cause messages to be silently skipped on restart;
    - `ManualCommitConsumer` — `commitSync()` called only after full batch succeeds, at-least-once guarantee; `OrderProcessingException` separates business failures (don't commit, redeliver) from unexpected runtime errors (stop consumer);
    - `BatchCommitConsumer` — commits every N records for balanced throughput; final `commitSync()` in `finally` block flushes remaining records on graceful shutdown;
    - `PerPartitionCommitConsumer` — commits each partition's offset independently after processing; isolates failures so one partition's problem doesn't block another's progress; `offset + 1` rule explained;
    - [how to run](src/main/java/com/vbforge/scenario_05_manual_offset_management/how_to_run_scenario_05_manual_offset_management.md)


6) **scenario_06_e_commerce_orders_app:**
    - descriptions not provided yet...


---