# workshop-kafka-apps

## Learning Kafka with Spring Boot & Thymeleaf

This repository is my **hands‑on workshop** for mastering Apache Kafka in **KRaft mode** (no ZooKeeper) using **Spring Boot 3**, **Thymeleaf**, and Docker.

The goal is not just "hello world" — but building **practical applications** that gradually cover real‑world Kafka patterns: reliable messaging, 
stateful processing, error handling, event sourcing, dead letter queues, etc.

---

## Why this repository exists

Kafka plays a **big role** in enterprise Java development — especially in fintech, e‑commerce, logistics, and microservices architectures. 
However, many tutorials stop at "send and receive a string". 

This workshop fills the gap by showing:
- ✅ How to build **production‑ready** producers/consumers
- ✅ Error handling strategies (retries, DLQ, poison pills)
- ✅ Exactly‑once semantics, idempotent producers, commit strategies
- ✅ Consumer rebalancing, partition awareness, state stores
- ✅ Visual feedback via Thymeleaf + WebSocket dashboards

All code runs against **Kafka in KRaft mode** (the modern, ZooKeeper‑less setup) inside Docker.

---

## Projects

| # | Project                          | Key Kafka Concepts | Target Skill |
|---|----------------------------------|--------------------|---------------|
| 1 | [**Echo Bot**](kafka-echo-bot)                 | `KafkaTemplate`, `@KafkaListener`, auto commit, basic topics | Verify your Kafka setup works — produce and consume with a simple web form |
| 2 | **Real‑time Dashboard**          | Consumer groups, rebalancing, in‑memory state aggregation, WebSocket streaming | Build live analytics from event streams |
| 3 | **Order Logger with Search**     | Idempotent producer, exactly‑once semantics, offset commit to database | Decouple ingestion from storage — Kafka as reliable buffer |
| 4 | **Event‑Sourced Shopping Cart**  | Compacted topics, state reconstruction from event log, snapshotting | Implement event sourcing pattern (auditable, replayable) |
| 5 | **Dead Letter Queue Visualizer** | `@RetryableTopic`, error handlers, poison pills, manual DLQ replay | Handle real‑world message failures gracefully |

Each project lives separately and can run independently.

---

## Tech stack

- **Java 17** (or 21)
- **Spring Boot 3.2.x** (latest stable)
- **Apache Kafka** (KRaft mode — no ZooKeeper)
- **Thymeleaf** + **WebSocket** (for live UI updates)
- **Docker** + **Docker Compose** (Kafka, optional MySql/PostgreSQL/H2)
- **Maven**

---

## Prerequisites

- Docker with Kafka in KRaft mode already running
- Basic Spring Boot knowledge (controllers, services, `application.yml`)
- No prior Kafka experience required

---

## Folder structure (planned)

```
workshop-kafka-apps/
├── README.md                     (this file)
├── kafka-echo-bot/               (Project #1)
├── kafka-dashboard/              (Project #2)
├── kafka-order-logger/           (Project #3)
├── kafka-shopping-cart/          (Project #4)
├── kafka-dlq-visualizer/         (Project #5)
└── other-apps-in-process
```

Each project is a **standalone Spring Boot application** with its own `pom.xml`, `docker-compose.yml` and `application.yml`.

---

## How deep should we know Kafka?

This workshop is designed according to:

| Level    | Topic                                           | Required    |
|----------|-------------------------------------------------|-------------|
| ✅ Must   | Produce/consume with Spring Boot                | Yes         |
| ✅ Must   | Topic, partition, offset, consumer group basics | Yes         |
| ✅ Must   | `application.yml` configuration                 | Yes         |
| ✅ Should | Commit strategies (auto, manual, ack mode)      | Yes         |
| ✅ Should | Error handling (retries, DLQ)                   | Yes         |
| ✅ Should | Idempotent producer & exactly‑once semantics    | Yes         |
| ✅ Should | Consumer rebalancing behavior                   | Yes         |
| ❌ Nice   | Kafka Streams DSL, state stores                 | For Middle+ |
| ❌ Nice   | Cluster administration, KRaft internals         | For Middle+ |

After completing all topics, we will have **practical, interview‑ready** knowledge.

---

## Running the projects

Each project contains its own:
- `docker-compose.yml` 
- `README.md` with specific run instructions
- Example `curl` commands or UI walkthrough

**Shared Kafka instance** (recommended):  
If you already have Kafka running in Docker on `localhost:9092`, simply reuse it across all projects.

---

## Learning philosophy

> **Don't go deep into Kafka Streams or cluster internals now. Master the producer/consumer API + error patterns — that's what real projects use daily.**

Each project takes **time** and includes:
- Step‑by‑step code walkthrough
- Expected output (screenshots where relevant)
- Common pitfalls and how to fix them

---

## Resources

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spring for Apache Kafka](https://docs.spring.io/spring-kafka/reference/)
- [Kafka in KRaft mode](https://developer.confluent.io/courses/kraft/kraft-intro/)

---

## Author

**vbforge**
[](https://github.com/vbforge)

---