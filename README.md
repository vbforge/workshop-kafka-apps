# workshop-kafka-apps

> Hands-on Kafka workshop built with **Spring Boot 3**, **Thymeleaf**, and **Docker** (KRaft mode — no ZooKeeper).  
> Each project is a standalone application targeting a real-world Kafka pattern, progressing from zero to production-ready knowledge.

---

## Goal

Most Kafka tutorials stop at "send and receive a string." This workshop doesn't.

The focus is on patterns that actually appear in production Java systems — reliable messaging, error handling, offset commit strategies, exactly-once semantics, and event sourcing. By the end, you will have practical, interview-ready knowledge of the producer/consumer API without getting lost in cluster internals or Kafka Streams DSL.

---

## Projects

| # | Project                                                                | Key Kafka Concepts                                                                                                                                                                                                                          |
|---|:-----------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 1 | [kafka-01-java-code-beginners](kafka-01-java-code-beginners/README.md) | Producers and Consumers using Java, showcasing various patterns including simple messaging, callbacks, key-based partitioning, graceful shutdown, and cooperative rebalancing. Configured with Kafka in Docker (+ Conduktor UI) (pure Java) |
| 2 | [kafka-02-learning-scenarios](kafka-02-learning-scenarios/README.md)   | 5 progressive scenarios: basic messaging → load balancing → keyed messages → manual offsets → complete e-commerce pipeline using pure Java Kafka client  (pure Java)                                                                        |
| 3 | [kafka-03-echo-bot](kafka-03-echo-bot/README.md)                       | `KafkaTemplate`, `@KafkaListener`, auto-commit, topic creation  (Spring Boot)                                                                                                                                                               |
| 4 | [kafka-04-dashboard](kafka-04-dashboard/README.md)                     | Consumer groups, partition awareness, in-memory aggregation, WebSocket streaming (Spring Boot)                                                                                                                                              |
| 5 | [kafka-05-notification-demo](kafka-05-notification-demo/README.md)     | A simple Spring Boot demo application demonstrating Apache Kafka (Kafka and Zookeeper using Docker) integration with Thymeleaf for sending and consuming notifications. (Spring Boot)                                                       |
| 6 | [kafka-06-producer-consumer-masterclass](kafka-06-producer-consumer-masterclass/README.md)     | Comprehensive Spring Boot + Kafka masterclass covering every producer/consumer pattern used in real projects: sync, async, keyed, filtered, transactional, error-handling, and manual offset control. (Spring Boot)                 |
| 7 | [kafka-07-producer-consumer-separate-modules](kafka-07-producer-consumer-separate-modules/README.md)     | Multi-module Spring Boot Kafka solution featuring separate producer-consumer architecture, KRaft broker, idempotent processing with caching, and Docker Compose orchestration. (Spring Boot)                 |

---

## Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| Java | 17 / 21 | Application runtime |
| Spring Boot | 3.4.x | Application framework |
| Spring Kafka | (managed by Boot) | Kafka producer/consumer abstraction |
| Apache Kafka | KRaft mode (no ZooKeeper) | Message broker |
| Thymeleaf + WebSocket | (managed by Boot) | Server-side UI with live updates |
| Docker + Docker Compose | Latest | Local Kafka + Conduktor setup |
| Maven | 3.9+ | Build tool |

---

## Learning Scope

This workshop is intentionally scoped. Here is exactly what will and won't be covered — and why.

| Priority | Topic |
|---|---|
| ✅ Must | Produce and consume messages with Spring Boot |
| ✅ Must | Topics, partitions, offsets, consumer groups |
| ✅ Must | `application.yml` configuration for producers and consumers |
| ✅ Must | Commit strategies: auto-commit vs manual ack |
| ✅ Must | Error handling: retries, `ErrorHandlingDeserializer`, poison pills |
| ✅ Must | Dead letter queues and `@RetryableTopic` |
| ✅ Should | Idempotent producer and exactly-once semantics |
| ✅ Should | Consumer rebalancing and partition assignment |
| ✅ Should | Event sourcing pattern with Kafka as the event log |
| ❌ Skip for now | Kafka Streams DSL and state stores |
| ❌ Skip for now | Cluster administration and KRaft internals |
| ❌ Skip for now | Schema Registry and Avro serialization |

> **Rule of thumb:** master the producer/consumer API with proper error handling — that is what 90% of real projects use day to day.

---

## Running the Projects

### Option A — Shared Kafka instance

If you already have Kafka running on `localhost:9092`, skip the per-project Docker Compose and point each app at it via `application.yml`. This is the normal development workflow once your environment is stable.

### Option B — Per-project Docker Compose (mostly used across repository projects)

Each project ships its own `docker-compose.yml` with Kafka in KRaft mode and a Conduktor UI panel.

```
cd kafka-echo-bot
docker compose up -d
./mvnw spring-boot:run
```

Conduktor UI is available at `http://localhost:8085` for visual inspection of topics, consumer groups, and offsets.
 
---

## Prerequisites

- Docker Desktop running
- Java 17 or 21 installed
- Maven 3.9+ (or use the `./mvnw` wrapper)
- Basic Spring Boot knowledge: controllers, services, `application.yml`
- No prior Kafka experience required — that is what this workshop is for

---

## Useful Kafka Documentations:

- [check useful commands with docker](helper_docs/useful-kafka-cli-commands-with-docker.md)
- [my kafka kraft guide](helper_docs/kafka_kraft_guide_vbforge.pdf)
- [Apache Kafka Beginners Guide](helper_docs/Apache_Kafka_Beginners_Guide.md)

---

## Resources

- [Spring for Apache Kafka — Reference Docs](https://docs.spring.io/spring-kafka/reference/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Kafka in KRaft mode — Confluent Developer](https://developer.confluent.io/courses/kraft/kraft-intro/)
- [Confluent cp-kafka Docker image](https://hub.docker.com/r/confluentinc/cp-kafka)

---

## Author

**vbforge** — [github.com/vbforge](https://github.com/vbforge)
