# Kafka 08 - Multi-Module Concepts

## Goal of the Project
Go through **each Kafka concept** with Java 21 and Spring Boot 4.x, using Docker-based Kafka (KRaft mode).
Each case is a **completely independent, runnable project** — no shared dependencies, no accidental complexity bleeding between concepts.

## Core Philosophy
- **One concept = One project** (even if trivial)
- **Copy-paste friendly** (each case stands alone)
- **Docker Kafka** shared across all cases
- **Production patterns** from day one

## Prerequisites
- Java 21
- Docker & Docker Compose
- Spring Boot 4.x

---

## My Tips

1. **Each module has its own README** with:
    - what this case demonstrates, run commands (mvn spring-boot:run), expected output
    - **`src/main/java/...`** — complete runnable code
    - **`application.yml`** — Kafka connection to `localhost:9092`
    - How to break it (failure scenarios)
    - **Postman collection** (optional) — API examples

2. **Used Docker volumes** for persistent Kafka data across cases

3. **Shared docker-compose.yml** — all cases connect to `localhost:9092`

4. **Module ports vary** — each case uses a different server port to avoid conflicts:
    - `case-01` → 8081
    - `case-02` → 8082
    - `case-03` → 8083
    - ... increment by 1

---

## 🗂️ Project Structure (focused modules)

```
kafka-08-multi-module-concepts/
├── pom.xml                              # Parent POM with all modules
├── docker-compose.yml                   # Shared Kafka (KRaft mode)
│
├── case-01-simple-producer-consumer/    # Foundation
├── case-02-producer-sync/               # Blocking send
├── case-03-producer-async/              # Non-blocking + callbacks
├── case-04-producer-key-based/          # Partition routing
│
├── case-05-consumer-annotation/         # @KafkaListener
├── case-06-consumer-manual-poll/        # Manual poll + commit
├── case-07-consumer-groups/             # Multi-group + rebalancing
├── case-08-consumer-offset-management/  # seek() + offset strategies
│
├── case-09-error-handling-basic/        # @KafkaHandler
├── case-10-error-handling-retry/        # Retry + backoff
├── case-11-error-handling-dlt/          # Dead Letter Topic
├── case-12-error-handling-global/       # DefaultErrorHandler
│
├── case-13-transactions/                # Exactly-once + @Transactional
│
├── case-14-validation-dto/              # Bean validation + DTO
├── case-15-batch-processing/            # High throughput batch
├── case-16-messaging-patterns/          # Request-reply + filtering
└── case-17-testcontainers/              # Integration tests
```

---

## 📚 Complete Concepts Mapping

| #  | Module                                                                                   | Concepts Covered                                                                                                                    |
|----|------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| 01 | [**case-01-simple-producer-consumer**](case-01-simple-producer-consumer/README.md)       | Basic producer-consumer, JSON serialization, topic auto-creation                                                                    |
| 02 | [**case-02-producer-sync**](case-02-producer-sync/README.md)                             | Sync producer, `Future.get()`, blocking send, timeout handling, error propagation                                                   |
| 03 | [**case-03-producer-async**](case-03-producer-async/README.md)                           | Async producer, callbacks, `CompletableFuture`, non-blocking patterns                                                               |
| 04 | [**case-04-producer-key-based**](case-04-producer-key-based/README.md)                   | Key-based partitioning, same key → same partition, custom partitioner                                                               |
| 05 | [**case-05-consumer-annotation**](case-05-consumer-annotation/README.md)                 | `@KafkaListener`, auto commit, concurrent consumers                                                                                 |
| 06 | [**case-06-consumer-manual-poll**](case-06-consumer-manual-poll/README.md)               | Manual `poll()`, manual commit, pause/resume, `seek()`                                                                              |
| 07 | [**case-07-consumer-groups**](case-07-consumer-groups/README.md)                         | Consumer groups, rebalancing, `group.id` behavior, partition assignment                                                             |
| 08 | [**case-08-consumer-offset-management**](case-08-consumer-offset-management/README.md)   | Offset management, `seek()`, `earliest`/`latest`/`none`, manual offset storage                                                      |
| 09 | [**case-09-error-handling-basic**](case-09-error-handling-basic/README.md)               | `@KafkaHandler`, exception throwing, `SeekToCurrentErrorHandler`                                                                    |
| 10 | [**case-10-error-handling-retry**](case-10-error-handling-retry/README.md)               | Retry template, backoff policies, recoverable vs non-recoverable errors                                                             |
| 11 | [**case-11-error-handling-dlt**](case-11-error-handling-dlt/README.md)                   | Dead Letter Topic, `@DltHandler`, poison pills, DLT monitoring                                                                      |
| 12 | [**case-12-error-handling-global**](case-12-error-handling-global/README.md)             | `DefaultErrorHandler`, error router, fallbacks, error topic                                                                         |
| 13 | [**case-13-transactions**](case-13-transactions/README.md)                               | Transactions, exactly-once semantics, `@Transactional`, idempotent producer, rollback                                               |
| 14 | [**case-14-validation-dto**](case-14-validation-dto/README.md)                           | DTO validation, Bean Validation (JSR-380), custom validators, error responses                                                       |
| 15 | [**case-15-batch-processing**](case-15-batch-processing/README.md)                       | Batch processing, `MAX_POLL_RECORDS`, batch listener, manual batch commit                                                           |
| 16 | [**case-16-messaging-patterns**](case-16-messaging-patterns/README.md)                   | Request-reply (RPC over Kafka), correlation IDs, temporary topics, message filtering, `@Filter`, header-based routing               |
| 17 | [**case-17-testcontainers**](case-17-testcontainers/README.md)                           | Testcontainers, integration tests with real Kafka                                                                                   |

---

## 🚀 Quick Start

#### 1. Start Kafka
```bash
cd kafka-08-multi-module-concepts
docker compose up -d
```

#### 2. Run any module
```bash
cd case-01-simple-producer-consumer
mvn spring-boot:run
```

#### 3. Verify with Postman or curl
```bash
curl -X POST http://localhost:8081/api/messages \
  -H "Content-Type: application/json" \
  -d '{"id":1,"content":"Hello Kafka"}'
```

#### 4. Stop Kafka
```bash
docker compose down

# or completely remove with volume
docker compose down -v
```

---

## Kafka (docker-compose.yml) shared for all cases

```yml
# ============================================================
# Kafka in KRaft mode — no ZooKeeper required
# Confluent cp-kafka 8.0.0 (Kafka 4.x)
#
# Usage:
#   docker compose up -d       — start in background
#   docker compose down        — stop and remove containers
#   docker compose logs -f     — tail logs
#
# Java app connects on: localhost:9092
# 
# By default KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true" (if you need to create topics manually set to: 'false')
# ============================================================

services:
  kafka:
    image: confluentinc/cp-kafka:8.0.0
    container_name: kafka-08-broker
    ports:
      - "9092:9092"    # external — your Java app connects here
      - "19092:19092"  # internal broker-to-broker traffic
      - "9099:9099"    # KRaft controller port
    environment:
      # ---- KRaft cluster identity ----
      CLUSTER_ID: "kafka-08-multi-module-concepts-cluster"
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller

      # ---- Listener configuration ----
      KAFKA_LISTENERS: INTERNAL://0.0.0.0:19092,EXTERNAL://0.0.0.0:9092,CONTROLLER://0.0.0.0:9099
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:19092,EXTERNAL://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka:9099"

      # ---- Replication / transactions (single node = factor 1) ----
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1

      # ---- Convenience settings ----
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "true"
      KAFKA_LOG_RETENTION_HOURS: 2

    healthcheck:
      test: ["CMD-SHELL", "kafka-broker-api-versions --bootstrap-server localhost:19092 || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 10
```
---