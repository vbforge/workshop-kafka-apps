# Case 17 — Testcontainers (Integration Tests with Real Kafka)

| Field          | Value                                          |
|----------------|------------------------------------------------|
| Module         | `case-17-testcontainers`                       |
| Port           | `8817`                                         |
| Events Topic   | `case-17-events-topic`                         |
| Consumer Group | `case-17-consumer-group`                       |
| Spring Boot    | `4.0.6`                                        |
| Java           | `21`                                           |
| Testcontainers | `1.20.4`                                       |

---

## What This Case Covers

| Concept | Description |
|---------|-------------|
| **`KafkaContainer`** | Testcontainers module that spins up a real Kafka broker in Docker for tests |
| **`@DynamicPropertySource`** | Injects the container's dynamic port into the Spring test context before context load |
| **Shared static container** | One container per JVM run — shared across all test classes via abstract base class |
| **`CountDownLatch` pattern** | Test synchronization for async `@KafkaListener` — block test thread until N messages arrive |
| **Test isolation** | Unique consumer group IDs per test, `resetLatch()` / `clear()` between tests |
| **Context loading** | How `@SpringBootTest` + `@DynamicPropertySource` + manual Kafka config all wire together |

### Why Testcontainers instead of `@EmbeddedKafka`?

| | `@EmbeddedKafka` | Testcontainers `KafkaContainer` |
|--|--|--|
| Real Kafka | ❌ In-process mock | ✅ Real Docker container |
| Version match | ❌ Fixed to spring-kafka-test dependency | ✅ Any image — match production exactly |
| Behavior fidelity | Lower — embedded has known edge-case differences | Higher — same binary as production |
| Startup time | ~1s | ~10–20s (first run), ~5s (cached image) |
| Resource usage | Low | Medium (Docker required) |
| CI requirements | Just JVM | Docker in CI (standard in 2024+) |

For learning early concepts, `@EmbeddedKafka` is fine. For serious integration tests — especially for error handling, transactions, offsets — Testcontainers is the correct tool.

---

## Project Structure

```
case-17-testcontainers/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/vbforge/case17/
    │   │   ├── MainApp.java
    │   │   ├── config/
    │   │   │   └── KafkaConfig.java
    │   │   ├── controller/
    │   │   │   └── EventController.java
    │   │   ├── model/
    │   │   │   ├── EventMessage.java
    │   │   │   └── ProducerResponse.java
    │   │   └── service/
    │   │       ├── EventProducerService.java
    │   │       └── EventConsumerService.java     ← has CountDownLatch + received list
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/vbforge/case17/
            ├── AbstractKafkaIntegrationTest.java  ← static KafkaContainer + @DynamicPropertySource
            ├── ProducerIntegrationTest.java        ← verifies producer output via raw consumer
            ├── ConsumerIntegrationTest.java        ← verifies @KafkaListener via latch pattern
            └── EndToEndIntegrationTest.java        ← full round-trip + edge cases
```

---

## Quick Start

### Run the app normally (uses docker-compose Kafka)
```bash
docker compose up -d
cd case-17-testcontainers
mvn spring-boot:run
```

### Run integration tests (Testcontainers starts its own Kafka)
```bash
# Docker must be running — Testcontainers will pull confluentinc/cp-kafka:8.0.0
cd case-17-testcontainers
mvn test
```

Expected output:
```
[INFO] -------------------------------------------------------
[INFO]  T E S T S
[INFO] -------------------------------------------------------
[INFO] Running com.vbforge.case17.ConsumerIntegrationTest
Testcontainers - Starting containers...
[INFO] Tests run: 4, Failures: 0, Errors: 0, Skipped: 0
[INFO] Running com.vbforge.case17.EndToEndIntegrationTest
[INFO] Tests run: 3, Failures: 0, Errors: 0, Skipped: 0
[INFO] Running com.vbforge.case17.ProducerIntegrationTest
[INFO] Tests run: 2, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

---

## How It Works

### Step 1: AbstractKafkaIntegrationTest starts the container
```java
@Container
static final KafkaContainer kafka =
    new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:8.0.0"));
```
`static` = started once per JVM, shared by all test classes. `@Testcontainers` + `@Container` manages lifecycle.

### Step 2: @DynamicPropertySource overrides bootstrap-servers
```java
@DynamicPropertySource
static void kafkaProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
}
```
Runs after container starts, before Spring context builds. `getBootstrapServers()` returns `localhost:<random-port>`.

### Step 3: Spring context loads with the real port
`KafkaConfig.java` reads `${spring.kafka.bootstrap-servers}` — now it's the Testcontainers port. All beans (KafkaTemplate, ConsumerFactory, etc.) connect to the test container automatically.

### Step 4: Tests use CountDownLatch for synchronization
```java
consumerService.resetLatch(5);
producerService.sendBatch(5, "ORDER_PLACED");
boolean completed = consumerService.getLatch().await(15, TimeUnit.SECONDS);
assertThat(completed).isTrue();
assertThat(consumerService.getReceivedEvents()).hasSize(5);
```

---

## Verify via Docker CLI

```bash
# While tests run, you can see the container briefly:
docker ps | grep kafka

# After tests, the container is stopped and removed automatically.
```

---

## How to Break It (Failure Scenarios)

### Scenario 1 — Wrong topic name in test
In `ProducerIntegrationTest.consumeFromTopic()`, change `"case-17-events-topic"` to `"wrong-topic"`. The raw consumer subscribes to a topic with no records — `result` stays empty. The test fails: `assertThat(consumed).hasSize(3)` fails because consumed is empty. Shows why topic names must match exactly.

### Scenario 2 — Latch count too high
In `ConsumerIntegrationTest`, change `resetLatch(1)` to `resetLatch(10)` before a single-message test. The latch never reaches 0. `await(15, TimeUnit.SECONDS)` returns `false`. `assertThat(completed).isTrue()` fails. This is how latch misconfiguration manifests — fast failure in 15s, not an infinite hang.

### Scenario 3 — Non-unique consumer groups (data bleed)
In `ProducerIntegrationTest.consumeFromTopic()`, change the group ID to a hardcoded value like `"fixed-group"`. The second test reuses the same group, which already has committed offsets pointing past the first test's messages. It might receive zero messages from the current test. Shows why unique group IDs per test are essential for isolation.

### Scenario 4 — Missing Docker
Stop Docker Desktop. Run `mvn test`. Testcontainers throws `DockerClientException: Cannot connect to the Docker daemon`. Clear failure with a readable message — Testcontainers doesn't silently fall back to embedded.

---

## API Reference

| Method | Endpoint | Params | Returns |
|--------|----------|--------|---------|
| GET | `/api/health` | — | `200` string |
| POST | `/api/events/send` | `count` (default 5), `type` (default ORDER_PLACED) | `200 ProducerResponse` |
| GET | `/api/events/stats` | — | `200` object |

---

## Learning Checklist

- [ ] Run `mvn test` — all tests pass against a real Kafka container
- [ ] Add a `Thread.sleep(100)` before `latch.await()` in a test and confirm it still passes (consumer needs time to start up on first test)
- [ ] Explain what `@DynamicPropertySource` does and why `application.yml` alone isn't enough
- [ ] Explain the CountDownLatch pattern: why `resetLatch(N)` before send, not after
- [ ] Explain why unique consumer group IDs are required for test isolation
- [ ] Explain the difference between this test setup and `@EmbeddedKafka` — tradeoffs for each
- [ ] Break Scenario 3 yourself — observe the data bleed with a fixed group ID

---

## See Also

- [THEORY-Q-and-A-SECTION.md](THEORY-Q-and-A-SECTION.md)
- case-08 → offset management concepts tested in `ProducerIntegrationTest` (seek, commit)
- case-13 → transactions (Testcontainers would be the proper way to test exactly-once)
