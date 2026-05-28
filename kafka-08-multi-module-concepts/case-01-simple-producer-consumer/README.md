# Case 01: Simple Producer & Consumer

| Property | Value |
|----------|-------|
| **Module** | `case-01-simple-producer-consumer` |
| **Parent** | `kafka-08-multi-module-concepts` |
| **Java** | 21 |
| **Spring Boot** | 4.0.6 |
| **Kafka** | cp-kafka 8.0.0 (KRaft mode) |
| **App port** | 8081 |

---

## 📖 What This Case Covers

- Basic `KafkaTemplate` producer via REST endpoint
- Basic `@KafkaListener` consumer
- JSON serialization/deserialization with `JacksonJsonSerializer` / `JacksonJsonDeserializer`
- String serialization for simple text messages
- Topic auto-creation via broker config
- End-to-end message flow verification
- Two producer patterns: **Object (Message)** and **String**

> The key decision here: separate topics for JSON and String. This is intentional and clean. 
> Putting both message types on the same topic causes deserialization conflicts — the JSON consumer doesn't know how to handle a raw string and throws errors. 
> Separate topics means each consumer gets exactly what it expects, no noise, no errors.

---

## 📁 Project Structure

```
case-01-simple-producer-consumer/
├── src/main/java/com/vbforge/case01/
│   ├── config/
│   │   └── KafkaConfig.java                  # Producer/Consumer configuration (for Object and String)
│   ├── controller/
│   │   └── ProducerController.java           # REST endpoints
│   ├── model/
│   │   └── Message.java                      # DTO with id, content, timestamp
│   ├── service/
│   │   ├── ConsumerService.java              # @KafkaListener consumer
│   │   └── ProducerService.java              # KafkaTemplate wrapper
│   └── MainApp.java                          # Spring Boot entry point (@EnableKafka)
├── src/main/resources/
│   └── application.yml                       # Configuration (port 8081)
├── src/test/java/
│   └── MainAppTests.java                     # Context load test
├── pom.xml
└── README.md
```

---

## 🚀 Quick Start

### Step 1: Start Kafka
From the **project root** (`kafka-08-multi-module-concepts/`):

```bash
docker compose up -d
```

### Step 2: Build & Run

```bash
cd case-01-simple-producer-consumer
mvn clean install
mvn spring-boot:run
```

Expected output:
```
Started MainApp in X.XXX seconds
```

### Step 3: Test with HTTP Requests

**Health check:**
```bash
GET http://localhost:8081/api/producer/health
```
Response: `"Producer is Running!"`

**Send default message (Message object):**
```bash
POST http://localhost:8081/api/producer/send
```
Response:
```json
{
  "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "content": "Hello from Kafka!",
  "timestamp": "2024-01-15T10:30:45.123"
}
```

**Send custom message (Message object):**
```bash
POST http://localhost:8081/api/producer/send?content=My first Kafka message!
```
Response:
```json
{
  "id": "b2c3d4e5-f6a7-8901-bcde-f12345678901",
  "content": "My first Kafka message!",
  "timestamp": "2024-01-15T10:31:22.456"
}
```

**Send string message (String only):**
```bash
POST http://localhost:8081/api/producer/send-string?content=Simple String value
```
Response:
```
"Simple String value"
```

### Step 4: Verify Consumer Output

After sending messages, check the application logs:

```
===== Message Received =====
  ID:        a1b2c3d4-e5f6-7890-abcd-ef1234567890
  Content:   Hello from Kafka!
  Timestamp: 2024-01-15T10:30:45.123
============================

===== Message Received =====
  ID:        b2c3d4e5-f6a7-8901-bcde-f12345678901
  Content:   My first Kafka message!
  Timestamp: 2024-01-15T10:31:22.456
============================
```

**Note:** String messages are NOT consumed by the current consumer (expects `Message` type). This demonstrates type safety.

---

## 🐳 Verify via Docker CLI

No local Kafka install needed. All CLI commands run inside the container:

**List topics:**
```bash
docker exec kafka-08-broker kafka-topics --list --bootstrap-server localhost:9092
```

**Describe topic:**
```bash
docker exec kafka-08-broker kafka-topics --describe --topic case-01-topic --bootstrap-server localhost:9092
```

**Console consumer (watch messages live):**
```bash
docker exec -it kafka-08-broker kafka-console-consumer \
  --topic case-01-topic \
  --from-beginning \
  --bootstrap-server localhost:9092
```

**Check consumer group lag:**
```bash
docker exec kafka-08-broker kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group case-01-consumer-group
```

---

## 🧪 IntelliJ HTTP Client Files

The module includes `.http` files for easy testing in IntelliJ IDEA:

| File | Endpoint |
|------|----------|
| `producer-health.http` | `GET /api/producer/health` |
| `producer-send-default.http` | `POST /api/producer/send` (default message) |
| `producer-send.http` | `POST /api/producer/send?content=...` |
| `producer-send-string.http` | `POST /api/producer/send-string?content=...` |

Click the green arrow next to each file to execute directly.

---

## 🔥 How to Break It (Failure Scenarios)

### Scenario 1: Kafka not running
```bash
# Don't start docker compose
mvn spring-boot:run
```
**Expected error:**
```
Connection refused: localhost/127.0.0.1:9092
org.apache.kafka.common.errors.TimeoutException
```

### Scenario 2: Wrong topic name
Change `application.yml`:
```yaml
kafka:
  topic:
    test: non-existent-topic
```
**Expected behavior:** Producer sends successfully (topic auto-creation may create it), but if auto-creation is disabled, consumer receives nothing.

### Scenario 3: Wrong deserializer
Change consumer value-deserializer to StringDeserializer:
```
ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class;
ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class;
```
**Expected error:**
```
org.springframework.kafka.support.serializer.DeserializationException: 
Failed to deserialize JSON
```

### Scenario 4: Consumer group restart
1. Send 5 messages
2. Stop the Spring Boot app (Ctrl+C)
3. Change `groupID` in `application.yml`:
   ```yaml
   consumer:
     groupID: case-01-consumer-group-v2
   ```
4. Restart the app
   **Expected behavior:** Only new messages consumed (old group offset lost, previous messages not re-processed)

### Scenario 5: Send String but consumer expects Message
```bash
POST http://localhost:8081/api/producer/send-string?content=Hello
```
**Expected:** No consumer log appears (deserialization error silently logged).

---

## 📤 API Reference

| Method | Endpoint | Parameter | Response |
|--------|----------|-----------|----------|
| GET | `/api/producer/health` | none | `String` |
| POST | `/api/producer/send` | `content` (optional) | `Message` object |
| POST | `/api/producer/send-string` | `content` (optional) | `String` |

---

## ⚙️ Configuration Reference

### `application.yml` key properties

| Property                         | Value | Description                        |
|----------------------------------|-------|------------------------------------|
| `server.port`                    | 8081 | HTTP server port                   |
| `spring.kafka.bootstrap-servers` | localhost:9092 | Kafka broker address               |
| `kafka.topic.test`               | case-01-topic | Topic name (for Messages)          |
| `kafka.topic.test-string`        | case-01-topic-string | Topic name (for String)            |
| `kafka.consumer.groupID`         | case-01-consumer-group | Consumer group ID                  |
| `kafka.default.message`          | Hello from Kafka! (default message) | Default message when none provided |

---

## 🛑 Stop Everything

### Stop Spring Boot app
Press `Ctrl+C` in the terminal where the app is running.

### Stop Kafka
From the project root (`kafka-08-multi-module-concepts/`):
```bash
docker compose down
```

Or when need to completely remove with data (volume will be removed as well):
```bash
docker compose down -v
```

---

## ✅ Learning Checklist

- Kafka starts successfully with `docker compose up -d`
- Spring Boot app starts on port 8081
- Health endpoint returns `"Producer is Running!"`
- Default message sent via `/send` → consumer logs output
- Custom message sent via `/send?content=...` → consumer logs output
- String message sent via `/send-string` → no consumer output (demonstrates type safety)
- Docker CLI commands work to inspect topics
- Failure scenarios produce expected errors

- **[check more theory and Q&A section](THEORY-Q-and-A-SECTION.md)**

---
