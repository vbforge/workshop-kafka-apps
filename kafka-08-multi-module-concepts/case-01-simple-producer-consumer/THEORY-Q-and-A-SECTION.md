# Case 01: Q&A Document

## 🎓 Theory Section

### Core Concepts Covered

| Concept | Description |
|---------|-------------|
| **Producer** | Component that sends messages to Kafka topics using `KafkaTemplate` |
| **Consumer** | Component that reads messages from Kafka topics using `@KafkaListener` |
| **Topic** | Named logical channel where messages are organized |
| **Consumer Group** | Set of consumers that work together to consume messages from a topic |
| **Serialization** | Converting Java objects to bytes for transmission |
| **Deserialization** | Converting bytes back to Java objects on receipt |
| **Bootstrap Server** | Initial connection point to Kafka cluster |
| **Offset** | Unique position of a message within a partition |

### Key Architecture Decisions

1. **Separate topics for different data types** — JSON messages go to `case-01-topic`, Strings to `case-01-topic-string`. This prevents deserialization conflicts.

2. **Manual Kafka configuration** — Spring Boot 4.x requires explicit `ConsumerFactory` and `ConcurrentKafkaListenerContainerFactory` beans when customizing serializers.

3. **`@EnableKafka` annotation** — Required on `MainApp` to activate `@KafkaListener` scanning.

---

## 📝 Interview Q&A

### Q1: What's the difference between synchronous and asynchronous message production? Which one does this case use?

**Answer:**

- **Synchronous:** Producer waits for broker acknowledgment before continuing. Use `kafkaTemplate.send().get()`.
- **Asynchronous:** Producer sends and continues immediately. Callback handles result later.

**This case uses ASYNCHRONOUS** — `kafkaTemplate.send()` returns immediately without waiting. The method doesn't call `.get()` or use callbacks.

```java
// Current (async)
kafkaTemplate.send(topic, key, message);

// Would be sync
kafkaTemplate.send(topic, key, message).get();
```

---

### Q2: Why do we need separate topics for Message objects and String messages? Can't we put both on the same topic?

**Answer:** No — it would cause deserialization failures.

**Why:** The consumer deserializer is configured for a specific type:
- `JacksonJsonDeserializer` expects JSON structure (`{"id":"...", "content":"..."}`)
- `StringDeserializer` expects plain text

If a String consumer reads a JSON message (or vice versa), deserialization throws an exception.

**Solution:** Separate topics = each consumer gets only the data type it expects.

---

### Q3: What does `@EnableKafka` do and why do we need it?

**Answer:** `@EnableKafka` activates Spring's annotation-driven Kafka listener infrastructure.

**Without it:** `@KafkaListener` annotations are ignored — consumers never receive messages.

**With it:** Spring scans for `@KafkaListener` methods and creates the necessary proxy beans to handle message delivery.

```java
@SpringBootApplication
@EnableKafka  // ← Required!
public class MainApp {
    // ...
}
```

---

### Q4: Explain the role of `ConsumerFactory` and `ConcurrentKafkaListenerContainerFactory`.

**Answer:**

| Factory | Responsibility |
|---------|----------------|
| `ConsumerFactory` | Creates `KafkaConsumer` instances with configuration (bootstrap servers, group ID, deserializers) |
| `ConcurrentKafkaListenerContainerFactory` | Wraps consumers to create message listeners; manages concurrency, threading, and container lifecycle |

**In code:**
```java
@Bean
public ConsumerFactory<String, Message> consumerFactory() {
    // Configuration: where to connect, how to deserialize
}

@Bean
public ConcurrentKafkaListenerContainerFactory<String, Message> kafkaListenerContainerFactory() {
    // Uses consumerFactory to create listener containers
    factory.setConsumerFactory(consumerFactory());
}
```

---

### Q5: What happens if a consumer dies or is restarted? Does it lose its place in the topic?

**Answer:** No — Kafka tracks each consumer group's offset.

**Scenario:**
1. Consumer reads message at offset 5
2. Consumer stops
3. Consumer restarts with same `group.id`

**Result:** Consumer resumes from offset 6 — no messages lost or re-read.

**Demonstrated in "How to Break It" Scenario 4:** Changing the `groupID` starts a new group, and offsets reset to `earliest` or `latest` based on configuration.

---

### Q6: Why do we use `scope=provided` for Lombok dependency?

**Answer:** Lombok is a **compile-time only** dependency.

**What `provided` means:**
- Available during compilation (generates getters/setters/constructors)
- **NOT included** in the final executable JAR
- Reduces JAR size and avoids potential conflicts

```xml
<dependency>
    <groupId>org.projectlombok</groupId>
    <artifactId>lombok</artifactId>
    <scope>provided</scope>  <!-- Not packaged -->
</dependency>
```

**Verification:** `mvn clean package && jar tf target/*.jar | grep lombok` returns nothing.

---

### Q7: How do you verify that messages are actually being consumed, not just sent?

**Answer:** Multiple methods:

| Method | Command/Approach |
|--------|------------------|
| **Application logs** | Watch terminal where `mvn spring-boot:run` runs |
| **Consumer group lag** | `docker exec kafka-08-broker kafka-consumer-groups --describe --group case-01-consumer-group` — `LAG = 0` means all consumed |
| **Console consumer** | `docker exec -it kafka-08-broker kafka-console-consumer --topic case-01-topic --from-beginning` |
| **Actuator metrics** | (Future case) `/actuator/metrics/kafka.consumer.records.consumed.total` |

**Quickest:** Add explicit logging in `ConsumerService`:
```java
@KafkaListener(...)
public void consume(Message message) {
    System.out.println(">>> CONSUMER TRIGGERED <<<");
    // ... processing
}
```

---

## 📊 Quick Reference Card

| Term | Definition |
|------|------------|
| `KafkaTemplate` | Spring's helper for sending messages |
| `@KafkaListener` | Annotation marking a method as message consumer |
| `ProducerFactory` | Creates KafkaProducer instances |
| `ConsumerFactory` | Creates KafkaConsumer instances |
| `bootstrap-servers` | Initial connection address (localhost:9092) |
| `group.id` | Identifier for a consumer group |
| `auto.offset.reset` | Behavior when no committed offset exists (`earliest` / `latest`) |
| `JacksonJsonSerializer` | Converts Java objects ↔ JSON bytes |

---

## ✅ Self-Assessment

After studying this Q&A, you should be able to:

- Explain async vs sync production
- Justify separate topics for different data types
- Describe the purpose of `@EnableKafka`
- Differentiate between `ConsumerFactory` and `ContainerFactory`
- Explain offset persistence across consumer restarts
- Understand why Lombok uses `scope=provided`
- List methods to verify message consumption

---
