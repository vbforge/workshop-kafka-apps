# 📚 Apache Kafka - Beginner's Gudie

## 📑 Table of Contents

* [Core Concepts](#core-concepts)
* [Practical Scenarios](#practical-scenarios)
* [Java Development Examples](#java-development-examples)
* [Common Interview Questions](#common-interview-questions)
* [Quick Reference Card](#quick-reference-card)

---

## Core Concepts

### 🎯 Key Terminology

| Term | Description | Example |
|------|-------------|---------|
| **Topic** | Category/feed name for messages | `orders`, `user-events` |
| **Partition** | Ordered, immutable sequence of records within a topic | Topic `orders` with 3 partitions |
| **Producer** | Publishes messages to topics | Order service sending order events |
| **Consumer** | Reads messages from topics | Analytics service reading orders |
| **Consumer Group** | Group of consumers sharing the workload | 3 consumers processing orders in parallel |
| **Offset** | Unique ID of a record within a partition | Message #42 in partition 0 |
| **Broker** | Kafka server that stores data | Your localhost:9092 |
| **Replication Factor** | Number of copies of data | `replication-factor 1` (single node) |

### 🔑 Important Rules

1. **One partition → One consumer per group** (but one consumer can handle multiple partitions)
2. **Same key → Same partition** (guarantees ordering for that key)
3. **More partitions than consumers** = Some consumers handle multiple partitions
4. **More consumers than partitions** = Some consumers stay idle
5. **Different consumer groups** = Independent consumption (each group gets all messages)

---

## Practical Scenarios

- docker running
`docker-compose up -d`

### Scenario 1: Single Partition Topic 🎯
**Use Case:** Simple queue, strict ordering required

```

# Create topicA (1 partition)
docker exec -it --create --topic topicA --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092

# Verify
docker exec -it kafka-topics --describe --topic topicA --bootstrap-server localhost:9092

# Producer (Terminal 1)
docker exec -it kafka-console-producer --topic topicA --bootstrap-server localhost:9092

# Consumer (Terminal 2)
docker exec -it kafka-console-consumer --topic topicA --from-beginning --bootstrap-server localhost:9092
```

**Test:**
- Send: `Order 1`, `Order 2`, `Order 3`
- Consumer receives in exact order
- ✅ Perfect for workflows requiring strict sequence

---

### Scenario 2: Load Balancing with Consumer Group ⚖️
**Use Case:** Parallel processing, scalability

```
# Create topicB (3 partitions)
docker exec -it --create --topic topicB --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092

# Verify
docker exec -it kafka-topics.bat --describe --topic topicB --bootstrap-server localhost:9092

# Producer (Terminal 1)
docker exec -it kafka-console-producer --topic topicB --bootstrap-server localhost:9092

# Consumer 1 (Terminal 2)
docker exec -it kafka-console-consumer --topic topicB --group groupB --bootstrap-server localhost:9092

# Consumer 2 (Terminal 3)
docker exec -it kafka-console-consumer --topic topicB --group groupB --bootstrap-server localhost:9092

# Consumer 3 (Terminal 4)
docker exec -it kafka-console-consumer --topic topicB --group groupB --bootstrap-server localhost:9092

# Check partition assignment (Terminal 5)
docker exec -it kafka-consumer-groups --bootstrap-server localhost:9092 --group groupB --describe
```

**Expected Output:**
```
TOPIC     PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID        HOST
topicB    0          5               5               0    consumer-1-xxx     /127.0.0.1
topicB    1          4               4               0    consumer-2-xxx     /127.0.0.1
topicB    2          6               6               0    consumer-3-xxx     /127.0.0.1
```

**Test:**
- Send 15 messages from producer
- See them distributed across 3 consumers
- Stop Consumer 1 → Watch rebalancing (partitions redistributed)

---

### Scenario 3: Keyed Messages for Guaranteed Routing 🔑
**Use Case:** User sessions, order processing, maintaining per-entity order

```
# Create topicC (3 partitions)
docker exec -it --create --topic topicC --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092

# Verify
docker exec -it kafka-topics.bat --describe --topic topicC --bootstrap-server localhost:9092

# Producer with key support (Terminal 1)
docker exec -it kafka-console-producer --topic topicC --bootstrap-server localhost:9092 --property "parse.key=true" --property "key.separator=:"

# Consumer 1 (Terminal 2) - showing keys
docker exec -it kafka-console-consumer --topic topicC --group groupC --from-beginning --bootstrap-server localhost:9092 --property print.key=true --property key.separator=:

# Consumer 2 (Terminal 3)
docker exec -it kafka-console-consumer --topic topicC --group groupC --from-beginning --bootstrap-server localhost:9092 --property print.key=true --property key.separator=:

# Consumer 3 (Terminal 4)
docker exec -it kafka-console-consumer --topic topicC --group groupC --from-beginning --bootstrap-server localhost:9092 --property print.key=true --property key.separator=:

# Monitor (Terminal 5)
docker exec -it kafka-consumer-groups --bootstrap-server localhost:9092 --group groupC --describe
```

**Producer Input (key:value format):**
```
user123:Login
user456:Login
user123:View product A
user123:Add to cart
user456:View product B
user789:Login
user123:Checkout
user456:Add to cart
```

**Observation:**
- All `user123` messages → Same consumer
- All `user456` messages → Same consumer
- All `user789` messages → Same consumer
- ✅ Guarantees ordering per user

---

### Scenario 4: Multiple Independent Consumer Groups 🔄
**Use Case:** Multiple microservices consuming same data independently

```
# Reuse topicC 

# New consumer group (Terminal 6)
docker exec -it kafka-console-consumer --topic topicC --group groupD --from-beginning --bootstrap-server localhost:9092 --property print.key=true --property key.separator=:

# List all groups (Terminal 7)
docker exec -it kafka-consumer-groups --list --bootstrap-server localhost:9092
```

**Real-World Example:**
- **groupC** = Real-time order processing service
- **groupD** = Analytics/reporting service
- Both consume same messages independently

---

### Scenario 5: Offset Management 📍
**Use Case:** Replay messages, reset consumer position

```
# Check current offset
docker exec -it kafka-consumer-groups --bootstrap-server localhost:9092 --group groupB --describe

# Reset to beginning (replay all messages)
docker exec -it kafka-consumer-groups --bootstrap-server localhost:9092 --group groupB --reset-offsets --to-earliest --topic topicB --execute

# Reset to specific offset
docker exec -it kafka-consumer-groups --bootstrap-server localhost:9092 --group groupB --reset-offsets --to-offset 5 --topic topicB:0 --execute

# Reset to latest (skip all unread messages)
docker exec -it kafka-consumer-groups --bootstrap-server localhost:9092 --group groupB --reset-offsets --to-latest --topic topicB --execute
```

**⚠️ Note:** Consumer group must be stopped to reset offsets

---

### Scenario 6: Topic Configuration & Management ⚙️
**Use Case:** Understanding topic settings

```
# Create topic with custom retention
docker exec -it kafka-topics --create --topic topicD --partitions 2 --replication-factor 1 --config retention.ms=3600000 --bootstrap-server localhost:9092
# retention.ms=3600000 → 1 hour retention

# Modify topic configuration
docker exec -it kafka-configs --bootstrap-server localhost:9092 --entity-type topics --entity-name topicD --alter --add-config retention.ms=7200000

# View topic configuration
docker exec -it kafka-configs --bootstrap-server localhost:9092 --entity-type topics --entity-name topicD --describe

# Increase partitions (cannot decrease!)
docker exec -it kafka-topics --bootstrap-server localhost:9092 --topic topicD --alter --partitions 4

# Delete topic
docker exec -it kafka-topics --delete --topic topicD --bootstrap-server localhost:9092
```

---

## Java Development Examples

### Maven Dependencies
```xml
<dependencies>
    <!-- Kafka Client -->
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>4.1.0</version>
    </dependency>
    
    <!-- Logging (optional but recommended) -->
    <dependency>
        <groupId>org.slf4j</groupId>
        <artifactId>slf4j-simple</artifactId>
        <version>2.0.9</version>
    </dependency>
</dependencies>
```

### Example 1: Simple Producer
```java
import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {
        // Configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
                  "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
                  "org.apache.kafka.common.serialization.StringSerializer");
        
        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        // Send messages
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = 
                new ProducerRecord<>("topicA", "Message " + i);
            
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Sent to partition %d, offset %d%n",
                                      metadata.partition(), metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        }
        
        // Cleanup
        producer.flush();
        producer.close();
    }
}
```

### Example 2: Producer with Keys
```java
import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class KeyedProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
                  "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
                  "org.apache.kafka.common.serialization.StringSerializer");
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        String[] users = {"user1", "user2", "user3"};
        
        // Send 30 messages (10 per user)
        for (int i = 0; i < 30; i++) {
            String key = users[i % 3];
            String value = "Action " + i + " from " + key;
            
            ProducerRecord<String, String> record = 
                new ProducerRecord<>("topicC", key, value);
            
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Key: %s -> Partition: %d%n",
                                      key, metadata.partition());
                }
            });
        }
        
        producer.flush();
        producer.close();
    }
}
```

### Example 3: Simple Consumer
```java
import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "java-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("topicA"));
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = 
                    consumer.poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Partition: %d, Offset: %d, Key: %s, Value: %s%n",
                                      record.partition(), record.offset(), 
                                      record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
```

### Example 4: Consumer with Manual Commit
```java
import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ManualCommitConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "manual-commit-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("topicB"));
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = 
                    consumer.poll(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Processing: %s%n", record.value());
                    
                    // Simulate processing
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                
                // Commit only after successful processing
                consumer.commitSync();
                System.out.println("Committed offsets");
            }
        } finally {
            consumer.close();
        }
    }
}
```

### Example 5: Spring Kafka Producer
```java
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {
    
    private final KafkaTemplate<String, String> kafkaTemplate;
    
    public OrderProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    public void sendOrder(String orderId, String orderData) {
        kafkaTemplate.send("orders", orderId, orderData)
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    System.out.println("Order sent: " + orderId);
                } else {
                    System.err.println("Failed to send order: " + ex.getMessage());
                }
            });
    }
}
```

### Example 6: Spring Kafka Consumer
```java
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class OrderConsumer {
    
    @KafkaListener(topics = "orders", groupId = "order-processing-group")
    public void consumeOrder(String message) {
        System.out.println("Received order: " + message);
        // Process order logic here
    }
    
    @KafkaListener(
        topics = "orders", 
        groupId = "analytics-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void analyzeOrder(String message) {
        System.out.println("Analyzing order: " + message);
        // Analytics logic here
    }
}
```

---

## Common Interview Questions

### Conceptual Questions

**Q1: What is Apache Kafka and what problems does it solve?**
- Distributed streaming platform for high-throughput, fault-tolerant messaging
- Solves: Real-time data pipelines, event streaming, log aggregation, microservices communication

**Q2: Explain partitions and their purpose**
- Partitions enable parallelism and scalability
- Each partition is an ordered, immutable sequence of records
- More partitions = more consumers can work in parallel

**Q3: What is a consumer group?**
- Group of consumers sharing the work of consuming a topic
- Each partition is consumed by exactly one consumer in the group
- Enables load balancing and fault tolerance

**Q4: How does Kafka guarantee message ordering?**
- Ordering guaranteed within a partition, not across partitions
- Use message keys to route related messages to the same partition

**Q5: What is an offset?**
- Unique sequential ID for each record within a partition
- Consumer tracks its position using offsets
- Enables replay, resume, and at-least-once/exactly-once semantics

### Technical Questions

**Q6: What happens if you have more consumers than partitions?**
- Extra consumers remain idle
- Example: 3 partitions, 5 consumers → 2 consumers do nothing

**Q7: What is replication factor?**
- Number of copies of each partition across brokers
- `replication-factor=3` means data exists on 3 brokers
- Provides fault tolerance (your setup uses 1 since single node)

**Q8: Difference between `at-most-once`, `at-least-once`, and `exactly-once`?**
- **At-most-once:** May lose messages (auto-commit before processing)
- **At-least-once:** May duplicate (commit after processing, default)
- **Exactly-once:** No loss, no duplicates (requires transactions)

**Q9: What is a producer acknowledgment (acks)?**
- `acks=0`: Fire and forget (no confirmation)
- `acks=1`: Leader acknowledgment (default)
- `acks=all`: All replicas must acknowledge (safest)

**Q10: How to handle consumer lag?**
- Add more consumers (up to partition count)
- Increase partitions (requires careful planning)
- Optimize processing logic
- Monitor using `kafka-consumer-groups --describe`

### Coding Questions

**Q11: Write code to send a message to Kafka**
```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "key", "value");
producer.send(record);
producer.close();
```

**Q12: How to consume messages with manual offset commit?**
```java
props.put("enable.auto.commit", "false");
KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList("my-topic"));

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<String, String> record : records) {
        // Process record
    }
    consumer.commitSync(); // Manual commit
}
```

### Key Kafka Concepts Summary

| Concept | Description | Example |
|---------|-------------|---------|
| **Topic** | Category for messages | `orders`, `user-events` |
| **Partition** | Ordered log within topic | Topic with 3 partitions: 0, 1, 2 |
| **Producer** | Publishes messages | Order service sending events |
| **Consumer** | Reads messages | Analytics service processing events |
| **Consumer Group** | Load-sharing consumers | 3 consumers processing in parallel |
| **Offset** | Message position in partition | Message #42 in partition 0 |
| **Key** | Routes message to partition | user_id routes all user events together |
| **Commit** | Mark message as processed | Manual vs auto-commit |


### Must-Know Questions
1. **What is Kafka?** - Distributed streaming platform for real-time data
2. **Why partitions?** - Enable parallelism and scalability
3. **What is a consumer group?** - Multiple consumers sharing workload
4. **How to guarantee order?** - Use message keys to route to same partition
5. **What is offset?** - Position of message in partition
6. **At-least-once vs exactly-once?** - Delivery guarantee semantics

### Coding Challenges
- Write a producer that sends keyed messages
- Implement a consumer with manual commit
- Handle consumer rebalancing gracefully
- Implement retry logic for failed messages

---

## Quick Reference Card

| Task | Command |
|------|---------|
| Create topic | `--create --topic NAME --partitions N` |
| List topics | `--list` |
| Produce | `kafka-console-producer --topic NAME` |
| Consume | `kafka-console-consumer --topic NAME --group GROUP` |
| Describe group | `kafka-consumer-groups --group NAME --describe` |
| Reset offset | `kafka-consumer-groups --group NAME --reset-offsets --to-earliest` |

---
