# kafka-07-producer-consumer-separate-modules

> A production-ready Spring Boot application demonstrating Kafka event-driven architecture with separate producer and consumer modules. 
> Features KRaft-mode Kafka (no ZooKeeper), manual offset management, idempotent consumer processing with Caffeine caching, 
> and Conduktor Console for real-time monitoring — all containerized with Docker Compose.

---

## 🚀 Project Features

- **Multi-module Maven project** with parent POM
- **KRaft-mode Kafka** (no Zookeeper needed)
- **Conduktor Console** for visual monitoring
- **Producer REST API** to send messages
- **Consumer with manual offset management** and idempotency
- **Caffeine caching** for duplicate detection
- **JSON serialization** with Jackson
- **Lombok** for clean code
- **Health checks** and proper shutdown handling

---

## 📁 Project Structure

```
kafka-07-producer-consumer-separate-modules/
│
├── pom.xml                                 # Parent POM
├── README.md                               # This file
├── QUICK_START.md                          # 📘 Detailed step-by-step guide
├── docker-compose.yml                      # Kafka + Conduktor + PostgreSQL
│
├── producer-app/
│   ├── pom.xml
│   └── src/
│       └── main/
│           ├── java/
│           │   └── com/
│           │       └── vbforge/
│           │           └── producerapp/
│           │               ├── ProducerApp.java
│           │               ├── config/
│           │               │   └── KafkaProducerConfig.java
│           │               ├── controller/
│           │               │   └── MessageController.java
│           │               ├── service/
│           │               │   └── MessageProducerService.java
│           │               └── model/
│           │                   └── MessageEvent.java
│           └── resources/
│               └── application.yml
│
└── consumer-app/
    ├── pom.xml
    └── src/
        └── main/
            ├── java/
            │   └── com/
            │       └── vbforge/
            │           └── consumerapp/
            │               ├── ConsumerApp.java
            │               ├── config/
            │               │   ├── KafkaConsumerConfig.java
            │               │   └── CacheConfig.java
            │               ├── listener/
            │               │   └── MessageListener.java
            │               ├── service/
            │               │   ├── MessageCacheService.java
            │               │   └── MessageProcessingService.java
            │               └── model/
            │                   └── MessageEvent.java
            └── resources/
                └── application.yml
```

---

## 🛠️ Prerequisites

| Tool | Version | Verification Command |
|------|---------|---------------------|
| **Java** | 17+ | `java -version` |
| **Maven** | 3.8+ | `mvn -version` |
| **Docker** | 20.10+ | `docker --version` |
| **Docker Compose** | 2.0+ | `docker compose version` |

> 💡 **Recommended IDE:** IntelliJ IDEA (with Spring Boot plugin)

---

## 🎯 Quick Start

### 📘 **Detailed Guide Available**

For complete step-by-step instructions with Docker commands, topic management, and troubleshooting, see:  
👉 **[detailed Quick Start Guide](Quick_Start_Guide.md) **

### ⚡ **Get Running in 5 Minutes**

```bash
# 1. Start Kafka infrastructure
docker compose up -d

# 2. Wait for health checks (30 seconds)
docker compose ps

# 3. Build both modules
mvn clean install

# 4. Run Producer (Terminal 1)
cd producer-app && mvn spring-boot:run

# 5. Run Consumer (Terminal 2)
cd consumer-app && mvn spring-boot:run
```

### 📊 **Service Endpoints**

| Service | Address | Purpose |
|---------|---------|---------|
| **Producer API** | http://localhost:8081 | Send messages via REST |
| **Conduktor UI** | http://localhost:8080 | Visual Kafka monitoring |
| **Kafka Broker** | `localhost:9092` | External access (from host) |
| **Kafka Internal** | `kafka-service-app:19092` | Container-to-container |

### 🔥 **Send Your First Message**

```bash
curl -X POST http://localhost:8081/api/messages \
  -H "Content-Type: application/json" \
  -d '{
    "message": "Hello Kafka!",
    "sender": "QuickStart",
    "priority": "HIGH"
  }'
```

**Expected Output:**
```json
{
  "messageId": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "status": "SENT",
  "timestamp": "2025-11-17T10:30:45.123"
}
```

### ✅ **Verify Everything Works**

| Check | Command/URL | Expected Result |
|-------|-------------|-----------------|
| Docker containers | `docker ps` | 3 containers running |
| Producer health | `curl http://localhost:8081/api/messages/test` | `"Producer is running"` |
| Consumer logs | Terminal 2 output | `"Message processed successfully"` |
| Conduktor UI | http://localhost:8080 | Topics visible |

### 🛑 **Stop Services**

```bash
# Stop containers (preserve data)
docker compose stop

# Stop and remove containers
docker compose down

# Complete reset (delete all data)
docker compose down -v
```

### 📖 **What's Next?**

- 📘 Read the [detailed Quick Start Guide](Quick_Start_Guide.md) for Docker commands and topic management
- 🧪 Try the [testing scenarios](#-testing-scenarios) section
- 🎓 Complete the [learning exercises](#-learning-exercises)

---

## 🧪 Testing Scenarios

### Scenario 1: High-Priority Messages

```bash
curl -X POST http://localhost:8081/api/messages \
  -H "Content-Type: application/json" \
  -d '{"message": "URGENT: System alert", "sender": "Monitor", "priority": "HIGH"}'
```

Consumer will log special handling for high-priority messages.


### Scenario 2: Multiple Messages

Send 10 messages quickly:

```bash
for i in {1..10}; do
  curl -X POST http://localhost:8081/api/messages \
    -H "Content-Type: application/json" \
    -d "{\"message\": \"Message $i\", \"sender\": \"Bot\", \"priority\": \"NORMAL\"}"
done
```

Watch consumer process them in parallel (concurrency: 3).


### Scenario 3: Consumer Group Behavior

1. Stop the consumer (`Ctrl+C`)
2. Send messages using producer
3. Restart consumer
4. Messages are consumed from last committed offset (`earliest` strategy)

---

## 🎓 Learning Exercises

### Exercise 1: Add a New Topic
1. Create topic manually:
```bash
docker exec -it kafka-service-app bash
kafka-topics --create --topic orders-topic --bootstrap-server localhost:19092 --partitions 3 --replication-factor 1
```
2. Update producer/consumer to use this topic
3. Test with multiple partitions


### Exercise 2: Multiple Consumer Instances
1. Run consumer in two terminals:
```bash
# Terminal 1
cd consumer-app && mvn spring-boot:run

# Terminal 2 (change server port)
cd consumer-app && mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8083
```
2. Send messages
3. Observe load balancing across consumers in the same group


### Exercise 3: Dead Letter Queue (DLQ)
1. Modify consumer to throw exceptions for certain messages
2. Create a `t-topic-dlq` topic
3. Send failed messages to DLQ after N retries
4. Monitor both topics in Conduktor


### Exercise 4: Custom Serialization
1. Add Avro or Protobuf serialization
2. Update producer/consumer configs
3. Test schema evolution

---

## 🧹 Cleanup & Reset

### Stop All Services
```bash
docker compose down
```

### Reset Everything (delete all data)
```bash
docker compose down -v
```
This removes all topics, offsets, and PostgreSQL data.

---

## 🔧 Configuration Details

### Kafka Connection

- **External (from Windows host):** `localhost:9092`
- **Internal (container-to-container):** `kafka-service-app:19092`

### Application Ports

- **Producer API:** 8081
- **Consumer:** 8082
- **Conduktor UI:** 8080

### Topic Configuration

- **Name:** `t-topic`
- **Partitions:** 1 (auto-created by Kafka)
- **Replication Factor:** 1

---

## 🐛 Troubleshooting

### Kafka Not Starting
```bash
docker logs kafka-service-app
```
Check for port conflicts or cluster ID issues.

### Consumer Not Receiving Messages
1. Check consumer logs for deserialization errors
2. Verify topic exists:
```bash
docker exec -it kafka-service-app kafka-topics --list --bootstrap-server localhost:19092
```
3. Check consumer group status:
```bash
docker exec -it kafka-service-app kafka-consumer-groups --bootstrap-server localhost:19092 --group message-consumer-group --describe
```

### Connection Refused
- Ensure Docker containers are running: `docker ps`
- Wait 30s after `docker compose up` for full initialization
- Check health checks: `docker compose ps`

---

## 📚 Key Concepts Demonstrated

✅ **Producer API** - REST endpoint to publish messages  
✅ **Consumer Groups** - Parallel message processing  
✅ **Manual Offset Management** - Full control over commits  
✅ **Idempotency** - Duplicate message detection with Caffeine  
✅ **JSON Serialization** - Type-safe message passing  
✅ **Priority Handling** - Business logic based on message attributes  
✅ **Error Handling** - Graceful failure and retry strategies  
✅ **Monitoring** - Real-time visualization with Conduktor  

---

## 🚀 Next Steps could be implemented to advance this app

- Implement **transactional producers**
- Add **Kafka Streams** for real-time processing
- Integrate **Spring Cloud Stream**
- Set up **monitoring with Prometheus + Grafana**
- Implement **schema registry** for Avro
- Add **security** (SSL/SASL)

---

## 📝 Notes

- **KRaft Mode:** No Zookeeper dependency (modern Kafka)
- **Auto-create Topics:** Enabled for convenience
- **Manual Commits:** Consumer uses `AckMode.MANUAL` for reliability
- **Health Checks:** Ensures services start in correct order
- **Localhost Binding:** Works on Windows, macOS, Linux

---
