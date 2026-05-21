# Project 1: Simple Order Notification System (Easy)

## 🎯 Project Overview
Build a basic order processing system where orders are published to Kafka and consumed to send notifications. This project introduces core Kafka concepts: producers, consumers, topics, and basic Spring Boot integration.

## 💡 What You'll Learn
- Setting up Spring Boot with Kafka
- Creating Kafka producers and consumers
- Working with simple message serialization (String/JSON)
- Basic error handling
- Configuring Kafka with KRaft (no Zookeeper)

## 🏗️ Architecture
```
User/API → Order Service (Producer) → Kafka Topic → Notification Service (Consumer) → Console Output
```

## 📋 Prerequisites
- Kafka 4.0.1 (KRaft mode) running locally
- Java 17
- Spring Boot 3.x
- MySQL (for storing order data)
- Maven or Gradle

## 🚀 Step-by-Step Implementation

### Step 1: Project Setup
**Goal**: Create Spring Boot application structure

1. Create a new Spring Boot project with dependencies:
   - Spring Web
   - Spring for Apache Kafka
   - Spring Data JPA
   - MySQL Driver
   - Lombok (optional, but recommended)
   - Validation

2. Create two modules/packages:
   - `order-service` (Producer)
   - `notification-service` (Consumer)

3. Configure `application.yml` for both services:
   - Database connection (MySQL)
   - Kafka bootstrap servers (localhost:9092)
   - Server ports (8080 for order-service, 8081 for notification-service)

### Step 2: Create Domain Models
**Goal**: Define the data structures

1. Create `Order` entity:
   - id (Long)
   - customerName (String)
   - productName (String)
   - quantity (Integer)
   - totalPrice (BigDecimal)
   - orderDate (LocalDateTime)
   - status (Enum: CREATED, PROCESSING, COMPLETED)

2. Create `OrderDTO` for Kafka messages:
   - Same fields as Order entity
   - Will be serialized to JSON

3. Create `OrderRepository` (JPA repository)

### Step 3: Set Up Kafka Topic
**Goal**: Create the Kafka topic for order events

1. Open terminal and create topic:
   ```bash
   kafka-topics.sh --create \
     --bootstrap-server localhost:9092 \
     --topic order-events \
     --partitions 3 \
     --replication-factor 1
   ```

2. Verify topic creation:
   ```bash
   kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

### Step 4: Implement Order Service (Producer)
**Goal**: Create REST API that publishes orders to Kafka

1. Create `KafkaProducerConfig`:
   - Configure JsonSerializer
   - Set bootstrap servers
   - Configure producer properties (acks, retries, etc.)

2. Create `OrderController`:
   - POST endpoint `/api/orders` to create new orders
   - Accept order details in request body
   - Validate input data

3. Create `OrderService`:
   - Save order to MySQL database
   - Send order to Kafka using `KafkaTemplate`
   - Handle exceptions and return appropriate responses

4. Create `KafkaProducerService`:
   - Method to send OrderDTO to "order-events" topic
   - Add logging for successful/failed sends
   - Implement callback to handle send results

### Step 5: Implement Notification Service (Consumer)
**Goal**: Consume orders from Kafka and process them

1. Create `KafkaConsumerConfig`:
   - Configure JsonDeserializer
   - Set consumer group id: "notification-group"
   - Configure consumer properties (auto-offset-reset, etc.)

2. Create `NotificationConsumer`:
   - Listen to "order-events" topic
   - Use `@KafkaListener` annotation
   - Deserialize OrderDTO

3. Create `NotificationService`:
   - Process received order
   - Log notification details to console
   - Simulate sending email/SMS (just print message)
   - Format output nicely with order details

### Step 6: Add Error Handling
**Goal**: Handle failures gracefully

1. In Producer:
   - Add try-catch blocks
   - Handle Kafka send failures
   - Return appropriate HTTP status codes

2. In Consumer:
   - Add `@KafkaListener` error handler
   - Log failed messages
   - Implement simple retry logic (manual for now)

### Step 7: Testing
**Goal**: Verify the complete flow

1. Start Kafka (KRaft mode)
2. Start MySQL database
3. Run notification-service first
4. Run order-service
5. Send POST request to create order:
   ```json
   {
     "customerName": "John Doe",
     "productName": "Laptop",
     "quantity": 1,
     "totalPrice": 999.99
   }
   ```
6. Verify:
   - Order saved in MySQL
   - Message appears in Kafka topic
   - Notification consumer logs the message
   - Check console output for notification

### Step 8: Add Monitoring
**Goal**: View what's happening in Kafka

1. Use Kafka console consumer to verify messages:
   ```bash
   kafka-console-consumer.sh \
     --bootstrap-server localhost:9092 \
     --topic order-events \
     --from-beginning
   ```

2. Add logging at each step:
   - Before sending to Kafka
   - After successful send
   - When consumer receives message

## 🧪 Testing Scenarios

1. **Happy Path**: Create order → Verify in DB → Check consumer logs
2. **Multiple Orders**: Send 10 orders quickly → Verify all processed
3. **Consumer Restart**: Stop consumer → Send orders → Start consumer → Verify processing
4. **Invalid Data**: Send invalid order → Verify error handling

## 📝 Deliverables

- Working Spring Boot applications (producer & consumer)
- Orders stored in MySQL
- Messages flowing through Kafka
- Console output showing processed notifications
- Clean code with proper error handling
- README with setup and run instructions

## 🎓 Key Concepts Covered

- Kafka Producer API
- Kafka Consumer API
- Consumer Groups
- Topics and Partitions
- JSON Serialization/Deserialization
- Spring Boot Kafka Integration
- Basic error handling

## 🔜 Next Steps (for Medium Project)

- Add multiple consumers
- Implement Dead Letter Queue (DLQ)
- Add message filtering
- Implement proper retry mechanisms
- Add monitoring and metrics

## 💻 Useful Kafka Commands

```bash
# Start Kafka (KRaft mode)
kafka-server-start.sh config/kraft/server.properties

# List topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# Describe topic
kafka-topics.sh --describe --topic order-events --bootstrap-server localhost:9092

# View consumer groups
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Check consumer lag
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group notification-group --describe
```

## ⚠️ Common Issues & Solutions

1. **Connection refused**: Check Kafka is running on port 9092
2. **Serialization errors**: Ensure matching serializers/deserializers
3. **Messages not consumed**: Verify consumer group and topic name
4. **Port conflicts**: Check if ports 8080/8081 are available

Good luck with your first Kafka project! 🚀