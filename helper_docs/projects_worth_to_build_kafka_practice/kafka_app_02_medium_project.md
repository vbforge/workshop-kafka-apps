# Project 2: E-commerce Order Processing Pipeline (Medium)

## 🎯 Project Overview
Build a realistic e-commerce order processing system with multiple microservices communicating through Kafka. This project introduces advanced Kafka patterns: multiple consumers, message routing, Dead Letter Queue (DLQ), Avro serialization, and transaction handling.

## 💡 What You'll Learn
- Multiple Kafka topics and message routing
- Consumer groups and parallel processing
- Dead Letter Queue (DLQ) pattern
- Avro schema for message serialization
- Kafka transactions
- Event-driven microservices architecture
- Spring Boot Actuator for monitoring

## 🏗️ Architecture
```
Order API → Kafka (order-created) → [Inventory Service, Payment Service, Email Service]
                                              ↓                    ↓
                                    Kafka (inventory-updated)  Kafka (payment-processed)
                                              ↓                    ↓
                                           Order Fulfillment Service → Kafka (order-completed)
                                                     ↓
                                              Shipping Service
                                                     
Failed messages → Dead Letter Queue (DLQ) → Manual Review/Retry
```

## 📋 Prerequisites
- Kafka 4.0.1 (KRaft mode)
- Java 17
- Spring Boot 3.x
- MySQL (for each service)
- Confluent Schema Registry (optional but recommended)
- Maven

## 🚀 Step-by-Step Implementation

### Step 1: Project Architecture Setup
**Goal**: Create multi-module Spring Boot project

1. Create parent Maven project with modules:
   - `order-service` (Producer/Consumer)
   - `inventory-service` (Consumer/Producer)
   - `payment-service` (Consumer/Producer)
   - `fulfillment-service` (Consumer/Producer)
   - `email-service` (Consumer)
   - `common-models` (Shared DTOs and configs)

2. Configure `application.yml` for each service:
   - Unique database per service (microservices pattern)
   - Different server ports (8080-8085)
   - Kafka configurations
   - Actuator endpoints for health checks

3. Set up parent `pom.xml` with common dependencies:
   - Spring Boot Kafka
   - Spring Data JPA
   - MySQL Driver
   - Spring Boot Actuator
   - Lombok
   - Spring Validation

### Step 2: Define Kafka Topics
**Goal**: Plan and create all necessary topics

1. Create topics with appropriate partitions:
   ```bash
   # Main event topics
   kafka-topics.sh --create --topic order-created --partitions 3 --replication-factor 1
   kafka-topics.sh --create --topic inventory-reserved --partitions 3 --replication-factor 1
   kafka-topics.sh --create --topic payment-processed --partitions 3 --replication-factor 1
   kafka-topics.sh --create --topic order-fulfilled --partitions 3 --replication-factor 1
   kafka-topics.sh --create --topic order-completed --partitions 3 --replication-factor 1
   
   # Dead Letter Queue topics
   kafka-topics.sh --create --topic order-dlq --partitions 1 --replication-factor 1
   kafka-topics.sh --create --topic inventory-dlq --partitions 1 --replication-factor 1
   kafka-topics.sh --create --topic payment-dlq --partitions 1 --replication-factor 1
   ```

2. Document topic naming convention and purpose in README

### Step 3: Create Domain Models (common-models module)
**Goal**: Define shared data structures

1. Create event models:
   - `OrderCreatedEvent` (orderId, customerId, items, totalAmount, timestamp)
   - `InventoryReservedEvent` (orderId, items, reservationId, status)
   - `PaymentProcessedEvent` (orderId, paymentId, amount, status, method)
   - `OrderFulfilledEvent` (orderId, fulfillmentId, status)
   - `OrderCompletedEvent` (orderId, completionDate, trackingNumber)

2. Create entity models for each service database:
   - `Order` (in order-service)
   - `Inventory` (in inventory-service)
   - `Payment` (in payment-service)
   - `Fulfillment` (in fulfillment-service)

3. Add status enums:
   - `OrderStatus`: CREATED, INVENTORY_RESERVED, PAYMENT_COMPLETED, FULFILLED, SHIPPED, COMPLETED, FAILED
   - `PaymentStatus`: PENDING, COMPLETED, FAILED, REFUNDED
   - `InventoryStatus`: AVAILABLE, RESERVED, OUT_OF_STOCK

### Step 4: Implement Order Service
**Goal**: Create orders and orchestrate the workflow

1. Create REST endpoints:
   - POST `/api/orders` - Create new order
   - GET `/api/orders/{id}` - Get order status
   - GET `/api/orders` - List all orders
   - PUT `/api/orders/{id}/cancel` - Cancel order

2. Implement `OrderProducerService`:
   - Publish `OrderCreatedEvent` to "order-created" topic
   - Use Kafka transactions for atomicity
   - Save order to database and send to Kafka in same transaction

3. Implement consumers for status updates:
   - Listen to "inventory-reserved" topic
   - Listen to "payment-processed" topic
   - Listen to "order-completed" topic
   - Update order status in database accordingly

4. Add correlation ID for tracking events across services

### Step 5: Implement Inventory Service
**Goal**: Check and reserve inventory

1. Create `InventoryConsumer`:
   - Listen to "order-created" topic
   - Consumer group: "inventory-group"

2. Implement `InventoryService`:
   - Check if items are available
   - Reserve inventory if available
   - Update inventory count in database
   - Calculate reserved quantity

3. Implement `InventoryProducer`:
   - Publish `InventoryReservedEvent` if successful
   - Include success/failure status
   - If failed, specify reason (OUT_OF_STOCK)

4. Add business logic:
   - Concurrent inventory updates handling
   - Pessimistic locking or optimistic locking
   - Inventory release on order cancellation

### Step 6: Implement Payment Service
**Goal**: Process payments

1. Create `PaymentConsumer`:
   - Listen to "inventory-reserved" topic
   - Only process if inventory was successfully reserved
   - Consumer group: "payment-group"

2. Implement `PaymentService`:
   - Simulate payment processing (random delay)
   - 90% success rate (simulate failures)
   - Store payment records in database
   - Generate unique payment ID

3. Implement `PaymentProducer`:
   - Publish `PaymentProcessedEvent`
   - Include payment method, transaction ID
   - Send to "payment-processed" topic

4. Add retry mechanism:
   - Retry failed payments up to 3 times
   - Exponential backoff between retries

### Step 7: Implement Fulfillment Service
**Goal**: Fulfill orders after payment

1. Create `FulfillmentConsumer`:
   - Listen to "payment-processed" topic
   - Only process successful payments
   - Consumer group: "fulfillment-group"

2. Implement `FulfillmentService`:
   - Create fulfillment record
   - Generate tracking number
   - Simulate picking and packing process

3. Implement `FulfillmentProducer`:
   - Publish `OrderFulfilledEvent`
   - Include tracking information
   - Send to "order-fulfilled" topic

### Step 8: Implement Email Service
**Goal**: Send notifications to customers

1. Create `EmailConsumer` with multiple listeners:
   - Listen to "order-created" → Send order confirmation
   - Listen to "payment-processed" → Send payment receipt
   - Listen to "order-fulfilled" → Send shipping notification
   - Consumer group: "email-group"

2. Implement `EmailService`:
   - Simulate sending emails (log to console)
   - Format nice email templates in logs
   - Include order details, tracking info

3. No producer needed (end of the chain)

### Step 9: Implement Dead Letter Queue (DLQ) Pattern
**Goal**: Handle failed messages gracefully

1. Create `KafkaErrorHandler` configuration:
   - Implement custom error handler
   - After 3 retry attempts, send to DLQ
   - Include error details and original message

2. For each consumer, configure error handling:
   - Set max retry attempts
   - Define retry backoff policy
   - Route to appropriate DLQ topic

3. Create `DLQConsumer` (monitoring service):
   - Listen to all DLQ topics
   - Log failed messages with error details
   - Store in database for manual review
   - Provide REST API to view and retry DLQ messages

4. Implement manual retry functionality:
   - Admin endpoint to republish DLQ messages
   - Fix message data if needed
   - Track retry history

### Step 10: Add Kafka Transactions
**Goal**: Ensure exactly-once semantics

1. Configure transactional producer in Order Service:
   - Set `transactional.id`
   - Enable idempotence
   - Configure transaction timeout

2. Implement transactional publishing:
   - Begin transaction
   - Save order to database
   - Send message to Kafka
   - Commit transaction
   - Rollback on error

3. Configure consumers for read_committed isolation

### Step 11: Implement Message Filtering and Headers
**Goal**: Add metadata for routing and filtering

1. Add Kafka headers to messages:
   - `correlation-id` (trace request across services)
   - `event-type` (for filtering)
   - `timestamp`
   - `source-service`

2. Implement message filtering in consumers:
   - Filter by header values
   - Skip already processed messages (idempotency)

3. Add correlation ID propagation across all services

### Step 12: Add Monitoring and Health Checks
**Goal**: Monitor Kafka and service health

1. Configure Spring Boot Actuator:
   - Enable health endpoints
   - Add Kafka health indicators
   - Custom metrics for message processing

2. Add custom metrics:
   - Messages produced per topic
   - Messages consumed per topic
   - Processing time
   - Error rate

3. Create dashboard-ready logs:
   - Structured logging (JSON format)
   - Include correlation IDs
   - Log processing times

### Step 13: Testing Strategy
**Goal**: Comprehensive testing

1. Unit tests:
   - Test business logic in each service
   - Mock Kafka producers/consumers

2. Integration tests:
   - Use `@EmbeddedKafka` annotation
   - Test message flow between services
   - Verify database state after processing

3. End-to-end test scenarios:
   - Happy path: Order → Inventory → Payment → Fulfillment → Email
   - Inventory out of stock scenario
   - Payment failure scenario
   - Service down and recovery scenario
   - DLQ processing

4. Load testing:
   - Send 1000 orders rapidly
   - Verify all processed correctly
   - Check consumer lag

## 🧪 Testing Scenarios

1. **Complete Flow**: Create order → All services process → Order completed
2. **Inventory Failure**: Out of stock → Order failed → Customer notified
3. **Payment Failure**: Payment declined → DLQ → Retry → Success
4. **Service Restart**: Stop payment service → Create orders → Restart → Verify catch-up
5. **Concurrent Orders**: 100 simultaneous orders → Verify inventory accuracy
6. **DLQ Recovery**: Messages in DLQ → Fix and republish → Successful processing

## 📝 Deliverables

- 6 working microservices
- End-to-end order processing flow
- DLQ implementation with retry mechanism
- Kafka transactions for data consistency
- Monitoring and health checks
- Comprehensive test suite
- API documentation (Swagger/OpenAPI)
- Detailed README with architecture diagram

## 🎓 Key Concepts Covered

- Event-driven microservices
- Consumer groups and parallel processing
- Message routing and filtering
- Dead Letter Queue pattern
- Kafka transactions
- Idempotency and exactly-once semantics
- Error handling and retry mechanisms
- Correlation IDs for distributed tracing
- Service orchestration vs choreography

## 🔜 Next Steps (for Advanced Project)

- Kafka Streams for real-time analytics
- SAGA pattern for distributed transactions
- Event sourcing and CQRS
- Kafka Connect for database integration
- Schema evolution with Avro
- Multi-datacenter replication

## 💻 Useful Commands

```bash
# Monitor consumer lag for all groups
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --all-groups --describe

# Check topic message count
kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic order-created

# View messages with headers
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic order-created \
  --from-beginning \
  --property print.headers=true \
  --property print.timestamp=true
```

## ⚠️ Common Issues & Solutions

1. **Consumer lag increasing**: Scale consumers by increasing instances
2. **Transaction timeout**: Increase transaction timeout in producer config
3. **Out of memory**: Adjust Kafka and JVM heap sizes
4. **Message ordering issues**: Use partition keys based on orderId
5. **Duplicate processing**: Implement idempotency checks

This project will give you real-world experience with Kafka in microservices! 🚀