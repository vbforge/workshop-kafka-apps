# SCENARIO 6: Real-World E-Commerce Order Processing System

## 🎯 Overview

This scenario demonstrates a **production-ready event-driven microservices architecture** using Kafka.
Five independent services process orders in parallel, each with different responsibilities and commit strategies.

**This scenario demonstrates:**
- JSON message serialization/deserialization
- Multi-stage order processing pipeline
- Multiple independent microservices (consumer groups)
- Real-world event-driven architecture

**Use Case: Complete e-commerce order workflow**

**Pipeline:**
1. OrderService → Produces orders
2. PaymentService → Processes payments
3. InventoryService → Updates stock
4. NotificationService → Sends confirmations
5. AnalyticsService → Tracks metrics

### Architecture Diagram

```
                    ┌─────────────────┐
                    │  OrderService   │
                    │   (Producer)    │
                    └────────┬────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │  Kafka Topic    │
                    │ topic-ecommerce-│
                    │    orders       │
                    │ (3 partitions)  │
                    └────────┬────────┘
                             │
            ┌────────────────┼────────────────┐
            │                │                │
            ▼                ▼                ▼
    ┌───────────────┐ ┌────────────────┐ ┌────────────────┐
    │PaymentService │ │InventoryService│ │Notification    │
    │  (Group A)    │ │   (Group B)    │ │Service (GroupC)│
    │Manual Commit  │ │Manual Commit   │ │Auto-Commit     │
    │  5% fail rate │ │Stock validation│ │Email notif     │
    └───────────────┘ └────────────────┘ └────────────────┘
            │                │                │
            └────────────────┼────────────────┘
                             │
                             ▼
                    ┌────────────────┐
                    │AnalyticsService│
                    │   (Group D)    │
                    │ Auto-Commit    │
                    │ Real-time stats│
                    └────────────────┘
```

### Key Concepts Demonstrated

| Concept | How It's Used |
|---------|--------------|
| **Broadcast Pattern** | Each service has its own consumer group → all receive every order |
| **Manual Commit** | Payment & Inventory services (critical data) |
| **Auto-Commit** | Notification & Analytics services (non-critical) |
| **Message Keys** | Orders keyed by `userId` → per-user ordering guaranteed |
| **Error Handling** | Payment failures & stock shortages trigger redelivery |
| **Independent Scaling** | Each service can run multiple instances |
| **Fault Isolation** | One service failing doesn't affect others |

### REAL-WORLD INSIGHTS:
- Each service is a separate consumer group (independent consumption)
- Services can scale independently
- Failed services don't block others
- Messages are replayed for new services
- Perfect for microservices architecture

---

## 📋 Prerequisites

- ✅ Docker Desktop installed and running
- ✅ Project built:  `> mvn clean compile`
- ✅ Ports available: 9092 (Kafka)

---

## 🚀 Step-by-Step Execution Guide

[Working PowerShell Commands](commands_to_run_terminals.md)

### STEP 1: Start Kafka Infrastructure

```bash
# From project root directory
docker-compose up -d

# Verify Kafka is healthy
docker-compose ps

# Expected output:
# NAME                       IMAGE                        STATUS
# kafka-learning-broker      confluentinc/cp-kafka:8.0.0  Up (healthy)

```

### STEP 2: Create the Topic (with 3 partitions)

```bash
# Create topic with 3 partitions for parallel processing
docker exec -it kafka-learning-broker kafka-topics \
  --create \
  --topic topic-ecommerce-orders \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:19092
  
>  docker exec -it kafka-learning-broker kafka-topics --create --topic topic-ecommerce-orders --partitions 3 --replication-factor 1 --bootstrap-server localhost:19092

# Verify topic creation
docker exec -it kafka-learning-broker kafka-topics \
  --describe \
  --topic topic-ecommerce-orders \
  --bootstrap-server localhost:19092

> docker exec -it kafka-learning-broker kafka-topics --describe --topic topic-ecommerce-orders --bootstrap-server localhost:19092
# Expected output shows 3 partitions (0,1,2)
```

### STEP 3: Start All Consumer Services (4 terminals)

Open **5 separate terminals** (or use IntelliJ's multiple run configurations).

#### Terminal 1: Payment Service
```bash
cd C:\Users\admin\Desktop\LABS\GITHUB\workshop-kafka-apps\kafka-02-learning-scenarios

mvn exec:java -Dexec.mainClass="com.vbforge.scenario_06_e_commerce_orders_app.PaymentService"

> in some cases could be this command: mvn exec:java "-Dexec.mainClass=com.vbforge.scenario_06_e_commerce_orders_app.PaymentService" 
```

**Expected output:**
```
======== Payment Service ========
=== Kafka Configuration ===
Bootstrap Servers: localhost:9092
Subscribed to: topic-ecommerce-orders | group: payment-service-group
Ctrl+C to stop
```

#### Terminal 2: Inventory Service
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_06_e_commerce_orders_app.InventoryService"

> in some cases could be this command: mvn exec:java "-Dexec.mainClass=com.vbforge.scenario_06_e_commerce_orders_app.InventoryService" 

```

**Expected output:**
```
======== Inventory Service ========
--- Current Inventory ---
laptop: 50 units
phone: 100 units
tablet: 75 units
headphones: 200 units
monitor: 30 units

Subscribed to: topic-ecommerce-orders | group: inventory-service-group
```

#### Terminal 3: Notification Service
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_06_e_commerce_orders_app.NotificationService"

> in some cases could be this command: mvn exec:java "-Dexec.mainClass=com.vbforge.scenario_06_e_commerce_orders_app.NotificationService" 
```

**Expected output:**
```
======== Notification Service ========
Subscribed to: topic-ecommerce-orders | group: notification-service-group
Ctrl+C to stop
```

#### Terminal 4: Analytics Service
```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_06_e_commerce_orders_app.AnalyticsService"

> in some cases could be this command: mvn exec:java "-Dexec.mainClass=com.vbforge.scenario_06_e_commerce_orders_app.AnalyticsService" 
```

**Expected output:**
```
======== Analytics Service ========
Subscribed to: topic-ecommerce-orders | group: analytics-service-group
Ctrl+C to stop
```

### STEP 4: Start Producer (Terminal 5)

```bash
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_06_e_commerce_orders_app.OrderService"

> in some cases could be this command: mvn exec:java "-Dexec.mainClass=com.vbforge.scenario_06_e_commerce_orders_app.OrderService" 
```

**Expected output:**
```
=== Order Service (Producer) ===
Sending 15 orders to topic: topic-ecommerce-orders
=== Kafka Configuration ===
Bootstrap Servers: localhost:9092

Order placed | id: ORD-00001 | user: user-003 | partition: 1 | offset: 0
Order placed | id: ORD-00002 | user: user-001 | partition: 0 | offset: 0
Order placed | id: ORD-00003 | user: user-002 | partition: 2 | offset: 0
...
All 15 orders submitted.
```

### STEP 5: Observe Real-Time Processing

Watch all 5 terminals simultaneously to see the order flow through the system:

#### Payment Service Output:
```
Payment OK | id: ORD-00001 | amount: $456.78 | user: user-003 | total processed: 1
Payment OK | id: ORD-00002 | amount: $234.56 | user: user-001 | total processed: 2
Payment FAILED | key: user-002 | reason: Gateway timeout for order: ORD-00003 — batch not committed
```

#### Inventory Service Output:
```
Inventory updated | order: ORD-00001 | product: laptop | qty: -2 | remaining: 48
Inventory updated | order: ORD-00002 | product: phone | qty: -1 | remaining: 99
--- Current Inventory ---
laptop: 48 units
phone: 99 units
...
```

#### Notification Service Output:
```
Notification sent | user: user-003 | order: ORD-00001 | product: laptop | amount: $456.78
Notification sent | user: user-001 | order: ORD-00002 | product: phone | amount: $234.56
```

#### Analytics Service Output:
```
╔═══════════════════════════════════════╗
║       ANALYTICS DASHBOARD             ║
╚═══════════════════════════════════════╝
📊 Total Orders: 2
💰 Total Revenue: $691.34
📈 Average Order Value: $345.67

Top Products:
  • laptop: 2 units
  • phone: 1 units

Top Customers:
  • user-003: 1 orders
  • user-001: 1 orders
═══════════════════════════════════════
```

---

## 🎓 What You'll Learn

### 1. Broadcast Pattern (Important!)
```
One Topic → Multiple Consumer Groups → Each service gets ALL messages

Topic: topic-ecommerce-orders
├── payment-service-group    → Receives all orders
├── inventory-service-group  → Receives all orders
├── notification-service-group → Receives all orders
└── analytics-service-group  → Receives all orders
```

**Why this matters:** Different services need the same data for different purposes. Each service maintains its own offset.

### 2. Commit Strategies Comparison

| Service      | Commit Strategy       | Why?                               | Failure Behavior               |
|--------------|-----------------------|------------------------------------|--------------------------------|
| Payment      | Manual (`commitSync`) | Critical - can't lose payment data | Uncommitted orders redelivered |
| Inventory    | Manual (`commitSync`) | Must maintain stock accuracy       | Failed orders retried          |
| Notification | Auto                  | Non-critical, user can retry       | May miss occasional email      |
| Analytics    | Auto                  | Idempotent calculations            | Duplicates acceptable          |

### 3. Message Keys for Ordering

```java
// OrderService.java - Key = userId
ProducerRecord<String, String> record = new ProducerRecord<>(
    TOPIC_ECOMMERCE_ORDERS, 
    order.getUserId(),  // KEY = userId
    orderJson           // VALUE = order data
);

// Result: All orders from user-001 go to same partition
// → User sees their orders in correct sequence
```

### 4. Error Handling Patterns

**Payment Service (5% failure rate):**
```java
if (paymentFails) {
    // Exception thrown → batch NOT committed
    // Failed order will be redelivered on next poll
    throw new PaymentException("Gateway timeout");
}
```

**Inventory Service (stock validation):**
```java
if (currentStock < order.getQuantity()) {
    // Insufficient stock → batch NOT committed
    throw new InsufficientStockException("Out of stock");
}
```

---

## 🧪 Experiments to Try

### Experiment 1: Service Failure Isolation

1. **Stop Payment Service** (Ctrl+C in Terminal 1)
2. Continue producing orders
3. **Observe:** Other services continue processing normally
4. **Restart Payment Service**
5. **Observe:** It catches up from last committed offset

### Experiment 2: View Consumer Group Offsets

```bash
# Check payment service progress
docker exec -it kafka-learning-broker kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --group payment-service-group \
  --describe

# Check inventory service progress
docker exec -it kafka-learning-broker kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --group inventory-service-group \
  --describe
  
# Check notification service progress
docker exec -it kafka-learning-broker kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --group notification-service-group \
  --describe 
  
# Check analytics service progress
docker exec -it kafka-learning-broker kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --group analytics-service-group \
  --describe
  
```

**Expected output:**
```
GROUP                TOPIC                 PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
payment-service-group topic-ecommerce-orders 0         5               5               0
payment-service-group topic-ecommerce-orders 1         7               7               0
payment-service-group topic-ecommerce-orders 2         3               3               0
```

### Experiment 3: Simulate Payment Failures

Run OrderService multiple times and watch PaymentService retry failed payments:

```bash
# First run - some payments fail
OrderService → 15 orders
PaymentService shows: 12 OK, 3 FAILED

# Second run (don't restart PaymentService)
OrderService → 15 more orders
PaymentService processes new orders + retries failed ones
```

### Experiment 4: Scale a Service

Start **two instances** of Notification Service:

```bash
# Terminal 3a - First instance
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_06_e_commerce_orders_app.NotificationService"

# Terminal 3b - Second instance (different terminal)
mvn exec:java -Dexec.mainClass="com.vbforge.scenario_06_e_commerce_orders_app.NotificationService"
```

**Observe:** Both instances share the workload (Kafka partitions distribute between them).

---

## 🛠️ Troubleshooting

### Issue: "Topic does not exist"

**Solution:**
```bash
# Create topic manually
docker exec -it kafka-learning-broker kafka-topics \
  --create \
  --topic topic-ecommerce-orders \
  --partitions 3 \
  --bootstrap-server localhost:19092
```

### Issue: Consumers not receiving messages

**Check consumer groups:**
```bash
# List all consumer groups
docker exec -it kafka-learning-broker kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --list

# Should show:
# payment-service-group
# inventory-service-group
# notification-service-group
# analytics-service-group
```

### Issue: Payment failures always occurring

**Check if 5% failure rate is too high:**
- Modify `PaymentService.java:95` - change `0.05` to `0.01` (1% failure)

### Issue: Port conflicts

**If Kafka port 9092 is in use:**
```bash
# Find process using port 9092 (Windows)
netstat -ano | findstr :9092

# Stop local Kafka if running
# Or change port in docker-compose.yml
```

---

## 📈 Performance Characteristics

| Metric | Value |
|--------|-------|
| Order production rate | 1 order/second |
| Payment processing time | ~500ms |
| Inventory update time | ~200ms |
| Notification time | ~100ms |
| Analytics time | ~50ms |
| **End-to-end latency** | ~850ms |

### Partition Distribution

With 3 partitions and keyed by `userId`:
- Same user → Same partition → Ordered processing
- Different users → Different partitions → Parallel processing

---

## 🧹 Cleanup

```bash
# Stop all services (Ctrl+C in each terminal)

# Stop Kafka and remove containers
docker-compose down

# Remove volumes (reset all data)
docker-compose down -v

# Delete topic (if needed)
docker exec -it kafka-learning-broker kafka-topics \
  --delete \
  --topic topic-ecommerce-orders \
  --bootstrap-server localhost:19092
```

---

## 🎯 Success Criteria

You've successfully completed this scenario if:

- ✅ All 5 services start without errors
- ✅ Producer sends 15 orders successfully
- ✅ Payment Service processes ~95% successfully (5% fail)
- ✅ Inventory Service updates stock correctly
- ✅ Notification Service logs each order
- ✅ Analytics Service shows dashboard updates
- ✅ You can stop/restart a service without data loss

---

## 📚 Real-World Production Considerations

| Consideration   | Our Demo                     | Production                   |
|-----------------|------------------------------|------------------------------|
| Consumer groups | One per service              | One per service + monitoring |
| Offset commit   | Manual for critical services | Manual + async commit        |
| Error handling  | Retry on same poll           | DLQ + alerting               |
| Idempotency     | Not implemented              | Required for all services    |
| Monitoring      | Conduktor UI                 | Prometheus + Grafana         |
| Scalability     | Manual                       | Horizontal Pod Autoscaler    |

---

## 💡 Pro Tips

1. **Run OrderService multiple times** without restarting consumers to see offset progression
2. **Use Conduktor to watch** consumer lag in real-time (if use UI Conductor)
3. **Experiment with partition count** - recreate topic with 1 vs 6 partitions
4. **Test failure scenarios** - kill Kafka, restart, watch recovery
5. **Add logging** to track end-to-end latency per order

---
