## 🎯 How to Run Everything Step by Step (Working PowerShell Commands)

### Step 1: Start Kafka (if not already running)
```powershell
docker-compose up -d
```

### Step 2: Create the topic (if not exists)
```powershell
docker exec -it kafka-learning-broker kafka-topics --create --topic topic-ecommerce-orders --partitions 3 --bootstrap-server localhost:19092
```

### Step 3: Open 5 PowerShell terminals and run:

**Terminal 1 - Payment Service:**
```powershell
mvn exec:java "-Dexec.mainClass=com.vbforge.scenario_06_e_commerce_orders_app.PaymentService"
```

**Terminal 2 - Inventory Service:**
```powershell
mvn exec:java "-Dexec.mainClass=com.vbforge.scenario_06_e_commerce_orders_app.InventoryService"
```

**Terminal 3 - Notification Service:**
```powershell
mvn exec:java "-Dexec.mainClass=com.vbforge.scenario_06_e_commerce_orders_app.NotificationService"
```

**Terminal 4 - Analytics Service:**
```powershell
mvn exec:java "-Dexec.mainClass=com.vbforge.scenario_06_e_commerce_orders_app.AnalyticsService"
```

**Terminal 5 - Order Service (Producer):**
**(Run after all consumers are ready)**
```powershell
mvn exec:java "-Dexec.mainClass=com.vbforge.scenario_06_e_commerce_orders_app.OrderService"
```


