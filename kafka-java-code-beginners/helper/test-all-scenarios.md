## Test All Scenarios

### Scenario 1: Basic Producer-Consumer Flow

**Step 1: Create topic**
```
docker exec -it kafka-java-broker kafka-topics \
  --bootstrap-server localhost:9092 \
  --create \
  --topic demo_topic_example \
  --partitions 3 \
  --replication-factor 1
```

**Step 2: Start Consumer (in Terminal 1)**
```
docker exec -it kafka-java-broker kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic demo_topic_example \
  --from-beginning
```

**Step 3: Run your Java Producer**
```
# In IntelliJ/Eclipse: Run ProducerDemo.main()
# Or via Maven:
 mvn exec:java -Dexec.mainClass="com.vbforge.producer.ProducerDemo"
```

**Expected Result:** Consumer terminal shows `Hello World from Docker Kafka Producer!`

---