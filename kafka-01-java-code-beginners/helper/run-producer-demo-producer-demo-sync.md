## We are working here with ProducerDemo and ProducerDemoSync

### 1. Start Kafka in Docker
```
cd your-project-folder
docker-compose up -d
```

### 2. Wait for Kafka to be ready (30 seconds)
```
docker-compose logs -f kafka | grep "started"

Press Ctrl+C when you see it's ready
```

### 3. Verify Kafka is running
```
docker ps | grep kafka-java-broker
```

### 4. Create the topic (optional - auto-creation is enabled, but explicit is better)
```
docker exec -it kafka-java-broker kafka-topics \
--bootstrap-server localhost:9092 \
--create \
--topic demo_topic_example \
--partitions 3 \
--replication-factor 1
```


### 5. Run your Java producer
#### 1) In IntelliJ: Run `ProducerDemo.main()` or `ProducerDemoSync.main()`
#### 2) Or via command line:
```
javac -cp ".:path/to/kafka-clients.jar" com/vbforge/producer/ProducerDemo.java
java -cp ".:path/to/kafka-clients.jar" com.vbforge.producer.ProducerDemo
```

### 6. (Optional) Verify messages via console consumer
```
docker exec -it kafka-java-broker kafka-console-consumer \
--bootstrap-server localhost:9092 \
--topic demo_topic_example \
--from-beginning
```

---

## Verification Checklist

| Test | Command | Expected Result |
|------|---------|-----------------|
| Kafka running | `docker ps` | Container `kafka-java-broker` is up |
| Topic exists | `docker exec -it kafka-java-broker kafka-topics --bootstrap-server localhost:9092 --list` | `demo_topic_example` in list |
| Message sent | Check producer logs | "Message sent successfully" |
| Message consumed | Console consumer | Shows your message |

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `Connection refused: localhost:9092` | Kafka not started → `docker-compose up -d` |
| `Timed out waiting for connection` | Wait longer for Kafka to start (30+ seconds) |
| `UNKNOWN_TOPIC_OR_PARTITION` | Topic auto-creation may be slow → create explicitly |
| `Producer send failed` | Check `bootstrap.servers: localhost:9092` |

---






