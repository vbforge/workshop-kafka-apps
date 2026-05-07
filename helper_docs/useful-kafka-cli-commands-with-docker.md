## Useful Kafka CLI Commands with Docker

**Important:** All commands need to be prefixed with `docker exec -it kafka-java-broker` when using Docker.

### Basic Commands

```bash
# List all topics
docker exec -it kafka-java-broker kafka-topics --bootstrap-server localhost:9092 --list

# Describe a specific topic
docker exec -it kafka-java-broker kafka-topics --bootstrap-server localhost:9092 --describe --topic demo_topic_example

# Create a topic with 3 partitions
docker exec -it kafka-java-broker kafka-topics --bootstrap-server localhost:9092 --create --topic demo_topic_example --partitions 3 --replication-factor 1

# Delete a topic
docker exec -it kafka-java-broker kafka-topics --bootstrap-server localhost:9092 --delete --topic demo_topic_example
```

### Consumer Groups

```bash
# List all consumer groups
docker exec -it kafka-java-broker kafka-consumer-groups --bootstrap-server localhost:9092 --list

# Describe a specific consumer group (shows lag, offsets)
docker exec -it kafka-java-broker kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group my-java-application

# Reset consumer group offsets to earliest
docker exec -it kafka-java-broker kafka-consumer-groups --bootstrap-server localhost:9092 --group my-java-application --reset-offsets --to-earliest --topic demo_topic_example --execute
```

### Producing and Consuming Messages

```bash
# Console consumer (reads messages)
docker exec -it kafka-java-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic demo_topic_example --from-beginning

# Console consumer with key and partition visible
docker exec -it kafka-java-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic demo_topic_example --from-beginning --property print.key=true --property print.partition=true

# Console producer (sends messages)
docker exec -it kafka-java-broker kafka-console-producer --bootstrap-server localhost:9092 --topic demo_topic_example

# Console producer with keys
docker exec -it kafka-java-broker kafka-console-producer --bootstrap-server localhost:9092 --topic demo_topic_example --property parse.key=true --property key.separator=:
```

### Monitoring and Debugging

```bash
# View Kafka broker logs
docker-compose logs -f kafka

# Check consumer lag (using kafka-consumer-groups describe)
docker exec -it kafka-java-broker kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group my-java-application

# Get partition offset information
docker exec -it kafka-java-broker kafka-run-class --class kafka.tools.GetOffsetShell --bootstrap-server localhost:9092 --topic demo_topic_example --time -1
```

### Quick Reference Card

| Operation | Docker Command |
|-----------|----------------|
| List topics | `docker exec -it kafka-java-broker kafka-topics --bootstrap-server localhost:9092 --list` |
| Create topic | `docker exec -it kafka-java-broker kafka-topics --bootstrap-server localhost:9092 --create --topic <name> --partitions 3 --replication-factor 1` |
| Delete topic | `docker exec -it kafka-java-broker kafka-topics --bootstrap-server localhost:9092 --delete --topic <name>` |
| Consume messages | `docker exec -it kafka-java-broker kafka-console-consumer --bootstrap-server localhost:9092 --topic <name> --from-beginning` |
| Produce messages | `docker exec -it kafka-java-broker kafka-console-producer --bootstrap-server localhost:9092 --topic <name>` |
| List consumer groups | `docker exec -it kafka-java-broker kafka-consumer-groups --bootstrap-server localhost:9092 --list` |
| Describe consumer group | `docker exec -it kafka-java-broker kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group <group-id>` |

**Note:** Replace `kafka-java-broker` with your actual container name if different (check with `docker ps`).


---
