## 🚀 Kafka + Docker – Quick Start Guide

### 📋 Prerequisites

- Docker Desktop installed and running
- Terminal access to the project root folder

> 📁 **File Location:** `kafka-07-producer-consumer-separate-modules/docker-compose.yml`

---

### 🐳 Docker Commands

#### Start the Environment

| Command | Description |
|---------|-------------|
| `docker compose up -d` | Pull images and start containers in detached mode |
| `docker ps` | Verify containers are running |

#### Manage Containers

| Command | Description |
|---------|-------------|
| `docker compose stop` | Stop containers (topics persist) |
| `docker compose start` | Start existing containers |
| `docker compose down` | Stop and remove containers |

---

### 🎯 Run the Applications

> **Important:** Both applications must be running for full functionality

1. **Start Producer App** – Run `ProducerApp` 
2. **Start Consumer App** – Run `ConsumerApp`

---

### 🔌 Test the API

**Check if Producer is running:**

```http
GET http://localhost:8081/api/messages/test
```

---

### 🐘 Kafka Container Operations

#### Access Kafka Container

```bash
docker exec -it kafka-service-app bash
```

#### Topic Management Commands

Run these **inside** the container (`[appuser@kafka-service-app ~]$`):

| Action | Command |
|--------|---------|
| **List topics** | `kafka-topics --bootstrap-server localhost:19092 --list` |
| **Create topic** | `kafka-topics --bootstrap-server localhost:19092 --create --topic t-hello --partitions 1` |
| **Describe topic** | `kafka-topics --bootstrap-server localhost:19092 --describe --topic t-hello` |
| **Delete topic** | `kafka-topics --bootstrap-server localhost:19092 --delete --topic t-test` |

#### Example: Create and Delete a Topic

```bash
# Create a test topic
kafka-topics --bootstrap-server localhost:19092 --create --topic t-test --partitions 1

# Verify it exists
kafka-topics --bootstrap-server localhost:19092 --list

# Delete the topic
kafka-topics --bootstrap-server localhost:19092 --delete --topic t-test

# Confirm deletion
kafka-topics --bootstrap-server localhost:19092 --list
```

---

### 🎨 Conduktor Console (UI)

> Access the web interface for visual Kafka monitoring

**URL:** http://localhost:8080/console/default/home

**Requirements:**
- ✅ Docker containers running
- ✅ Producer & Consumer apps running
- ✅ Conduktor Console healthy

---

### 📊 Quick Reference

| Component | Access Point |
|-----------|--------------|
| **Conduktor UI** | http://localhost:8080 |
| **Producer API** | http://localhost:8081 |
| **Kafka Broker** | `localhost:9092` (external) / `kafka-service-app:19092` (internal) |

---

### ⚠️ Notes

- Kafka runs in **KRaft mode** (no ZooKeeper required)
- Topics persist after `docker compose stop`
- Use `docker compose down` to completely remove containers

---