# Case 07 — Consumer Groups (Multi-Group Fan-Out + Rebalancing)

| Field          | Value                                          |
|----------------|------------------------------------------------|
| Module         | `case-07-consumer-groups`                      |
| Port           | `8087`                                         |
| Topic          | `case-07-topic` (3 partitions)                 |
| Consumer Groups| `case-07-analytics-group` · `case-07-audit-group` · `case-07-notify-group` |
| Spring Boot    | `4.0.6`                                        |
| Java           | `21`                                           |

---

## What This Case Covers

| Concept | Description |
|---------|-------------|
| **Consumer groups** | Multiple independent groups on the same topic — each gets every message |
| **Fan-out pattern** | One event → many independent consumers, each with different business logic |
| **Independent offsets** | Each group tracks its own committed offsets; one group's lag doesn't affect others |
| **Per-group `ContainerFactory`** | Separate Spring factory bean per group is required to configure distinct `group.id` |
| **Rebalancing** | Kafka redistributes partitions when a consumer joins or leaves a group |
| **Partition assignment** | With 3 partitions + concurrency=3: each thread in each group owns one partition |

---

## Project Structure

```
case-07-consumer-groups/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/vbforge/case07/
    │   │   ├── MainApp.java
    │   │   ├── config/
    │   │   │   └── KafkaConfig.java               # 3 listener factories
    │   │   ├── controller/
    │   │   │   └── ConsumerGroupsController.java
    │   │   ├── model/
    │   │   │   ├── OrderEvent.java
    │   │   │   └── ProducerResponse.java
    │   │   └── service/
    │   │       ├── ProducerService.java
    │   │       ├── AnalyticsConsumer.java          # group 1
    │   │       ├── AuditConsumer.java              # group 2
    │   │       └── NotifyConsumer.java             # group 3
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/vbforge/case07/
            └── MainAppTests.java
```

---

## Quick Start

#### 1. Start Kafka + create topic
```bash
docker compose up -d

docker exec kafka-08-broker kafka-topics \
  --bootstrap-server localhost:19092 \
  --create --topic case-07-topic \
  --partitions 3 --replication-factor 1
```

#### 2. Run the module
```bash
cd case-07-consumer-groups
mvn spring-boot:run
```

At startup, all three consumer groups join and each performs a rebalance. Watch the logs for `[ANALYTICS]`, `[AUDIT]`, and `[NOTIFY]` prefixes.

---

## Demo

```bash
# Send 9 order events (3 PLACED, 3 PAID, 3 SHIPPED across EU/US/ASIA regions)
curl -X POST "http://localhost:8087/api/producer/send?count=9"

# Check all three groups' independent state
curl http://localhost:8087/api/status
```

Expected `/status` response shape:
```json
{
  "analytics": {
    "group": "case-07-analytics-group",
    "countsByRegion": { "EU": 3, "US": 3, "ASIA": 3 },
    "totalsByRegion": { "EU": 85.5, "US": 108.0, "ASIA": 130.5 }
  },
  "audit": {
    "group": "case-07-audit-group",
    "auditLogSize": 9,
    "lastEntries": ["AUDIT | partition=..."]
  },
  "notify": {
    "group": "case-07-notify-group",
    "notificationsSent": 6,
    "notificationsSkipped": 3
  }
}
```

All three processed 9 messages. Notify sent 6 (PLACED + SHIPPED) and skipped 3 (PAID).

---

## Verify via Docker CLI

```bash
# See all three groups and their independent committed offsets
docker exec kafka-08-broker kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --describe --group case-07-analytics-group

docker exec kafka-08-broker kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --describe --group case-07-audit-group

docker exec kafka-08-broker kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --describe --group case-07-notify-group
```

Each group shows its own `CURRENT-OFFSET` and `LAG` columns — independent of each other.

---

## How to Break It (Failure Scenarios)

### Scenario 1 — All groups share same group.id (messages split, not fanned out)
Change all three `containerFactory` references in the consumer classes to `"analyticsContainerFactory"` (same factory = same group). Restart and send 9 messages. Instead of all three groups getting all 9, the 9 messages are divided between the three `@KafkaListener` methods competing within the same group. Fan-out breaks — this is the classic multi-listener misconfiguration.

### Scenario 2 — Observe independent lag
Stop the app. Send 9 messages while it's down. Restart. All three groups see `LAG=9` initially via Docker CLI, then drain independently as each group processes its backlog. The groups catch up at their own pace.

### Scenario 3 — Rebalance trigger
With the app running, open a second terminal and run the app again on a different port (temporarily edit `server.port=8090`). Both instances try to join the same groups. Watch the logs for `Rebalancing` — each group redistributes its 3 partitions across 2 instances (2 + 1 split). Stop the second instance — another rebalance, partitions return to the first.

---

## API Reference

| Method | Endpoint | Params | Returns |
|--------|----------|--------|---------|
| GET | `/api/health` | — | `200` string |
| POST | `/api/producer/send` | `count` (default 9) | `200 ProducerResponse` |
| GET | `/api/status` | — | `200` all-groups summary |

---

## Learning Checklist

- [ ] Send 9 events, confirm all three groups received all 9 via `/status`
- [ ] Confirm notify skipped exactly 3 (the PAID events)
- [ ] Run Docker CLI commands — confirm three separate committed offset rows
- [ ] Explain why three separate `ContainerFactory` beans are required (not one)
- [ ] Explain why groups don't share messages (independent offset tracking)
- [ ] Explain when you'd add a 4th group vs reusing an existing group
- [ ] Explain what triggers a rebalance and what happens during one

---

## See Also

- [THEORY-Q-and-A-SECTION.md](THEORY-Q-and-A-SECTION.md)
- case-05 → concurrency within a single group
- case-08 → offset management (`seek()`, strategies)
