# Case 08 — Consumer Offset Management (`seek()` + Offset Strategies)

| Field          | Value                                          |
|----------------|------------------------------------------------|
| Module         | `case-08-consumer-offset-management`           |
| Port           | `8088`                                         |
| Topic          | `case-08-topic`                                |
| Consumer Group | `case-08-consumer-group`                       |
| Spring Boot    | `4.0.6`                                        |
| Java           | `21`                                           |

---

## What This Case Covers

| Concept | Description |
|---------|-------------|
| **`seekToBeginning()`** | Rewind all assigned partitions to offset 0 — full replay |
| **`seekToEnd()`** | Jump to latest offset — intentionally skip backlog |
| **`seek(partition, offset)`** | Move one partition to a specific offset — surgical seek |
| **`position(tp)`** | Where the consumer will fetch NEXT — not persisted |
| **`committed(partitions)`** | Last committed offset — what persists across restarts |
| **Skip pattern** | Commit past a bad offset without processing it (poison pill handling) |
| **`auto.offset.reset`** | `earliest` / `latest` / `none` — applies only when no committed offset exists |

### Position vs Committed Offset — the core distinction

```
After polling 10 records, NO commitSync called:

  position(partition=0)   = 10   ← consumer will fetch record #10 next
  committed(partition=0)  = 0    ← broker thinks we're still at 0

  → restart now = reprocess records 0–9 (at-least-once)

After commitSync():

  position(partition=0)   = 10
  committed(partition=0)  = 10   ← they match

  → restart now = resume from record #10 (no duplicates)
```

---

## Project Structure

```
case-08-consumer-offset-management/
├── pom.xml
└── src/
    ├── main/
    │   ├── java/com/vbforge/case08/
    │   │   ├── MainApp.java
    │   │   ├── config/
    │   │   │   └── KafkaConfig.java
    │   │   ├── controller/
    │   │   │   └── OffsetManagementController.java
    │   │   ├── model/
    │   │   │   ├── EventMessage.java
    │   │   │   ├── OffsetStatus.java
    │   │   │   └── ProducerResponse.java
    │   │   └── service/
    │   │       ├── OffsetManagementConsumerService.java   # ← THE main class
    │   │       └── ProducerService.java
    │   └── resources/
    │       └── application.yml
    └── test/
        └── java/com/vbforge/case08/
            └── MainAppTests.java
```

---

## Quick Start

```bash
docker compose up -d
cd case-08-consumer-offset-management
mvn spring-boot:run
```

---

## Interactive Demo — Offset Operations

### Demo 1: Position vs Committed (the core distinction)
```bash
# 1. Send 10 messages — consumer processes and commits them
curl -X POST "http://localhost:8088/api/producer/send?count=10"

# 2. Check status — positions and committed should match at 10
curl http://localhost:8088/api/consumer/status
# → currentPositions: {0: 10}, committedOffsets: {0: 10}
```

### Demo 2: seekToBeginning — full replay
```bash
# 1. Rewind to offset 0
curl -X POST http://localhost:8088/api/consumer/seek/beginning

# 2. Watch logs — all 10 messages replay from the start
# Sequence numbers 1–10 appear again in logs

# 3. Check status — positions back at 0 → advance to 10 again as they process
curl http://localhost:8088/api/consumer/status
```

### Demo 3: seekToEnd — skip the backlog
```bash
# 1. Pause consumer
curl -X POST http://localhost:8088/api/consumer/pause

# 2. Send 15 more messages (they queue up)
curl -X POST "http://localhost:8088/api/producer/send?count=15"

# 3. Skip entire backlog
curl -X POST http://localhost:8088/api/consumer/seek/end

# 4. Resume — consumer starts from 'now', the 15 queued messages are skipped
curl -X POST http://localhost:8088/api/consumer/resume

# 5. Send 3 fresh messages — only THESE are consumed
curl -X POST "http://localhost:8088/api/producer/send?count=3"
```

### Demo 4: Skip a specific offset (poison pill pattern)
```bash
# 1. Check what offset a known-bad message is at via /status
curl http://localhost:8088/api/consumer/status

# 2. Skip offset 3 on partition 0 (commit past it without processing)
curl -X POST "http://localhost:8088/api/consumer/skip?partition=0&offset=3"

# 3. Consumer committed offset 4, record at offset 3 is permanently skipped
```

### Demo 5: Surgical seek — replay one specific batch
```bash
# Replay partition 0 from offset 5 only
curl -X POST "http://localhost:8088/api/consumer/seek/offset?partition=0&offset=5"
# Records from offset 5 onward replay — earlier records untouched
```

---

## Verify via Docker CLI

```bash
# Check committed offsets
docker exec kafka-08-broker kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --describe --group case-08-consumer-group

# Watch CURRENT-OFFSET and LAG columns change as you run demo commands
```

---

## How to Break It (Failure Scenarios)

### Scenario 1 — The position/committed gap (at-least-once proof)
After consuming 10 records: **stop the app** (Ctrl+C) before the next `commitSync()` fires (tricky timing, but worth trying). Restart — the consumer replays from the last committed offset. If position was 10 but committed was 5, you get records 5–9 again.

### Scenario 2 — seekToEnd data loss
Pause → queue 20 messages → `seekToEnd` → resume → check status. The 20 messages are gone forever for this consumer group — their committed offset jumps past them. Run `kafka-consumer-groups --describe` and note that `LAG = 0` despite 20 unprocessed messages (because committed offset now equals end offset).

### Scenario 3 — auto.offset.reset = none
Change `auto-offset-reset: earliest` to `none` in `application.yml`. Delete the consumer group offsets via CLI, then restart the app:
```bash
docker exec kafka-08-broker kafka-consumer-groups \
  --bootstrap-server localhost:19092 \
  --delete --group case-08-consumer-group
```
The consumer throws `NoOffsetForPartitionException` on startup — `none` refuses to guess a starting position. Useful in production when missing history is a bug, not a recoverable situation.

---

## API Reference

| Method | Endpoint | Params | Returns |
|--------|----------|--------|---------|
| GET | `/api/health` | — | `200` string |
| POST | `/api/producer/send` | `count` (default 10) | `200 ProducerResponse` |
| GET | `/api/consumer/status` | — | `200 OffsetStatus` |
| POST | `/api/consumer/seek/beginning` | — | `200` string |
| POST | `/api/consumer/seek/end` | — | `200` string |
| POST | `/api/consumer/seek/offset` | `partition`, `offset` | `200` string |
| POST | `/api/consumer/skip` | `partition`, `offset` (required) | `200` string |
| POST | `/api/consumer/pause` | — | `200` string |
| POST | `/api/consumer/resume` | — | `200` string |

---

## Learning Checklist

- [ ] Run Demo 1 — confirm `currentPositions` and `committedOffsets` match after processing
- [ ] Run Demo 2 — watch messages replay from offset 0 after `seekToBeginning`
- [ ] Run Demo 3 — confirm `seekToEnd` skips queued messages entirely
- [ ] Run Demo 4 — use `skip` to commit past a specific offset
- [ ] Articulate the difference between `position` and `committed offset` in one sentence
- [ ] Explain what `auto.offset.reset=none` does and when to use it
- [ ] Explain the skip pattern and why you'd publish to a DLT before committing past a bad record
- [ ] Explain why `seekToBeginning` alone doesn't make the rewind "permanent" (you must also commit)

---

## See Also

- [THEORY-Q-and-A-SECTION.md](THEORY-Q-and-A-SECTION.md)
- case-06 → manual poll loop foundations (`commitSync`, `WakeupException`, thread safety)
- case-11 → Dead Letter Topic (the proper poison-pill handler, pairs with the skip pattern)
