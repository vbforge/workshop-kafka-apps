# Project 3: Real-Time Analytics Platform with Kafka Streams (Advanced)

## 🎯 Project Overview
Build a comprehensive real-time analytics and monitoring platform for an e-commerce system using Kafka Streams, KSQL, Event Sourcing, CQRS pattern, and Kafka Connect. This advanced project demonstrates production-grade Kafka architecture with stream processing, state stores, windowing operations, and real-time dashboards.

## 💡 What You'll Learn
- Kafka Streams API for stream processing
- KSQL for SQL-like stream queries
- Event Sourcing and CQRS patterns
- Kafka Connect for CDC (Change Data Capture)
- Stateful stream processing with state stores
- Windowing and aggregations
- Stream joins and enrichment
- Schema Registry with Avro
- Real-time dashboards with WebSocket
- Multi-datacenter considerations

## 🏗️ Architecture
```
Source Systems → Kafka Connect (CDC) → Kafka Topics → Kafka Streams Apps → Materialized Views
                                            ↓
                                     [Order Stream, User Stream, Product Stream]
                                            ↓
                        [Join, Filter, Aggregate, Window Operations]
                                            ↓
                              State Stores & Output Topics
                                            ↓
                    [WebSocket Server → Real-time Dashboard]
                                            ↓
                              Read Models (CQRS)
```

## 📋 Prerequisites
- Kafka 4.0.1 (KRaft mode) with 3 brokers
- Java 17
- Spring Boot 3.x
- Confluent Schema Registry
- Kafka Connect
- MySQL (source database)
- PostgreSQL (read models)
- Redis (caching)
- WebSocket support
- Docker & Docker Compose (for final migration)

## 🚀 Step-by-Step Implementation

### Step 1: Infrastructure Setup
**Goal**: Set up production-like Kafka cluster

1. Configure 3-broker Kafka cluster (KRaft):
   - Broker 1: localhost:9092
   - Broker 2: localhost:9093
   - Broker 3: localhost:9094

2. Install and configure Schema Registry:
   - Port: 8081
   - Configure schema compatibility settings

3. Install Kafka Connect:
   - Standalone or distributed mode
   - MySQL JDBC connector
   - Debezium MySQL connector for CDC

4. Set up database infrastructure:
   - MySQL: Source database (write model)
   - PostgreSQL: Read models (CQRS)
   - Redis: Caching layer

### Step 2: Define Comprehensive Topic Architecture
**Goal**: Design topic structure for event sourcing

1. Create event-sourced topics (log compacted):
   ```bash
   # Event Store Topics (infinite retention)
   kafka-topics.sh --create --topic user-events --partitions 5 --replication-factor 3 \
     --config cleanup.policy=compact --config min.compaction.lag.ms=3600000
   
   kafka-topics.sh --create --topic order-events --partitions 5 --replication-factor 3 \
     --config cleanup.policy=compact
   
   kafka-topics.sh --create --topic product-events --partitions 5 --replication-factor 3 \
     --config cleanup.policy=compact
   
   kafka-topics.sh --create --topic inventory-events --partitions 5 --replication-factor 3 \
     --config cleanup.policy=compact
   ```

2. Create stream processing topics:
   ```bash
   # Analytics Topics
   kafka-topics.sh --create --topic order-metrics --partitions 3 --replication-factor 3
   kafka-topics.sh --create --topic user-activity --partitions 3 --replication-factor 3
   kafka-topics.sh --create --topic product-performance --partitions 3 --replication-factor 3
   kafka-topics.sh --create --topic revenue-analytics --partitions 3 --replication-factor 3
   
   # Aggregated Results Topics
   kafka-topics.sh --create --topic hourly-sales --partitions 1 --replication-factor 3
   kafka-topics.sh --create --topic top-products --partitions 1 --replication-factor 3
   kafka-topics.sh --create --topic customer-segments --partitions 3 --replication-factor 3
   ```

3. Create changelog and repartition topics (auto-created by Streams, but good to know)

### Step 3: Set Up Schema Registry with Avro
**Goal**: Implement schema evolution and compatibility

1. Define Avro schemas for all events:
   - `OrderEvent.avsc`
   - `UserEvent.avsc`
   - `ProductEvent.avsc`
   - `InventoryEvent.avsc`

2. Create schema evolution strategy:
   - Define backward/forward compatibility rules
   - Document breaking vs non-breaking changes

3. Implement schema registry integration:
   - Configure Avro serializers/deserializers
   - Handle schema evolution in consumers
   - Implement schema validation in producers

4. Create `SchemaRegistryService`:
   - Register schemas programmatically
   - Validate compatibility before deployment
   - Version management

### Step 4: Implement Event Sourcing Foundation
**Goal**: Build event store and event replay capabilities

1. Create `EventStore` service:
   - Append-only event logging
   - Event versioning
   - Metadata (timestamp, correlation-id, causation-id)

2. Implement event types:
   - `OrderCreatedEvent`
   - `OrderPaymentProcessedEvent`
   - `OrderShippedEvent`
   - `OrderCompletedEvent`
   - `OrderCancelledEvent`
   - `ProductCreatedEvent`
   - `ProductPriceUpdatedEvent`
   - `InventoryAdjustedEvent`
   - `UserRegisteredEvent`
   - `UserProfileUpdatedEvent`

3. Create `EventPublisher`:
   - Publish events to appropriate topics
   - Ensure event ordering per aggregate (partition by ID)
   - Implement outbox pattern for transactional events

4. Implement `EventReplayer`:
   - Rebuild state from event history
   - Support point-in-time reconstruction
   - Handle schema evolution during replay

### Step 5: Implement CQRS Pattern
**Goal**: Separate write and read models

1. **Write Model (Command Side)**:
   - Create command handlers:
     - `CreateOrderCommandHandler`
     - `UpdateInventoryCommandHandler`
     - `UpdatePriceCommandHandler`
   - Validate business rules
   - Publish events to Kafka
   - Store in MySQL (source of truth)

2. **Read Model (Query Side)**:
   - Create projection builders:
     - `OrderProjection` (PostgreSQL)
     - `InventoryProjection` (PostgreSQL)
     - `ProductCatalogProjection` (PostgreSQL)
     - `UserProfileProjection` (Redis cache)
   - Subscribe to event topics
   - Update read models asynchronously
   - Handle eventual consistency

3. Create `QueryService`:
   - Read from optimized read models
   - Support complex queries
   - Implement caching strategy

4. Implement synchronization mechanism:
   - Track event processing position
   - Handle read model rebuilds
   - Ensure idempotency in projections

### Step 6: Build Kafka Streams Application - Real-Time Metrics
**Goal**: Process streams for real-time analytics

1. Create `OrderMetricsStream`:
   - Read from "order-events" topic
   - Calculate metrics:
     - Total orders per minute/hour
     - Average order value
     - Orders by status
     - Orders by region/category
   - Use tumbling windows (1 minute, 5 minutes, 1 hour)
   - Output to "order-metrics" topic

2. Create `RevenueAnalyticsStream`:
   - Join order events with payment events
   - Calculate:
     - Real-time revenue (per minute, hour, day)
     - Revenue by product category
     - Revenue by customer segment
   - Use hopping windows for overlapping periods
   - Maintain state store for running totals

3. Create `ProductPerformanceStream`:
   - Aggregate product sales
   - Calculate:
     - Units sold per product
     - Revenue per product
     - Product ranking (top sellers)
   - Use session windows to detect purchase patterns
   - Join with inventory events for stock alerts

4. Implement state stores:
   - Configure RocksDB state stores
   - Enable changelog topics for recovery
   - Implement custom state store for specific needs

### Step 7: Implement Advanced Stream Processing
**Goal**: Complex stream joins and aggregations

1. **Stream-Stream Join** (Order + Payment):
   - Join order-events with payment-events
   - Use windowed join (within 5 minutes)
   - Handle late arrivals with grace period
   - Produce enriched order-payment events

2. **Stream-Table Join** (Order + User):
   - Create KTable from user-events
   - Join orders with user profiles
   - Enrich orders with customer segment data
   - Output personalized offers based on history

3. **Table-Table Join** (Product + Inventory):
   - Create KTables from product and inventory events
   - Join for complete product availability view
   - Update in real-time as inventory changes
   - Trigger low-stock alerts

4. **Global KTable** (Product Catalog):
   - Create GlobalKTable for product reference data
   - Enable broadcast to all stream processing instances
   - Use for lookups without co-partitioning

### Step 8: Implement Windowing and Time-Based Aggregations
**Goal**: Time-based analytics and aggregations

1. Create `HourlySalesAggregator`:
   - Tumbling window (1 hour)
   - Group by product category
   - Calculate sum, count, average
   - Emit results to "hourly-sales" topic

2. Create `RealTimeLeaderboard`:
   - Sliding window (last 5 minutes)
   - Top 10 products by sales
   - Update every 10 seconds
   - Store in state store + output topic

3. Create `CustomerActivityTracker`:
   - Session window (30 minutes inactivity gap)
   - Track user sessions
   - Calculate session metrics (duration, pages viewed, purchases)
   - Detect abandoned carts

4. Handle out-of-order events:
   - Configure grace period for late arrivals
   - Implement watermark strategy
   - Handle window late data

### Step 9: Implement Kafka Connect for CDC
**Goal**: Capture database changes in real-time

1. Configure Debezium MySQL connector:
   - Monitor `orders`, `products`, `inventory`, `users` tables
   - Capture INSERT, UPDATE, DELETE operations
   - Publish to respective event topics
   - Enable schema changes capture

2. Create connector configuration:
   ```json
   {
     "connector.class": "io.debezium.connector.mysql.MySqlConnector",
     "database.hostname": "localhost",
     "database.port": "3306",
     "database.user": "debezium",
     "database.password": "dbz",
     "database.server.id": "184054",
     "database.server.name": "ecommerce",
     "database.include.list": "ecommerce_db",
     "table.include.list": "ecommerce_db.orders,ecommerce_db.products,ecommerce_db.inventory,ecommerce_db.users",
     "database.history.kafka.bootstrap.servers": "localhost:9092",
     "database.history.kafka.topic": "schema-changes.ecommerce"
   }
   ```

3. Implement transform for CDC events:
   - Extract `after` state from Debezium envelope
   - Transform to domain events
   - Route to appropriate topics
   - Handle tombstone records (deletes)

4. Create sink connectors:
   - JDBC Sink Connector for read models (PostgreSQL)
   - Elasticsearch Sink Connector for search
   - MongoDB Sink Connector for document storage

### Step 10: Build Real-Time Dashboard with WebSocket
**Goal**: Visualize streaming data in real-time

1. Create `WebSocketServer`:
   - Spring WebSocket configuration
   - STOMP protocol for messaging
   - Subscribe to multiple topics

2. Create `DashboardController`:
   - Stream real-time metrics to connected clients
   - Support multiple dashboard views:
     - Sales Dashboard (revenue, orders)
     - Inventory Dashboard (stock levels, alerts)
     - Customer Dashboard (active users, segments)
     - Product Dashboard (top sellers, performance)

3. Implement `MetricsAggregator` consumer:
   - Subscribe to all analytics topics
   - Aggregate data for dashboard
   - Push updates via WebSocket every second
   - Store latest metrics in Redis for quick access

4. Create frontend dashboard (simple HTML/JS):
   - Connect to WebSocket endpoint
   - Display real-time charts (Chart.js)
   - Show live metrics counters
   - Alert on anomalies

### Step 11: Implement Anomaly Detection Stream
**Goal**: Detect unusual patterns in real-time

1. Create `AnomalyDetectionStream`:
   - Monitor order patterns
   - Detect unusual spikes or drops
   - Calculate moving averages and standard deviations
   - Use stateful processing for historical context

2. Implement detection algorithms:
   - Sudden revenue drop (>30% in 5 minutes)
   - Unusual order volume spike
   - High failure rate (payment/inventory)
   - Suspicious user activity patterns

3. Create alert mechanism:
   - Publish alerts to "anomaly-alerts" topic
   - Send to monitoring systems
   - Trigger notifications (email/Slack)
   - Store in database for review

4. Implement `AlertService`:
   - Consume anomaly alerts
   - Deduplicate similar alerts
   - Priority-based routing
   - Auto-resolution for transient issues

### Step 12: Implement Stream Processor Scaling
**Goal**: Design for horizontal scalability

1. Configure stream application for scalability:
   - Set appropriate number of threads
   - Configure standby replicas
   - Enable state store replication
   - Optimize partition assignment

2. Implement partition strategy:
   - Partition by key for related events (orderId, userId)
   - Balance partitions across brokers
   - Plan for repartitioning operations

3. Create monitoring for stream processors:
   - Track lag per partition
   - Monitor state store sizes
   - Rebalance metrics
   - Processing rate and latency

4. Implement graceful shutdown:
   - Close streams cleanly
   - Commit offsets
   - Flush state stores
   - Zero-downtime deployments

### Step 13: Add Complex Event Processing (CEP)
**Goal**: Detect event patterns and sequences

1. Create `PatternDetectionStream`:
   - Detect "view product → add to cart → abandon" pattern
   - Identify potential fraud patterns
   - Detect cross-sell opportunities
   - Monitor fulfillment delays

2. Implement temporal pattern matching:
   - Sequence detection (Event A followed by Event B within time window)
   - Pattern matching with conditions
   - State machine for complex workflows

3. Create `CustomerJourneyTracker`:
   - Track complete customer journey
   - Identify drop-off points
   - Calculate conversion funnels
   - Personalization triggers

4. Output actionable insights:
   - Send to recommendation engine
   - Trigger marketing campaigns
   - Alert customer support
   - Feed ML models

### Step 14: Implement Multi-Environment Configuration
**Goal**: Support dev, staging, production environments

1. Create environment-specific configs:
   - `application-dev.yml`
   - `application-staging.yml`
   - `application-prod.yml`

2. Configure different Kafka clusters per environment:
   - Local: Single broker for development
   - Staging: 3-broker cluster
   - Production: 5+ broker cluster with geo-replication

3. Implement configuration management:
   - Externalize sensitive configs (Spring Cloud Config)
   - Use environment variables
   - Secrets management (Vault)

4. Create deployment pipeline:
   - CI/CD with automated tests
   - Blue-green deployment strategy
   - Canary releases for stream processors
   - Rollback procedures

### Step 15: Add Observability and Monitoring
**Goal**: Production-grade monitoring and debugging

1. Implement distributed tracing:
   - OpenTelemetry integration
   - Trace correlation across services
   - Track message flow through Kafka
   - Visualize with Jaeger/Zipkin

2. Add comprehensive logging:
   - Structured JSON logging
   - Correlation IDs in all logs
   - Log sampling for high-volume streams
   - Centralized logging (ELK stack)

3. Implement metrics collection:
   - Micrometer with Prometheus
   - Custom metrics for business KPIs
   - JMX metrics from Kafka
   - Dashboard in Grafana

4. Create alerting rules:
   - Consumer lag alerts
   - Processing latency alerts
   - Error rate thresholds
   - State store size alerts
   - Rebalance frequency alerts

### Step 16: Implement Data Retention and Archiving
**Goal**: Manage data lifecycle

1. Configure topic retention policies:
   - Hot data: 7 days (fast queries)
   - Warm data: 30 days (archival)
   - Cold data: S3/object storage (compliance)

2. Implement tiered storage:
   - Configure Kafka tiered storage
   - Move old segments to S3
   - Query historical data when needed

3. Create archival jobs:
   - Export old data to data lake
   - Parquet format for analytics
   - Partition by date for efficient queries

4. Implement GDPR compliance:
   - User data deletion pipeline
   - Tombstone records for deleted users
   - Audit log for data access

### Step 17: Testing Strategy
**Goal**: Comprehensive testing at all levels

1. **Unit Tests**:
   - Test topology logic with `TopologyTestDriver`
   - Mock state stores
   - Verify transformations and aggregations

2. **Integration Tests**:
   - Use `@EmbeddedKafka` for integration tests
   - Test complete stream flows
   - Verify state store contents
   - Test windowing behavior

3. **Performance Tests**:
   - Load testing with millions of events
   - Measure throughput and latency
   - Test backpressure handling
   - State store performance

4. **Chaos Engineering**:
   - Kill stream processors randomly
   - Simulate broker failures
   - Network partitions
   - Verify recovery and consistency

5. **End-to-End Tests**:
   - Complete business scenarios
   - Multi-service flows
   - Verify eventual consistency
   - Dashboard updates validation

### Step 18: Docker and Kubernetes Deployment
**Goal**: Containerize and orchestrate the platform

1. Create Dockerfiles for all services:
   - Multi-stage builds for optimization
   - Health check endpoints
   - Proper signal handling

2. Create `docker-compose.yml`:
   - Kafka cluster (3 brokers)
   - Schema Registry
   - Kafka Connect
   - All microservices
   - Databases (MySQL, PostgreSQL, Redis)
   - Monitoring stack (Prometheus, Grafana)

3. Prepare for Kubernetes (optional):
   - Create Helm charts
   - StatefulSets for Kafka
   - Deployments for stream processors
   - ConfigMaps and Secrets
   - Horizontal Pod Autoscaling

4. Implement service mesh considerations:
   - Inter-service communication
   - Traffic management
   - Security policies

## 🧪 Testing Scenarios

1. **Real-Time Analytics**: Generate 10K orders → Verify real-time metrics updates → Check dashboard
2. **Stream Processing**: Test windowing with out-of-order events → Verify aggregations
3. **Anomaly Detection**: Simulate spike in orders → Verify alert triggered
4. **CDC**: Update database directly → Verify event captured and processed
5. **Failover**: Kill stream processor → Verify state recovery from changelog
6. **Schema Evolution**: Deploy new schema version → Verify backward compatibility
7. **Scalability**: Add stream processor instance → Verify rebalancing and processing
8. **Event Sourcing**: Replay events from beginning → Rebuild state → Verify consistency
9. **CQRS**: Update write model → Verify eventual consistency in read model
10. **End-to-End**: Place order → Track through all systems → Verify analytics updated

## 📝 Deliverables

- Complete event-sourced e-commerce platform
- 10+ Kafka Streams applications
- Real-time analytics dashboard
- CDC pipeline with Kafka Connect
- CQRS implementation with read/write separation
- Anomaly detection system
- Comprehensive monitoring and observability
- Docker Compose setup for entire platform
- Performance test results and tuning guide
- Complete documentation:
  - Architecture Decision Records (ADRs)
  - API documentation
  - Deployment guide
  - Operations runbook
  - Troubleshooting guide

## 🎓 Key Concepts Covered

- Kafka Streams API (KStream, KTable, GlobalKTable)
- Stateful stream processing with RocksDB
- Windowing (tumbling, hopping, sliding, session)
- Stream-stream, stream-table, table-table joins
- Event Sourcing and Event Store
- CQRS (Command Query Responsibility Segregation)
- Kafka Connect and CDC (Change Data Capture)
- Schema Registry and Avro
- Exactly-once semantics in streams
- State store management and recovery
- Real-time analytics and aggregations
- Complex Event Processing (CEP)
- WebSocket for real-time updates
- Observability and distributed tracing
- Production deployment patterns

## 🚀 Performance Optimization Tips

1. **Tune Kafka Streams**:
   - Adjust `num.stream.threads`
   - Optimize `commit.interval.ms`
   - Configure `cache.max.bytes.buffering`

2. **State Store Optimization**:
   - Enable RocksDB bloom filters
   - Tune block cache size
   - Configure compaction strategies

3. **Partition Strategy**:
   - Ensure even partition distribution
   - Co-locate related data (co-partitioning)
   - Monitor partition skew

4. **Network Optimization**:
   - Batch records with `linger.ms`
   - Compression (snappy, lz4, zstd)
   - Minimize serialization overhead

## 💻 Advanced Kafka Commands

```bash
# Describe stream application state
kafka-streams-application-reset.sh \
  --application-id order-metrics-app \
  --bootstrap-servers localhost:9092 \
  --input-topics order-events

# Monitor state store size
kafka-run-class.sh kafka.tools.StateStoreDumper \
  --directory /tmp/kafka-streams/order-metrics-app

# Check lag for streams application
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-metrics-app \
  --describe

# Export metrics to Prometheus
kafka-run-class.sh kafka.tools.JmxTool \
  --object-name kafka.streams:type=stream-metrics \
  --reporting-interval 1000
```

## 🔍 Troubleshooting Guide

1. **High Consumer Lag**: Increase parallelism, optimize processing logic
2. **Rebalance Storms**: Increase `max.poll.interval.ms`, optimize processing time
3. **State Store Too Large**: Implement windowed aggregations, add retention
4. **Out of Memory**: Adjust RocksDB cache, tune JVM heap
5. **Duplicate Processing**: Verify idempotency, check exactly-once config
6. **Slow Joins**: Verify co-partitioning, optimize join windows

## 📚 Additional Features to Explore

- **Kafka Streams Interactive Queries**: Query state stores directly
- **Kafka Streams GlobalKTable**: Reference data without co-partitioning
- **Custom Serdes**: Implement custom serialization
- **Processor API**: Low-level stream processing
- **Kafka MirrorMaker 2**: Multi-datacenter replication
- **Kafka Quotas**: Rate limiting and fair resource allocation
- **Kafka ACLs**: Security and authorization

## 🎯 Interview Talking Points

This project demonstrates:
- Production-grade Kafka architecture
- Real-time stream processing at scale
- Event-driven architecture patterns
- Distributed systems concepts
- Microservices best practices
- DevOps and observability
- Problem-solving and system design skills

You'll be able to discuss:
- Trade-offs in stream processing (latency vs throughput)
- Handling failures and ensuring consistency
- Scaling strategies for high-volume systems
- Schema evolution and backward compatibility
- Monitoring and debugging distributed systems

## 🏆 Congratulations!

By completing this advanced project, you'll have:
- Portfolio-worthy project demonstrating expertise
- Deep understanding of Kafka ecosystem
- Real-world experience with stream processing
- Production-ready code examples
- Strong foundation for Senior Developer role

Good luck with your Kafka journey! 🚀🎉