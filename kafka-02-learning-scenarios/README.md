# Kafka Learning Scenarios

> This is a comprehensive, hands-on Java project designed to teach Apache Kafka through practical, real-world scenarios.

---

## Key Features
- 5+ complete scenarios covering beginner to advanced topics
- Self-contained examples with detailed comments
- Real-world e-commerce use case
- Production-ready code patterns
- Comprehensive documentation

---

## Test Kafka Connection
- start docker;
- run docker-compose from the root of the project: `docker-compose up -d`
- run `DockerConnectivityTest`
- expected console output:
```
[main] INFO com.vbforge.config.DockerConnectivityTest - Test Kafka Configuration Connectivity Started...
[main] INFO com.vbforge.config.Utility - === Kafka Configuration ===
[main] INFO com.vbforge.config.Utility - Bootstrap Servers: localhost:9092
[main] INFO com.vbforge.config.Utility - Source:            default (localhost)
[main] INFO com.vbforge.config.Utility - Active Topics:
[main] INFO com.vbforge.config.Utility -   - Scenario 00: topic-test-connectivity-kafka
[main] INFO com.vbforge.config.Utility - ===========================
...
[main] INFO com.vbforge.config.DockerConnectivityTest - Test Kafka Configuration Connectivity Completed!
```

- stop and remove container: `docker-compose down -v`

NOTES: 
1) during development and exploration of concerns, `Utility` and `Constants` will be updated with new topics and configurations;
2) for simplicity, I use Kafka (KRaft Mode) only image, but in some cases it might be useful to have a full complete docker image with Kafka UI, DB, etc.
3) in each package of concrete scenario provided a flow of `how-to-run` description;

---

## What we explore (6 scenarios):

1) **scenario_01_simple:** 
   - explore simple `Producer` & `Consumer`;
   - `Consumer` could be gracefully shutdown `Ctrl + C` since it run infinitely (shutdown-hook-pattern provided);
   - callbacks for sent messages also investigated;

2) **scenario_02_demo_app**

3) **scenario_03_load_balancing**

4) **scenario_04_topic_keyed**

5) **scenario_05_topic_orders**

6) **scenario_06_e_commerce_orders_app**


---

### next todo:
1) **scenario_01_simple:** 






















