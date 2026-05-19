# Kafka Learning Scenarios

* > This is a comprehensive, hands-on Java project designed to teach Apache Kafka through practical, real-world scenarios.
* > 5 progressive scenarios across app provided (using pure Java Kafka client): basic messaging → load balancing → keyed messages → manual offsets → complete e-commerce pipeline

---

## Key Features
- 5+ complete scenarios covering beginner to advanced topics
- Self-contained examples with detailed comments
- Real-world e-commerce use case
- Production-ready code patterns
- Comprehensive documentation

---

## Project structure

- **kafka-02-learning-scenarios**: 

   * **config** (configurations across all scenarios)
   * **scenario_01_simple**
   * **scenario_02_demo_app**
   * **scenario_03_load_balancing**
   * **scenario_04_topic_keyed**
   * **scenario_05_topic_orders**
   * **scenario_06_e_commerce_orders_app**

---

## Test Kafka Connection
- start docker;
- run docker-compose from the root of the project: **docker-compose up -d**
- run **DockerConnectivityTest**
- expected console output:
```
[main] INFO com.vbforge.config.DockerConnectivityTest - Successfully connected to Docker Kafka!

...

[main] INFO com.vbforge.config.DockerConnectivityTest - Test Kafka Configuration Connectivity Completed!
```

- stop and remove container: `docker-compose down -v`

NOTES: 
1) during development and exploration of concerns, `Utility` and `Constants` could be updated with new topics and configurations;
2) for simplicity, I use Kafka (KRaft Mode) only image, but in some cases it might be useful to have a full complete docker image with Kafka UI, DB, etc.
3) in each package of concrete scenario provided a flow of `how_to_run_scenario_X` description;

---

## What we explore (6 scenarios):

1) **scenario_01_simple:** 
   - explore simple `Producer` & `Consumer`;
   - `Consumer` could be gracefully shutdown `Ctrl + C` since it run infinitely (shutdown-hook-pattern provided);
   - callbacks for sent messages also provided;
   - [how to run this scenario](src/main/java/com/vbforge/scenario_01_simple/how_to_run_scenario_01_simple.md)


2) **scenario_02_demo_app:**
   - explore 3 types of Producer: one message only, some messages with `null` key (round-robin) and some messages with keys;
   - MyConsumer and MyConsumer2 for testing broadcast behavior (different group IDs → both consumers receive all messages); 
   - Load balance (messages shared between consumers) when consumer group is same for both;
   - demonstrated how Kafka persists messages across consumer restarts;
   - [how to run this scenario](src/main/java/com/vbforge/scenario_02_demo_app/how_to_run_scenario_02_demo_app.md)


3) **scenario_03_load_balancing:**
   - descriptions not provided yet...

   
4) **scenario_04_topic_keyed:**
   - descriptions not provided yet...


5) **scenario_05_topic_orders:**
   - descriptions not provided yet...


6) **scenario_06_e_commerce_orders_app:**
   - descriptions not provided yet...


---