package com.vbforge.case17;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

// JUNIOR NOTE: This abstract base class is the central piece of the Testcontainers setup.
//
// WHY A BASE CLASS?
// If every test class spins up its own KafkaContainer, you get:
//   - A new Docker container per test class
//   - Slow startup time × N test classes
//   - Resource exhaustion in CI
//
// By declaring @Container as static, Testcontainers starts the container
// ONCE per JVM run and shares it across ALL test classes that extend this base.
// This is the "shared container" pattern — recommended for heavy infrastructure.
//
// CONTAINER LIFECYCLE:
//   static @Container → started before the first test in any extending class,
//                       stopped after ALL tests in the JVM run complete.
//   non-static @Container → started/stopped per test method (expensive for Kafka).
//
// @DynamicPropertySource is the magic:
//   Spring's test context is built before the container starts, so you can't
//   put the dynamic port in application.yml. @DynamicPropertySource runs AFTER
//   the container starts but BEFORE the Spring context is built.
//   It injects the actual host:port into the property system, which KafkaConfig
//   picks up via @Value("${spring.kafka.bootstrap-servers}").
//   Result: your real KafkaConfig beans connect to the Testcontainers Kafka.
//
// DOCKER IMAGE:
//   confluentinc/cp-kafka matches the production docker-compose.yml.
//   Using the same image = same Kafka version = same behavior in tests as in prod.

@Testcontainers
public abstract class AbstractKafkaIntegrationTest {

    // JUNIOR NOTE: Docker connection is configured via
    // src/test/resources/testcontainers.properties — no code needed here.
    // tc.host points to the real Docker Desktop engine pipe on Windows.


    @Container
    static final ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:8.0.0")
    );


    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        // JUNIOR NOTE: This overrides spring.kafka.bootstrap-servers in the test context.
        // kafka.getBootstrapServers() returns something like "localhost:55123" —
        // the randomly assigned Docker host port. Spring rebuilds KafkaConfig beans
        // with this value, so all producers and consumers connect to the test container.
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

}
