package com.vbforge.kafka06;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

/**
 * Application context smoke test.
 *
 * <p><b>Problem with the default generated test:</b>
 * {@code @SpringBootTest} starts the full Spring context. Our context
 * includes {@code KafkaAdmin} (in {@code KafkaTopicConfig}) and all the
 * listener container factories, which immediately try to connect to a
 * real Kafka broker on {@code localhost:9092}. If Docker is not running,
 * they retry indefinitely and the test hangs.
 *
 * <p><b>Solution — {@code @EmbeddedKafka}:</b>
 * This annotation starts a lightweight, in-memory Kafka broker (provided
 * by {@code spring-kafka-test}) for the duration of the test. It also
 * overrides {@code spring.kafka.bootstrap-servers} to point at the embedded
 * broker automatically, so no Docker or external Kafka is needed at all.
 *
 * <p>{@code @DirtiesContext} tells Spring to shut down and discard the
 * application context after this test class. This is important when using
 * {@code @EmbeddedKafka} — it ensures the embedded broker and all its
 * listener threads are cleanly stopped so they do not bleed into other tests.
 *
 * <p><b>What this test verifies:</b>
 * The entire Spring context wires up correctly — all beans in
 * {@code config/}, {@code consumer/}, {@code controller/}, and
 * {@code producer/} are created without errors. If any bean definition,
 * property binding, or dependency injection is broken, this test will fail
 * with a descriptive error before you even run the application.
 */
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
		partitions = 3,   // enough partitions for all three topics in this demo
		// Override bootstrap-servers so every Kafka bean uses the embedded broker
		// instead of trying to connect to localhost:9092
		brokerProperties = {
				"transaction.state.log.replication.factor=1",  // required for transactional producer
				"transaction.state.log.min.isr=1"              // required for transactional producer
		}
)
class Kafka06ApplicationTests {

	/**
	 * Verifies that the Spring application context loads without errors.
	 *
	 * <p>No assertions needed — if the context fails to start, the test
	 * fails automatically with the full error message and stack trace.
	 */
	@Test
	void contextLoads() {
		// Intentionally empty.
		// Spring Boot's test infrastructure handles the assertion:
		// if any bean fails to initialize, this test throws and fails.
	}
}
