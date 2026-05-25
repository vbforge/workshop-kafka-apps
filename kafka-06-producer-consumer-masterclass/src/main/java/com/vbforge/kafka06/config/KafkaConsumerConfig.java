package com.vbforge.kafka06.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumer configuration: one {@link ConcurrentKafkaListenerContainerFactory} per scenario.
 *
 * <p>Each factory is a named Spring bean. {@link com.vbforge.kafka06.consumer.MessageListener}
 * selects the right factory via {@code containerFactory = "..."} in {@code @KafkaListener}.
 *
 * <p>Factory summary:
 * <pre>
 * ┌──────────────────────────────────────┬────────────────────────────────────────────────┐
 * │ Bean name                            │ Purpose                                        │
 * ├──────────────────────────────────────┼────────────────────────────────────────────────┤
 * │ generalListenerFactory               │ Basic multi-group / metadata demo              │
 * │ priorityListenerFactory              │ Priority-1 messages only (key filter)          │
 * │ otherPriorityListenerFactory         │ All non-priority-1 messages (key filter)       │
 * │ errorHandlerListenerFactory          │ Auto-retry with fixed backoff                  │
 * │ transactionalListenerFactory         │ Read-committed isolation (sees only committed) │
 * └──────────────────────────────────────┴────────────────────────────────────────────────┘
 * </pre>
 */
@Slf4j
@EnableKafka   // required — activates scanning for @KafkaListener annotations
@Configuration
public class KafkaConsumerConfig {

    /** Isolation level that makes consumers see only transactionally committed messages. */
    private static final String READ_COMMITTED = "read_committed";

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // =============================================================
    // FACTORY 1: General — used for sync/async/keyed demos
    // =============================================================

    /**
     * The "default" factory used by most listeners in this project.
     * No special features — demonstrates the standard Spring Kafka setup.
     *
     * <p>Concurrency = 1 means one thread polls one partition at a time.
     * Increase it (up to the partition count) to process partitions in parallel.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> generalListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("group-general"));
        factory.setConcurrency(1);
        factory.setAutoStartup(true);
        return factory;
    }

    // =============================================================
    // FACTORY 2 & 3: Message filtering by key
    // =============================================================

    /**
     * Accepts ONLY messages whose key is exactly {@code "priority1"}.
     *
     * <p>How {@code setRecordFilterStrategy} works:
     * The lambda returns {@code true}  → record is SKIPPED (filtered out).
     * The lambda returns {@code false} → record is PASSED to the listener.
     *
     * <p>So "filter out everything that is NOT priority1" means:
     * {@code return !"priority1".equals(key)} — keep only priority1.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> priorityListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("group-priority"));
        // Filter: skip the record if its key is NOT "priority1"
        factory.setRecordFilterStrategy(record -> !"priority1".equals(record.key()));
        factory.setConcurrency(1);
        factory.setAutoStartup(true);
        return factory;
    }

    /**
     * Accepts every message EXCEPT those with key {@code "priority1"}.
     * The inverse of {@link #priorityListenerFactory()}.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> otherPriorityListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("group-other-priority"));
        // Filter: skip the record if its key IS "priority1"
        factory.setRecordFilterStrategy(record -> "priority1".equals(record.key()));
        factory.setConcurrency(1);
        factory.setAutoStartup(true);
        return factory;
    }

    // =============================================================
    // FACTORY 4: Error handling with automatic retry
    // =============================================================

    /**
     * Demonstrates {@link DefaultErrorHandler}: if the listener throws an exception,
     * Kafka automatically retries up to 2 more times with a 500 ms gap.
     * After all retries are exhausted, the record is skipped (logged) and processing continues.
     *
     * <p>In production you would route failed records to a Dead Letter Topic (DLT)
     * using {@code DeadLetterPublishingRecoverer} — that is covered in kafka-07.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> errorHandlerListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("group-error-handler"));
        factory.setCommonErrorHandler(buildDefaultErrorHandler());
        factory.setConcurrency(1);
        factory.setAutoStartup(true);
        return factory;
    }

    /**
     * Builds a {@link DefaultErrorHandler} that retries 2 times with 500 ms between attempts.
     *
     * <p>{@link FixedBackOff} parameters: (intervalMs, maxAttempts).
     * maxAttempts = 2 means the listener is called 3 times total (1 original + 2 retries).
     */
    private DefaultErrorHandler buildDefaultErrorHandler() {
        DefaultErrorHandler handler = new DefaultErrorHandler(new FixedBackOff(500L, 2L));

        // RetryListener gives us visibility into each failed attempt — great for debugging
        handler.setRetryListeners((record, exception, attempt) ->
                log.warn("[ERROR-HANDLER] Retry #{} | topic={} partition={} offset={} | error={}",
                        attempt,
                        record.topic(),
                        record.partition(),
                        record.offset(),
                        exception.getMessage())
        );

        return handler;
    }

    // =============================================================
    // FACTORY 5: Transactional — read-committed isolation
    // =============================================================

    /**
     * Consumers using this factory will ONLY see messages that were
     * committed as part of a completed transaction.
     *
     * <p>Without {@code read_committed}, a consumer could read messages
     * from a transaction that was later rolled back — a phantom read.
     * Setting isolation level = read_committed prevents this.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> transactionalListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("group-transactional", READ_COMMITTED));
        factory.setConcurrency(1);
        factory.setAutoStartup(true);
        return factory;
    }

    // =============================================================
    // Private helpers — ConsumerFactory builders
    // =============================================================

    /** Shortcut: default isolation level (read_uncommitted). */
    private ConsumerFactory<String, Object> consumerFactory(String groupId) {
        return consumerFactory(groupId, "read_uncommitted");
    }

    /**
     * Builds a fully configured {@link ConsumerFactory}.
     *
     * <p><b>Deserialization strategy — two layers of defence:</b>
     *
     * <p><b>Layer 1 — {@code VALUE_DEFAULT_TYPE}:</b>
     * The producer sends a {@code __TypeId__} header with every message (containing the
     * fully-qualified class name). But as a safety net, we also set a default target type.
     * This covers messages sent from tests or external systems that arrive WITHOUT headers.
     * Without it those messages fail with:
     * "No type information in headers and no default type provided".
     *
     * <p><b>Layer 2 — {@link ErrorHandlingDeserializer} wrapper:</b>
     * Without this wrapper, a single malformed/undeserializable message crashes the
     * entire listener thread — the container enters a failure loop and spams the logs.
     * With the wrapper, a bad record is caught cleanly and handed to the container's
     * error handler (retry then skip), so the consumer moves past it gracefully.
     *
     * @param groupId        Kafka consumer group ID
     * @param isolationLevel {@code "read_uncommitted"} or {@code "read_committed"}
     */
    private ConsumerFactory<String, Object> consumerFactory(String groupId, String isolationLevel) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);

        // Outer deserializer: ErrorHandlingDeserializer catches bad records before
        // they crash the listener thread and delegates to the real deserializer below.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);

        // Inner (real) deserializers used by ErrorHandlingDeserializer
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        // Trust all packages so JsonDeserializer can reconstruct MessageEvent.
        // In production, replace "*" with "com.vbforge.kafka06.model" for safety.
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        // Fallback default type: if a message has no __TypeId__ header, deserialize
        // it as MessageEvent. This handles messages from tests or external producers.
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.vbforge.kafka06.model.MessageEvent");

        return new DefaultKafkaConsumerFactory<>(props);
    }
}
