package com.vbforge.kafka.dashboard.config;

import com.vbforge.kafka.dashboard.model.DashboardEvent;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka configuration for the dashboard project.
 *
 * ROOT CAUSE OF THE BUG (zeroed stats):
 * The previous version configured JsonDeserializer programmatically using
 * setUseTypeHeaders(false). In Spring Kafka 3.x this is unreliable —
 * when the type cannot be resolved the deserializer silently returns null
 * instead of throwing. The AggregatorConsumer null-guard fired every time,
 * statsService.record() was never called, and all stat panels stayed at zero.
 *
 * THE FIX — two changes:
 *
 * 1. Configure JsonDeserializer via the ConsumerConfig properties map
 *    (JsonDeserializer.VALUE_DEFAULT_TYPE, TRUSTED_PACKAGES, USE_TYPE_INFO_HEADERS).
 *    This is the correct, version-stable approach.
 *
 * 2. Wrap with ErrorHandlingDeserializer so any future deserialization failure
 *    is explicit (logged as an error) instead of a silent null drop.
 *
 * AckMode contract — unchanged:
 *   aggregatorListenerContainerFactory → AckMode.BATCH
 *   auditListenerContainerFactory      → AckMode.MANUAL_IMMEDIATE
 */
@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${app.kafka.aggregator-group}")
    private String aggregatorGroup;

    @Value("${app.kafka.audit-group}")
    private String auditGroup;

    // ── Topic ────────────────────────────────────────────────────────────

    @Bean
    public NewTopic dashboardEventsTopic(@Value("${app.kafka.topic}") String topicName) {
        return TopicBuilder.name(topicName)
                .partitions(3)
                .replicas(1)
                .build();
    }

    // ── Base consumer config ──────────────────────────────────────────────

    /**
     * Builds the consumer property map shared by both factories.
     *
     * Why configure JsonDeserializer via properties instead of setters?
     * When you call new JsonDeserializer<>() and configure it programmatically,
     * Spring Kafka 3.x may re-instantiate the deserializer internally using
     * reflection — losing your programmatic configuration silently.
     * Properties-based configuration survives that re-instantiation.
     */
    private Map<String, Object> baseConsumerConfig(String groupId) {
        Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Key: plain String
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Value: ErrorHandlingDeserializer wraps JsonDeserializer.
        // On failure → DeserializationException logged explicitly, not a silent null.
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());

        // Deserialize directly into DashboardEvent — no type header lookup needed.
        // This is the key fix: explicit target type, header-independent.
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, DashboardEvent.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.vbforge.kafka.dashboard.model");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        return props;
    }

    // ── Aggregator — AckMode.BATCH ────────────────────────────────────────

    @Bean
    public ConsumerFactory<String, DashboardEvent> aggregatorConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(baseConsumerConfig(aggregatorGroup));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, DashboardEvent>
    aggregatorListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, DashboardEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(aggregatorConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.setConcurrency(1);
        return factory;
    }

    // ── Audit — AckMode.MANUAL_IMMEDIATE ─────────────────────────────────

    @Bean
    public ConsumerFactory<String, DashboardEvent> auditConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(baseConsumerConfig(auditGroup));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, DashboardEvent>
    auditListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, DashboardEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(auditConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(1);
        return factory;
    }
}

















