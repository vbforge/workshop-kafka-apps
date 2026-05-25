package com.vbforge.kafka06.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Producer configuration: creates two {@link KafkaTemplate} beans.
 *
 * <ul>
 *   <li>{@code kafkaTemplate}             — standard producer (fire-and-forget / sync)</li>
 *   <li>{@code transactionalKafkaTemplate} — transactional producer (exactly-once)</li>
 * </ul>
 *
 * <p>Why two templates?
 * A transactional producer must be initialized with a fixed {@code transactional.id}.
 * Mixing transactional and non-transactional sends on the same template causes errors,
 * so we keep them separate.
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // =========================================================
    // BEAN 1: Standard KafkaTemplate (non-transactional)
    // =========================================================

    /**
     * The default template used for all sync and async sends.
     * Named "kafkaTemplate" — Spring auto-wires this by type in most cases,
     * but we use {@code @Qualifier} in services to be explicit.
     */
    @Bean("kafkaTemplate")
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(standardProducerFactory());
    }

    private ProducerFactory<String, Object> standardProducerFactory() {
        return new DefaultKafkaProducerFactory<>(baseProducerProps());
    }

    // =========================================================
    // BEAN 2: Transactional KafkaTemplate
    // =========================================================

    /**
     * Template for transactional sends.
     *
     * <p>Key difference: {@code TRANSACTIONAL_ID_CONFIG} is set.
     * This tells the Kafka broker that messages from this producer
     * should be written atomically — either all succeed or none are visible.
     *
     * <p>The broker uses "tx-masterclass-" as a prefix and appends a
     * sequence number for each producer instance.
     */
    @Bean("transactionalKafkaTemplate")
    public KafkaTemplate<String, Object> transactionalKafkaTemplate() {
        return new KafkaTemplate<>(transactionalProducerFactory());
    }

    private ProducerFactory<String, Object> transactionalProducerFactory() {
        Map<String, Object> props = baseProducerProps();

        // Setting this enables the Kafka transactional API on this factory.
        // All KafkaTemplates sharing this factory will require transactions.
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-masterclass-");

        return new DefaultKafkaProducerFactory<>(props);
    }

    // =========================================================
    // Shared base configuration
    // =========================================================

    /**
     * Properties common to both producers.
     *
     * <ul>
     *   <li>Key   → {@link StringSerializer}  — topic keys are plain strings</li>
     *   <li>Value → {@link JsonSerializer}     — message objects serialized to JSON</li>
     * </ul>
     *
     * <p><b>Why we do NOT disable type headers:</b>
     * By default, {@link JsonSerializer} adds a {@code __TypeId__} header to every message.
     * This header contains the fully-qualified class name (e.g. {@code com.vbforge.kafka06.model.MessageEvent}).
     * The consumer-side {@link org.springframework.kafka.support.serializer.JsonDeserializer}
     * reads that header to know which Java class to deserialize into.
     * Without it, deserialization fails with:
     * <pre>IllegalStateException: No type information in headers and no default type provided</pre>
     * So: always keep type headers ON when producer and consumer share the same class model.
     * The only reason to turn them off is cross-language messaging (e.g. Java → Python),
     * in which case you must configure {@code VALUE_DEFAULT_TYPE} on the consumer side instead.
     */
    private Map<String, Object> baseProducerProps() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        // JsonSerializer.ADD_TYPE_INFO_HEADERS defaults to TRUE — we rely on this.
        // Do not set it to false here. See Javadoc above for the full explanation.
        return props;
    }
}
