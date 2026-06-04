package com.vbforge.case10.config;

import com.vbforge.case10.exception.NonRetryableException;
import com.vbforge.case10.model.TaskMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.Map;

// JUNIOR NOTE: The key difference from case-09: FixedBackOff → ExponentialBackOff.
//
// FixedBackOff (case-09):
//   Retry 1 → wait Xms → Retry 2 → wait Xms → Retry 3 → recovery
//   Every wait is the same. Good for: short, predictable transient failures.
//
// ExponentialBackOff (case-10):
//   Retry 1 → wait 500ms → Retry 2 → wait 1000ms → Retry 3 → wait 2000ms → ...
//   Each wait doubles. Good for: giving overloaded systems time to recover.
//   The multiplier prevents hammering a struggling downstream service with rapid retries.
//
// ExponentialBackOff parameters:
//   setInitialInterval(ms)   → first backoff duration
//   setMultiplier(double)    → factor by which each interval is multiplied
//   setMaxInterval(ms)       → cap — intervals never exceed this
//   setMaxElapsedTime(ms)    → total retry budget — stop retrying after this wall-clock time
//
// maxElapsedTime is the production-critical config:
//   It bounds TOTAL retry time regardless of how many individual retries that translates to.
//   With initialInterval=500, multiplier=2.0:
//     retry 1: 500ms wait   (total elapsed: 500ms)
//     retry 2: 1000ms wait  (total elapsed: 1500ms)
//     retry 3: 2000ms wait  (total elapsed: 3500ms)
//     retry 4: 4000ms wait  (total elapsed: 7500ms) ← maxInterval hit
//     retry 5: 4000ms wait  (total elapsed: 11500ms) ← EXCEEDS maxElapsedTime=10000ms → stop
//   So with these settings: ~4 retries before recovery.
//
// Why ExponentialBackOff over FixedBackOff in production:
//   If a DB goes down, immediate retries pile pressure on it when it comes back.
//   Exponential backoff gives the system breathing room — by retry 3 (2s+ wait),
//   most transient outages (connection pool exhaustion, brief network blips) have resolved.

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.groupID}")
    private String consumerGroupId;

    @Value("${kafka.consumer.autoOffsetReset}")
    private String autoOffsetReset;

    @Value("${kafka.retry.initialIntervalMs}")
    private long initialIntervalMs;

    @Value("${kafka.retry.multiplier}")
    private double multiplier;

    @Value("${kafka.retry.maxIntervalMs}")
    private long maxIntervalMs;

    @Value("${kafka.retry.maxElapsedTimeMs}")
    private long maxElapsedTimeMs;


    // ===================================================================
    // PRODUCER
    // ===================================================================

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class,
                ProducerConfig.ACKS_CONFIG, "1",
                ProducerConfig.RETRIES_CONFIG, 0
        ));
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }


    // ===================================================================
    // ERROR HANDLER — ExponentialBackOff
    // ===================================================================

    @Bean
    public DefaultErrorHandler errorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(initialIntervalMs, multiplier);
        backOff.setMaxInterval(maxIntervalMs);
        backOff.setMaxElapsedTime(maxElapsedTimeMs);

        DefaultErrorHandler handler = new DefaultErrorHandler(backOff);
        handler.addNotRetryableExceptions(NonRetryableException.class);

        // JUNIOR NOTE: Retry listener logs timing so you can observe backoff intervals
        // in the logs. The deliveryAttempt counter increments with each attempt.
        // In a real system this is where you'd emit retry metrics.
        handler.setRetryListeners((record, ex, deliveryAttempt) -> {
            long elapsed = System.currentTimeMillis(); // simplified — real impl tracks start time
            org.slf4j.LoggerFactory.getLogger(KafkaConfig.class)
                    .warn(">>> [BACKOFF] attempt={} offset={} exception={} | waiting for next retry...",
                            deliveryAttempt, record.offset(), ex.getClass().getSimpleName());
        });

        return handler;
    }


    // ===================================================================
    // CONSUMER
    // ===================================================================

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class,
                JacksonJsonDeserializer.TRUSTED_PACKAGES, "*",
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, TaskMessage.class.getName()
        ));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(errorHandler());
        return factory;
    }

}
