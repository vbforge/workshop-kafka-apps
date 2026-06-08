package com.vbforge.case12.config;

import com.vbforge.case12.exception.NonRetryableException;
import com.vbforge.case12.model.GenericMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.ExponentialBackOff;
 
import java.util.Map;
 
// JUNIOR NOTE: This is THE central lesson of case-12.
//
// In cases 09-11, the DefaultErrorHandler was configured per-factory.
// If you had 5 topics → 5 factories → you'd copy-paste the error handler config 5 times.
// Any change to retry policy or non-retryable exceptions requires editing 5 places.
// That's a maintenance nightmare and a source of subtle inconsistencies between topics.
//
// The global error handler pattern:
//   1. Define DefaultErrorHandler as a SINGLE @Bean
//   2. Define a SINGLE DeadLetterPublishingRecoverer as a SINGLE @Bean
//   3. Inject the same errorHandler() into ALL container factories
//
// Result: one place to tune retry policy, one place to classify exceptions,
// one DLT for all topics (with routing by source topic embedded in kafka_dlt-* headers).
//
// The shared DLT destination function:
//   All three topics route to case-12-global.DLT.
//   The kafka_dlt-original-topic header tells the DLT consumer which topic the record
//   originally came from — enabling type-specific handling within one DLT consumer.
//
// When NOT to use a global handler:
//   If different topics need genuinely different retry policies (orders: 10s budget,
//   notifications: 2s budget), you need separate factories.
//   Global = uniformity. Per-factory = flexibility.
 
@Configuration
public class KafkaConfig {
 
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
 
    @Value("${kafka.consumer.ordersGroupID}")
    private String ordersGroupId;
 
    @Value("${kafka.consumer.paymentsGroupID}")
    private String paymentsGroupId;
 
    @Value("${kafka.consumer.notificationsGroupID}")
    private String notificationsGroupId;
 
    @Value("${kafka.consumer.dltGroupID}")
    private String dltGroupId;
 
    @Value("${kafka.consumer.autoOffsetReset}")
    private String autoOffsetReset;
 
    @Value("${kafka.topic.dlt}")
    private String dltTopic;
 
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
    // GLOBAL ERROR HANDLER — defined ONCE, reused by all factories
    // ===================================================================
 
    @Bean
    public DeadLetterPublishingRecoverer globalRecoverer() {
        // JUNIOR NOTE: Single recoverer routes ALL topics' failures to one shared DLT.
        // The destination function embeds the source topic identity in the routing key,
        // allowing the DLT consumer to branch on kafka_dlt-original-topic header.
        // If we needed per-topic DLTs, the destination function would be:
        //   (record, ex) -> new TopicPartition(record.topic() + ".DLT", 0)
        // which would create case-12-orders-topic.DLT, case-12-payments-topic.DLT, etc.
        return new DeadLetterPublishingRecoverer(
                kafkaTemplate(),
                (record, exception) -> {
                    org.slf4j.LoggerFactory.getLogger(KafkaConfig.class)
                            .error(">>> [GLOBAL-RECOVERER] Routing to DLT | sourceTopic={} offset={} ex={}",
                                    record.topic(), record.offset(),
                                    exception.getClass().getSimpleName());
                    return new TopicPartition(dltTopic, 0);
                }
        );
    }
 
    @Bean
    public DefaultErrorHandler globalErrorHandler() {
        // JUNIOR NOTE: This single bean is wired into all three container factories below.
        // Non-retryable exceptions registered here apply to ALL listeners automatically.
        // New listener added next sprint? Just use globalErrorHandler() in its factory —
        // it inherits the same retry policy and classification immediately.
        ExponentialBackOff backOff = new ExponentialBackOff(initialIntervalMs, multiplier);
        backOff.setMaxInterval(maxIntervalMs);
        backOff.setMaxElapsedTime(maxElapsedTimeMs);
 
        DefaultErrorHandler handler = new DefaultErrorHandler(globalRecoverer(), backOff);
        handler.addNotRetryableExceptions(NonRetryableException.class);
 
        handler.setRetryListeners((record, ex, attempt) ->
                org.slf4j.LoggerFactory.getLogger(KafkaConfig.class)
                        .warn(">>> [GLOBAL-HANDLER] attempt={} topic={} offset={} ex={}",
                                attempt, record.topic(), record.offset(),
                                ex.getClass().getSimpleName())
        );
 
        return handler;
    }
 
 
    // ===================================================================
    // SHARED CONSUMER FACTORY BUILDER
    // ===================================================================
 
    // JUNIOR NOTE: One helper method builds any consumer factory given a group ID.
    // All factories use the same deserializer config, same ack mode, same error handler.
    // This is the DRY version of what would otherwise be three identical factory methods.
    private ConcurrentKafkaListenerContainerFactory<String, Object> buildFactory(String groupId) {
        DefaultKafkaConsumerFactory<String, Object> consumerFactory =
                new DefaultKafkaConsumerFactory<>(Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, groupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset,
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class,
                        JacksonJsonDeserializer.TRUSTED_PACKAGES, "*",
                        JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, GenericMessage.class.getName()
                ));
 
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
 
        // JUNIOR NOTE: The key line — ALL factories share the same globalErrorHandler bean.
        // One change to globalErrorHandler() propagates to all three listeners automatically.
        factory.setCommonErrorHandler(globalErrorHandler());
 
        return factory;
    }
 
 
    // ===================================================================
    // THREE LISTENER FACTORIES — each using the GLOBAL error handler
    // ===================================================================
 
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> ordersContainerFactory() {
        return buildFactory(ordersGroupId);
    }
 
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> paymentsContainerFactory() {
        return buildFactory(paymentsGroupId);
    }
 
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> notificationsContainerFactory() {
        return buildFactory(notificationsGroupId);
    }
 
 
    // ===================================================================
    // DLT FACTORY — no error handler, as always
    // ===================================================================
 
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> dltContainerFactory() {
        DefaultKafkaConsumerFactory<String, Object> consumerFactory =
                new DefaultKafkaConsumerFactory<>(Map.of(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ConsumerConfig.GROUP_ID_CONFIG, dltGroupId,
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset,
                        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class,
                        JacksonJsonDeserializer.TRUSTED_PACKAGES, "*",
                        JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, GenericMessage.class.getName()
                ));
 
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        // No setCommonErrorHandler — intentional. DLT consumer uses default log-and-skip.
        return factory;
    }
 
}