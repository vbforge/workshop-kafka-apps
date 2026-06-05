package com.vbforge.case11.config;

import com.vbforge.case11.exception.NonRetryableOrderException;
import com.vbforge.case11.model.OrderMessage;
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
 
// JUNIOR NOTE: The key addition in this case vs case-09/10:
// DeadLetterPublishingRecoverer replaces the default "log and skip" recovery action.
//
// How it works:
//   1. Message fails all retries (or is non-retryable)
//   2. DefaultErrorHandler calls DeadLetterPublishingRecoverer.accept(record, exception)
//   3. Recoverer publishes the ORIGINAL record to the DLT topic, enriched with headers:
//        kafka_dlt-original-topic       → case-11-topic
//        kafka_dlt-original-partition   → 0
//        kafka_dlt-original-offset      → 7
//        kafka_dlt-original-consumer-group → case-11-consumer-group
//        kafka_dlt-exception-fqcn       → com.vbforge.case11.exception.NonRetryableOrderException
//        kafka_dlt-exception-message    → "Invalid order amount: -50.0"
//        kafka_dlt-exception-stacktrace → full stack trace bytes
//   4. Main topic offset committed — consumer moves past the bad record
//   5. DLT consumer (separate @KafkaListener) receives the enriched record
//
// The destination function:
//   By default DeadLetterPublishingRecoverer routes to "<topic>.DLT" partition 0.
//   We provide a custom destination function to keep routing explicit and readable.
//   The function receives (ConsumerRecord, Exception) and returns a TopicPartition.
//
// Why DLT over "log and skip":
//   - The failed record is preserved with full context — orderId, amount, original exception
//   - Ops team can monitor DLT lag → alert on failures
//   - After a code fix, records can be replayed from the DLT back to the main topic
//   - Nothing is silently lost
 
@Configuration
public class KafkaConfig {
 
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
 
    @Value("${kafka.consumer.groupID}")
    private String consumerGroupId;
 
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
    // PRODUCER — used both for sending order messages AND by the recoverer
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
    // DEAD LETTER PUBLISHING RECOVERER
    // ===================================================================
 
    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer() {
        // JUNIOR NOTE: DeadLetterPublishingRecoverer takes a KafkaTemplate and a
        // destination function. The destination function decides WHICH topic+partition
        // receives the failed record.
        //
        // (record, exception) -> new TopicPartition(dltTopic, 0)
        //   record    → the original ConsumerRecord that failed
        //   exception → the exception thrown by the listener
        //   return    → TopicPartition where the failed record will be published
        //
        // We always route to partition 0 of the DLT. In production with multiple DLT
        // partitions you might route by record.partition() to preserve relative ordering
        // within DLT processing. For a single-partition DLT, 0 is correct.
        //
        // The recoverer automatically adds the kafka_dlt-* headers to the published record
        // so the DLT consumer has full context about where the message came from and why it failed.
        return new DeadLetterPublishingRecoverer(
                kafkaTemplate(),
                (record, exception) -> {
                    org.slf4j.LoggerFactory.getLogger(KafkaConfig.class)
                            .error(">>> [DLT-RECOVERER] Publishing to DLT | topic={} partition={} offset={} exception={}",
                                    record.topic(), record.partition(), record.offset(),
                                    exception.getClass().getSimpleName());
                    return new TopicPartition(dltTopic, 0);
                }
        );
    }
 
 
    // ===================================================================
    // ERROR HANDLER
    // ===================================================================
 
    @Bean
    public DefaultErrorHandler errorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(initialIntervalMs, multiplier);
        backOff.setMaxInterval(maxIntervalMs);
        backOff.setMaxElapsedTime(maxElapsedTimeMs);
 
        // JUNIOR NOTE: Pass the recoverer as the first argument to DefaultErrorHandler.
        // This replaces the default "log and skip" recovery with "publish to DLT + commit past".
        // The second argument is the BackOff policy — same as before.
        DefaultErrorHandler handler = new DefaultErrorHandler(deadLetterPublishingRecoverer(), backOff);
 
        handler.addNotRetryableExceptions(NonRetryableOrderException.class);
 
        handler.setRetryListeners((record, ex, deliveryAttempt) ->
                org.slf4j.LoggerFactory.getLogger(KafkaConfig.class)
                        .warn(">>> [ERROR-HANDLER] attempt={} offset={} ex={}",
                                deliveryAttempt, record.offset(), ex.getClass().getSimpleName())
        );
 
        return handler;
    }
 
 
    // ===================================================================
    // CONSUMER — main topic
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
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, OrderMessage.class.getName()
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
 
 
    // ===================================================================
    // DLT CONSUMER FACTORY — separate group, no error handler needed
    // ===================================================================
 
    // JUNIOR NOTE: The DLT consumer uses a DIFFERENT consumer group (dltGroupId)
    // and a DIFFERENT container factory (dltContainerFactory).
    // Why separate factory:
    //   1. Different group.id — DLT consumer must not share offsets with main consumer
    //   2. No error handler — if DLT processing fails we DO NOT want to publish to a DLT-of-DLT
    //      (infinite recursion). DLT consumer failures just log and move on.
    //   3. Potentially different concurrency, ack mode, or deserialization config.
    //
    // The DLT topic holds raw bytes of the original value — the JacksonJsonDeserializer
    // still works because DeadLetterPublishingRecoverer copies the original record's
    // value bytes unchanged into the DLT record.
 
    @Bean
    public ConsumerFactory<String, Object> dltConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, dltGroupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class,
                JacksonJsonDeserializer.TRUSTED_PACKAGES, "*",
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, OrderMessage.class.getName()
        ));
    }
 
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> dltContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(dltConsumerFactory());
        factory.setConcurrency(1);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        // JUNIOR NOTE: Intentionally NO setCommonErrorHandler() here.
        // DLT consumer uses Spring's default minimal handler — logs and moves on.
        // This prevents DLT → DLT-of-DLT infinite loops.
        return factory;
    }
 
}