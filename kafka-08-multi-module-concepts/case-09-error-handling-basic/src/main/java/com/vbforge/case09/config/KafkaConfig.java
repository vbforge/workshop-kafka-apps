package com.vbforge.case09.config;

import com.vbforge.case09.exception.FatalProcessingException;
import com.vbforge.case09.model.TaskMessage;
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
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

// JUNIOR NOTE: This is the first case where we configure DefaultErrorHandler explicitly.
//
// DefaultErrorHandler is Spring Kafka's built-in error handler (since Spring Kafka 2.8,
// replacing the older SeekToCurrentErrorHandler). It handles what happens when your
// @KafkaListener method throws an exception:
//
//   1. Retry the failed record up to (maxAttempts - 1) times
//   2. If all retries exhausted → call the recovery action (by default: log and commit past it)
//
// The FixedBackOff(interval, maxAttempts - 1) arguments:
//   interval    → milliseconds between retries (0 here = immediate, no sleep)
//   maxAttempts - 1 → number of RETRY attempts (not total attempts)
//                     FixedBackOff counts retries, not total attempts:
//                     FixedBackOff(0, 2) = 2 retries = 3 total delivery attempts
//
// Non-retryable exceptions:
//   addNotRetryableExceptions(FatalProcessingException.class) tells the handler:
//   "if this exception type is thrown, don't retry at all — go straight to recovery."
//   NullPointerException is also added to demonstrate that JVM errors can be classified.
//
// Why back-and-forth between retryable and non-retryable matters:
//   Without classification, EVERY exception gets retried maxAttempts times.
//   A FatalProcessingException (bad data) would waste 3 retry attempts burning
//   thread time and potentially delaying other messages on the same partition.
//   Classification makes error handling precise and efficient.

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.groupID}")
    private String consumerGroupId;

    @Value("${kafka.consumer.autoOffsetReset}")
    private String autoOffsetReset;

    @Value("${kafka.error.maxAttempts}")
    private int maxAttempts;


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
    // ERROR HANDLER
    // ===================================================================

    @Bean
    public DefaultErrorHandler errorHandler() {
        // JUNIOR NOTE: FixedBackOff(intervalMs, maxRetries)
        // intervalMs = 0 → retry immediately with no sleep between attempts
        // maxRetries = maxAttempts - 1 → because FixedBackOff counts retries, not total attempts
        // So maxAttempts=3 → FixedBackOff(0, 2) → original + 2 retries = 3 total
        // Case-10 replaces this with ExponentialBackOff for realistic retry delays.
        FixedBackOff backOff = new FixedBackOff(0L, maxAttempts - 1);

        DefaultErrorHandler handler = new DefaultErrorHandler(backOff);

        // JUNIOR NOTE: addNotRetryableExceptions registers exception types that should
        // skip retries entirely and go straight to the recovery action.
        // The handler checks if the thrown exception IS-A any of these types
        // (including subclasses) before deciding to retry.
        handler.addNotRetryableExceptions(FatalProcessingException.class);
        handler.addNotRetryableExceptions(NullPointerException.class);

        // JUNIOR NOTE: setRetryListeners allows you to hook into the retry lifecycle.
        // Three callback points:
        //   onNextAttempt(record, ex, deliveryAttempt) → fires before each retry attempt
        //   recovered(record, ex)                      → fires when recovery action runs
        //                                                (all retries exhausted or non-retryable)
        //   recoveryFailed(record, original, failure)  → fires if recovery itself fails
        handler.setRetryListeners((record, ex, deliveryAttempt) ->
                org.slf4j.LoggerFactory.getLogger(KafkaConfig.class)
                        .warn(">>> [ERROR-HANDLER] Retry attempt #{} for offset={} exception={}",
                                deliveryAttempt, record.offset(), ex.getClass().getSimpleName())
        );

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

        // JUNIOR NOTE: setCommonErrorHandler() wires our DefaultErrorHandler into
        // the listener container. Every exception thrown by @KafkaListener methods
        // managed by this factory will be intercepted by this handler.
        // Without this line, Spring uses its own default error handler which just
        // logs the exception — no retries, no recovery action classification.
        factory.setCommonErrorHandler(errorHandler());

        return factory;
    }

}
