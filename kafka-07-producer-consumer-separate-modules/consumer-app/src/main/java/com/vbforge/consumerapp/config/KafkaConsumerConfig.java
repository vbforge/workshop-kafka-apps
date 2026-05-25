package com.vbforge.consumerapp.config;

import com.vbforge.consumerapp.model.MessageEvent;
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

@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, MessageEvent> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, MessageEvent.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.vbforge.*");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public DefaultErrorHandler errorHandler() {
        // Retry 3 times with 2 second delay, then skip the message
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                new FixedBackOff(2000L, 3L) // 2 seconds interval, 3 retries
        );

        // Log errors
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            System.err.println("Retry attempt " + deliveryAttempt + " for message: " + record.value());
        });

        return errorHandler;
    }

    /**
     * Concurrency in Kafka + Spring
     * In Spring Kafka, the concurrency setting controls how many consumer threads are created for a single listener container.
     * Each thread is assigned one or more partitions, so real parallelism depends on the number of topic partitions.
     * For example, if concurrency is 3 and the topic has 3 partitions, then messages can be consumed in parallel by 3 threads.
     * If the topic has fewer partitions, increasing concurrency gives no additional benefit.
     * */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MessageEvent> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, MessageEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // max 3 concurrent consumer threads; effective parallelism depends on number of topic partitions
        factory.setConcurrency(3); // creates 3 concurrent consumer threads (one per assigned partition) for parallel processing

        factory.getContainerProperties().setPollTimeout(3000);

        // Set custom error handler
        factory.setCommonErrorHandler(errorHandler());

        return factory;
    }

}
