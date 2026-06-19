package com.vbforge.case14.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

// JUNIOR NOTE: Manual Kafka configuration — the rule of this project.
// Spring Boot 4.x autoconfiguration backs off once you declare any Kafka bean manually.
// Declaring ProducerFactory or ConsumerFactory here is enough to deactivate
// auto-config for the whole module. We own every setting explicitly.
//
// case-14 focus: the Kafka plumbing is deliberately minimal.
// The interesting code is in the validator, the DTO, and the error handler.
// Kafka is just the transport — we receive a JSON DTO over it and validate it.

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.groupID}")
    private String consumerGroupId;

    @Value("${kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;

    // ===================================================================
    // PRODUCER
    // ===================================================================

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class,
                ProducerConfig.ACKS_CONFIG, "1",
                ProducerConfig.RETRIES_CONFIG, 0
        ));
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }


    // ===================================================================
    // CONSUMER
    // ===================================================================

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        // JUNIOR NOTE: VALUE_DEFAULT_TYPE is intentionally Object.class here.
        // The consumer service receives raw Map (deserialized from JSON) and manually
        // maps it to the DTO before validating. This is a deliberate demonstration choice:
        // it shows validation as an explicit application-layer step, not a silent Kafka-layer step.
        // In production you'd often configure the exact target type and use @Valid directly.
        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class,
                JsonDeserializer.TRUSTED_PACKAGES, "*",
                JsonDeserializer.VALUE_DEFAULT_TYPE, Object.class.getName()
        ));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // JUNIOR NOTE: MANUAL ack mode is paired with manual commitSync in the listener.
        // This ensures we only commit AFTER validation succeeds — a failed validation
        // does NOT advance the offset. The bad message is re-consumed on restart
        // (or you could seek past it manually, case-08 style).
        factory.getContainerProperties().setAckMode(
                org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE
        );
        return factory;
    }

}
