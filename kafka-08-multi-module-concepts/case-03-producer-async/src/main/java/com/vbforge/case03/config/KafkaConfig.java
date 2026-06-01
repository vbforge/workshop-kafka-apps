package com.vbforge.case03.config;

import com.vbforge.case03.model.MyMessageObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.Map;

// JUNIOR NOTE: Manual Kafka config — same pattern as case-01 and case-02.
// Spring Boot 4.x autoconfiguration backs off once you declare your own KafkaTemplate bean.
// So we own all producer + consumer wiring here.
//
// Producer config for async sends:
//   ACKS_CONFIG = "1"  → leader must write before ACK. Even for async, you want the
//                         callback to fire only after the broker actually committed,
//                         not just after it hit the network buffer.
//
//   RETRIES_CONFIG = 0 → Same as case-02: fail fast for observability.
//                         In production with async, you'd set retries > 0 or use
//                         idempotent producer (case-13) so Kafka retries internally
//                         without you managing it.
//
//   LINGER_MS_CONFIG = 0 → Kafka sends messages immediately, no micro-batching delay.
//                           In high-throughput async scenarios you'd raise this (e.g. 5ms)
//                           to allow Kafka to batch multiple messages per request.
//                           For this learning case, 0 means you see callbacks instantly.
//
//   BATCH_SIZE_CONFIG = 16384 → Default batch size (16KB). Fine for our use case.
//                               Mentioned here so you know it exists — it pairs with LINGER_MS.

@Configuration
public class KafkaConfig {

    public static final String EARLIEST = "earliest";
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.groupID}")
    private String consumerGroupId;

    //producer

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class,
                ProducerConfig.ACKS_CONFIG, "1",
                ProducerConfig.RETRIES_CONFIG, "0",

                // JUNIOR NOTE: LINGER_MS_CONFIG is how long the Kafka client waits to
                // accumulate more messages before sending a batch to the broker.
                // 0 = send immediately (good for demos, low-latency scenarios).
                // Higher values (5-100ms) = better throughput in fire-and-forget pipelines
                // because more messages get batched per network round-trip.
                ProducerConfig.LINGER_MS_CONFIG, 0,

                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000
        ));
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }


    //consumer
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class,
                JacksonJsonDeserializer.TRUSTED_PACKAGES, "*",
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, MyMessageObject.class.getName()
        ));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }


}



















