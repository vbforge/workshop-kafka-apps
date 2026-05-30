package com.vbforge.case02.config;

import com.vbforge.case02.model.MyMessageObject;
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

// JUNIOR NOTE: Same "all-or-nothing" manual config as case-01.
// Spring Boot 4.x autoconfiguration backs off once you declare your own KafkaTemplate bean.
// So we own the entire producer + consumer wiring here.
//
// One important addition for sync sends: ACKS config.
// ProducerConfig.ACKS_CONFIG controls when the broker considers a write "done":
//   "0"  — fire and forget (broker never replies)        ← not safe
//   "1"  — leader partition acknowledged                  ← default, some risk
//   "all" — all in-sync replicas acknowledged             ← safest, what sync send is meant for
//
// If ACKS="0", .get() would still unblock immediately (no real ACK waited),
// defeating the purpose of blocking. For this case we use "1" (single-node cluster),
// which is equivalent to "all" here since replication factor = 1.

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.groupID}")
    private String consumerGroupId;


    // ===================================================================
    // PRODUCER
    // ===================================================================

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        return new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class,

                // JUNIOR NOTE: ACKS_CONFIG = "1" means the leader partition must write
                // the message to its log before ACK-ing the producer.
                // With a single-node local Kafka (replication factor 1), "1" and "all" are identical.
                // In a real cluster you'd use "all" for full durability guarantees.
                ProducerConfig.ACKS_CONFIG, "1",

                // JUNIOR NOTE: REQUEST_TIMEOUT_MS_CONFIG is the Kafka-level timeout —
                // how long the producer waits for a single broker response attempt.
                // This is different from the application-level timeout we pass to .get(seconds).
                // Think of it as: Kafka's own internal deadline vs. our application's deadline.
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000,

                // JUNIOR NOTE: RETRIES_CONFIG = 0 is intentional for this learning case.
                // We want to see failures immediately, not have Kafka silently retry and mask them.
                // In production you'd typically set retries > 0 (or use idempotent producer in case-13).
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
        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
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
