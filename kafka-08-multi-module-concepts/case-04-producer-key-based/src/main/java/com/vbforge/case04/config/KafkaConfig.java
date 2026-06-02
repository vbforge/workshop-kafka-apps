package com.vbforge.case04.config;

import com.vbforge.case04.model.KeyedMessage;
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

import java.util.HashMap;
import java.util.Map;

// JUNIOR NOTE: This case introduces two producer factories — one for each partitioning strategy.
//
// defaultProducerFactory  → uses Kafka's built-in DefaultPartitioner (murmur2 hash)
// customProducerFactory   → uses our CustomPartitioner (region-based routing)
//
// Each factory gets its own KafkaTemplate bean. The service injects both and uses
// the appropriate one depending on which endpoint is called.
//
// Why two factories instead of one factory with a dynamic partitioner?
// Because ProducerConfig.PARTITIONER_CLASS_CONFIG is set at producer creation time.
// You can't swap the partitioner per-message on the same producer instance.
// If you need different partitioning strategies at runtime, you need separate producers.
//
// The topic has 3 partitions (created via topic config in docker) so the
// custom partitioner's region routing has meaningful distinct targets.

@Configuration
public class KafkaConfig {

    public static final String EARLIEST = "earliest";
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.groupID}")
    private String consumerGroupId;

    @Value("${kafka.producer.send-timeout-seconds}")
    private int sendTimeoutSeconds;


    //PRODUCER — Default partitioner (murmur2 hash of key)
    @Bean
    public ProducerFactory<String, Object> defaultProducerFactory() {
        return new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class,
                ProducerConfig.ACKS_CONFIG, "1",
                ProducerConfig.RETRIES_CONFIG, 0
                // JUNIOR NOTE: No PARTITIONER_CLASS_CONFIG here = Kafka uses DefaultPartitioner.
                // DefaultPartitioner: murmur2(keyBytes) % numPartitions.
                // Same key always → same partition (deterministic hash).
                // Different keys may or may not land on the same partition.
        ));
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        // JUNIOR NOTE: This is the "primary" KafkaTemplate bean — the one Spring
        // will inject when you just @Autowire KafkaTemplate without a qualifier.
        return new KafkaTemplate<>(defaultProducerFactory());
    }


    //PRODUCER — Custom partitioner (region-based routing)
    @Bean
    public ProducerFactory<String, Object> customProducerFactory() {
        // JUNIOR NOTE: Map.of() returns an immutable map. Since we're adding
        // PARTITIONER_CLASS_CONFIG on top of the base config, we use HashMap here
        // to build the map incrementally before passing it to the factory.
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);

        // JUNIOR NOTE: This is what activates the custom partitioner.
        // Kafka will instantiate CustomPartitioner via reflection, call configure(),
        // and then call partition() for every message this producer sends.
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean
    public KafkaTemplate<String, Object> customKafkaTemplate() {
        return new KafkaTemplate<>(customProducerFactory());
    }


    //CONSUMER
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class,
                JacksonJsonDeserializer.TRUSTED_PACKAGES, "*",
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, KeyedMessage.class.getName()
        ));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }



}

















