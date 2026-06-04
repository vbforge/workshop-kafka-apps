package com.vbforge.case07.config;

import com.vbforge.case07.model.OrderEvent;
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
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.Map;


// JUNIOR NOTE: The key structural difference in this config vs previous cases:
// THREE separate ConcurrentKafkaListenerContainerFactory beans, one per group.
//
// Why separate factories and not just three @KafkaListener annotations with different groupId?
// Because each factory creates consumers with a DIFFERENT group.id baked into the
// ConsumerFactory config. If you used the same factory for all three listeners,
// they'd all share the same group.id and compete for partitions (one event → one consumer)
// instead of fan-out (one event → all three consumers).
//
// Factory naming convention:
//   analyticsContainerFactory  → group: case-07-analytics-group
//   auditContainerFactory      → group: case-07-audit-group
//   notifyContainerFactory     → group: case-07-notify-group
//
// Each @KafkaListener in the service references its own factory by name via
// containerFactory = "analyticsContainerFactory" etc.

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.analyticsGroupId}")
    private String analyticsGroupId;

    @Value("${kafka.consumer.auditGroupId}")
    private String auditGroupId;

    @Value("${kafka.consumer.notifyGroupId}")
    private String notifyGroupId;

    @Value("${kafka.consumer.concurrency}")
    private int concurrency;

    @Value("${kafka.consumer.autoOffsetReset}")
    private String autoOffsetReset;


    // producer
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


    //consumer (shared consumer factory builder)

    // JUNIOR NOTE: Helper method — builds consumer config for a given group.id.
    // Called three times below, once per group. Avoids repeating the full config map.
    // This is just a private Java method, not a Spring bean.
    private ConsumerFactory<String, Object> buildConsumerFactory(String groupId) {
        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class,
                JacksonJsonDeserializer.TRUSTED_PACKAGES, "*",
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, OrderEvent.class.getName()
        ));
    }

    private ConcurrentKafkaListenerContainerFactory<String, Object> buildFactory(String groupId) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(buildConsumerFactory(groupId));
        factory.setConcurrency(concurrency);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        return factory;
    }

    // 3 LISTENER CONTAINER FACTORIES — one per consumer group
    //(here are the beans)

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> analyticsContainerFactory() {
        return buildFactory(analyticsGroupId);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> auditContainerFactory() {
        return buildFactory(auditGroupId);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> notifyContainerFactory() {
        return buildFactory(notifyGroupId);
    }



}

























