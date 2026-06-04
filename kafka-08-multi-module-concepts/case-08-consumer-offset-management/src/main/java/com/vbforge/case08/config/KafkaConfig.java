package com.vbforge.case08.config;

import com.vbforge.case08.model.EventMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.Map;

// JUNIOR NOTE: Same architecture as case-06 — manual poll, raw KafkaConsumer.
// No ConsumerFactory, no ConcurrentKafkaListenerContainerFactory.
// The consumer is created directly in OffsetManagementConsumerService using the
// consumerProperties() map injected as a bean.
//
// case-08 focuses on OFFSET OPERATIONS specifically:
//   seekToBeginning()  → rewind all assigned partitions to offset 0
//   seekToEnd()        → skip to the latest offset (skip all unread messages)
//   seekToOffset()     → go to a specific partition + offset
//   commitSync()       → manual commit after deliberate processing
//
// Understanding positions vs committed offsets is the core lesson:
//   position(partition)          → where the consumer will fetch NEXT
//   committed offset(partition)  → where it will resume after a RESTART
// These are different numbers — position advances as you poll,
// committed only advances when you call commitSync/Async.

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.groupID}")
    private String consumerGroupId;

    @Value("${kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;

    @Value("${kafka.consumer.max-poll-records}")
    private int maxPollRecords;


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
    // CONSUMER PROPERTIES — used to build raw KafkaConsumer
    // ===================================================================

    @Bean
    public Map<String, Object> consumerProperties() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class,
                JacksonJsonDeserializer.TRUSTED_PACKAGES, "*",
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, EventMessage.class.getName(),
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords,
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60_000
        );
    }

}
