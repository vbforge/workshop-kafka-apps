package com.vbforge.case15.config;

import com.vbforge.case15.model.EventMessage;
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
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Map;

// JUNIOR NOTE: The key difference from single-record listeners is ONE line:
//   factory.setBatchListener(true)
//
// With this set, the @KafkaListener method receives List<ConsumerRecord<...>>
// instead of a single ConsumerRecord. The entire batch from one poll() call
// arrives as a single method invocation.
//
// The ACK mode pairing matters:
//   BATCH ack mode → Spring commits all offsets at the end of the batch method automatically.
//   MANUAL_IMMEDIATE → you call ack.acknowledge() yourself to commit.
//
// In case-15, we use MANUAL_IMMEDIATE so you can see the commit happen explicitly
// and understand WHEN it fires (after your processing loop, not per-record).

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

    @Value("${kafka.consumer.max-poll-interval-ms}")
    private int maxPollIntervalMs;


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
    public ConsumerFactory<String, EventMessage> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class,
                JsonDeserializer.TRUSTED_PACKAGES, "*",
                JsonDeserializer.VALUE_DEFAULT_TYPE, EventMessage.class.getName(),
                // JUNIOR NOTE: max.poll.records limits how many records ONE poll() call returns.
                // This is the primary knob for batch size.
                // It does NOT affect how often poll() is called — only the upper bound per call.
                // If the topic has fewer records available than this limit, you get fewer records.
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords,
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs
        ));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, EventMessage> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, EventMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // JUNIOR NOTE: setBatchListener(true) is what activates batch mode.
        // Without this, Spring unwraps each record individually and calls your method
        // once per record. With this, your method receives the entire List from one poll().
        factory.setBatchListener(true);

        // JUNIOR NOTE: MANUAL_IMMEDIATE commits the offset immediately when you call
        // ack.acknowledge() — not at the end of the next poll cycle.
        // This is the most explicit and predictable option for batch processing:
        //   1. process the whole batch
        //   2. if processing succeeded, call ack.acknowledge()
        //   3. offset commits immediately
        // If you don't call ack.acknowledge(), the offset is NOT committed.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        return factory;
    }

}
