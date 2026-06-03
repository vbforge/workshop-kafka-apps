package com.vbforge.case06.config;

import com.vbforge.case06.model.WorkMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.Arrays;
import java.util.Map;


// JUNIOR NOTE: Notice what is MISSING from this config compared to cases 01-05:
// there is NO ConsumerFactory bean and NO ConcurrentKafkaListenerContainerFactory bean.
//
// In manual poll mode, we create the KafkaConsumer directly inside ManualPollConsumerService
// using raw Kafka client API (org.apache.kafka.clients.consumer.KafkaConsumer).
// Spring's listener container infrastructure is completely bypassed.
//
// This is intentional and educational:
//   case-05 = Spring manages everything (@KafkaListener, container, poll loop, commits)
//   case-06 = You manage everything (KafkaConsumer, poll loop, commits, pause/resume)
//
// We still use Spring for the producer (KafkaTemplate) because the producer side
// isn't the focus of this case — we just need it to feed messages.
//
// The consumer properties map IS defined here as a @Bean so the service can
// inject it cleanly rather than building the map inline. Keeps concerns separate.

@Slf4j
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


    //PRODUCER - (minimal config)
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


    //CONSUMER PROPERTIES - injected into ManualPollConsumerService

    // JUNIOR NOTE: We return a Map<String, Object> instead of a ConsumerFactory bean.
    // ManualPollConsumerService uses this map to build a raw KafkaConsumer directly:
    //   new KafkaConsumer<>(consumerProperties())
    //
    // Key config choices here:
    //
    //   ENABLE_AUTO_COMMIT_CONFIG = false
    //     Mandatory for manual commit. If auto-commit is on, Kafka commits on a timer
    //     regardless of your manual commitSync() calls — your manual commits would be
    //     redundant at best, confusing at worst.
    //
    //   MAX_POLL_RECORDS_CONFIG = maxPollRecords (5)
    //     Each poll() returns at most this many records. Small value = easy to observe
    //     batch boundaries in logs. In production you'd tune this for throughput
    //     (higher = fewer network round-trips per unit of work).
    //
    //   MAX_POLL_INTERVAL_MS_CONFIG = 30_000
    //     If your poll loop doesn't call poll() within this window, Kafka assumes
    //     the consumer is dead and triggers a rebalance. With manual poll you MUST
    //     ensure your processing + commit time per batch stays under this limit.
    //     We set 30s (much higher than the default 5min isn't needed here, but
    //     it's good to show you know this config exists).

    @Bean
    public Map<String, Object> consumerProperties(){
        return Map.of(

                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class,
                JacksonJsonDeserializer.TRUSTED_PACKAGES, "*",
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, WorkMessage.class.getName(),
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords,
                ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 30_000

        );
    }

}





























