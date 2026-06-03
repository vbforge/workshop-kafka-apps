package com.vbforge.case05.config;

import com.vbforge.case05.model.WorkMessage;
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

// JUNIOR NOTE: The central addition in this config vs previous cases is concurrency.
//
// factory.setConcurrency(concurrency) tells Spring Kafka how many consumer threads to start.
// Internally, Spring creates N ConcurrentMessageListenerContainer instances, each running
// its own Kafka consumer. All N consumers join the same consumer group and Kafka distributes
// partitions across them.
//
// The auto-commit behavior is controlled by ContainerProperties.AckMode:
//
//   BATCH (default) → Spring commits offsets after each poll() batch is fully processed.
//                     "Processed" means your @KafkaListener method returned without exception.
//                     This is the standard "auto-commit" most people mean — you don't call
//                     anything manually, offsets advance automatically per batch.
//
//   RECORD          → Commit after every single record. Safer but slower.
//
//   MANUAL          → You call Acknowledgment.acknowledge() yourself. case-06 territory.
//
// We set AckMode.BATCH explicitly here so the config is readable and intentional,
// even though it's the default. Explicit > implicit in learning code.


@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.groupID}")
    private String consumerGroupId;

    @Value("${kafka.consumer.concurrency}")
    private int concurrency;

    @Value("${kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;


    //---->PRODUCER - minimal, just enough to feed the consumer demo

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


    //---->CONSUMER

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset,

                // JUNIOR NOTE: enable.auto.commit = false is required when Spring Kafka
                // manages commits via AckMode. If you leave it true, Kafka's own background
                // thread commits offsets on a timer (auto.commit.interval.ms) independently
                // of whether your listener actually processed the message.
                // That means a crash between Kafka's auto-commit and your listener finishing
                // causes silent message loss — the offset is committed but the message wasn't processed.
                // Setting it to false hands commit control entirely to Spring Kafka's AckMode.
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,

                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class,
                JacksonJsonDeserializer.TRUSTED_PACKAGES, "*",
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, WorkMessage.class.getName()
        ));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // JUNIOR NOTE: This is the key line for case-05.
        // concurrency=3 with a 3-partition topic → each thread gets exactly one partition.
        // Watch the thread names in the logs: they'll show as
        //   case-05-consumer-group-0-C-1, case-05-consumer-group-0-C-2, case-05-consumer-group-0-C-3
        // Each "C-N" suffix is a separate consumer thread, proving parallel consumption
        factory.setConcurrency(concurrency);

        // JUNIOR NOTE: AckMode.BATCH = commit offsets after each poll batch is processed.
        // This is the "auto-commit" most production codebases use with Spring Kafka.
        // It's not Kafka's native auto-commit (which is time-based) — it's Spring's
        // batch-boundary commit, which is safer because it's tied to processing completion.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);

        return factory;
    }




}























