package com.vbforge.case13.config;

import com.vbforge.case13.model.OrderMessage;
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
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.Map;

// JUNIOR NOTE: This is the most important config file in case-13.
// Three things make Kafka transactions work — all three must be present:
//
// 1. TRANSACTIONAL PRODUCER
//    ProducerConfig.TRANSACTIONAL_ID_CONFIG must be set.
//    This upgrades the producer from "at-most-once" or "at-least-once" to
//    "exactly-once" by enabling the transactional protocol on the broker side.
//    The broker assigns a Producer ID (PID) and epoch to this transactional ID.
//    On restart, the producer "fences" any zombie instance with the same transactional ID.
//
// 2. IDEMPOTENT PRODUCER
//    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG = true
//    Idempotence is a PREREQUISITE for transactions — you cannot have transactions
//    without idempotence. It ensures that retried sends don't produce duplicates
//    by tracking sequence numbers per (PID, partition).
//    Setting TRANSACTIONAL_ID_CONFIG automatically enables idempotence.
//
// 3. READ_COMMITTED CONSUMER
//    ConsumerConfig.ISOLATION_LEVEL_CONFIG = "read_committed"
//    Without this, the consumer reads records from in-flight (uncommitted) transactions.
//    Those records may disappear on rollback, breaking the exactly-once guarantee.
//    read_committed makes the consumer only see records from fully committed transactions.
//
// All three together = exactly-once semantics (EOS).
// Missing any one of them and you're back to at-least-once at best.

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.groupID}")
    private String consumerGroupId;

    @Value("${kafka.consumer.isolationLevel}")
    private String isolationLevel;

    @Value("${kafka.consumer.autoOffsetReset}")
    private String autoOffsetReset;

    @Value("${kafka.producer.transactionalId}")
    private String transactionalId;


    // ===================================================================
    // TRANSACTIONAL PRODUCER
    // ===================================================================

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        // JUNIOR NOTE: DefaultKafkaProducerFactory must be used (not a plain Map factory)
        // because it has special support for transactions — it manages the
        // producer-per-thread pool needed for @Transactional to work correctly.
        // When transactional.id is set, Spring creates a new producer instance per
        // logical transaction to properly support concurrent @Transactional methods.
        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JacksonJsonSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all",
                // JUNIOR NOTE: acks=all is required for exactly-once.
                // acks=1 only waits for the leader to acknowledge — if the leader
                // crashes before replication, the record is lost despite being "committed".
                // acks=all waits for all in-sync replicas (ISR) — the record survives
                // any single broker failure after acknowledgement.
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                // JUNIOR NOTE: Idempotence prevents duplicate records on producer retry.
                // Each (ProducerID, partition) pair tracks sequence numbers.
                // If the broker receives the same sequence number twice, it deduplicates.
                // This means retries are safe — no duplicate charges, no duplicate orders.
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId
                // JUNIOR NOTE: Setting transactional.id is what "activates" the producer
                // for Kafka's transactional protocol. The broker registers this ID and
                // uses it to coordinate commit/abort across partitions.
        ));
        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // JUNIOR NOTE: KafkaTransactionManager bridges Spring's @Transactional mechanism
    // with Kafka's native transaction protocol.
    //
    // Without KafkaTransactionManager: calling kafkaTemplate.send() in a
    // @Transactional method does NOT actually wrap the sends in a Kafka transaction —
    // Spring's transaction interceptor runs, but Kafka doesn't know about it.
    //
    // With KafkaTransactionManager: when @Transactional fires, Spring calls
    // KafkaTransactionManager.doBegin() which calls kafkaTemplate.beginTransaction().
    // All sends in that method go through the same Kafka transaction.
    // On normal exit: Spring calls doCommit() → kafkaTemplate.commitTransaction().
    // On exception: Spring calls doRollback() → kafkaTemplate.abortTransaction().
    //
    // This is how "rollback on exception" works for Kafka sends.
    @Bean
    public KafkaTransactionManager<String, Object> kafkaTransactionManager() {
        return new KafkaTransactionManager<>(producerFactory());
    }


    // ===================================================================
    // CONSUMER — read_committed isolation
    // ===================================================================

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class,
                JacksonJsonDeserializer.TRUSTED_PACKAGES, "*",
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, OrderMessage.class.getName(),
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel
                // JUNIOR NOTE: "read_committed" is a string constant here.
                // Kafka's consumer config accepts: "read_committed" or "read_uncommitted".
                // Spring's IsolationLevel enum (READ_COMMITTED/READ_UNCOMMITTED) maps to these.
                // Using the string directly avoids an extra import for this demo.
        ));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(1);
        // JUNIOR NOTE: AckMode.RECORD commits one offset per processed record.
        // In a transactional consume-process-produce pattern, the consumer offset
        // commit should ideally happen INSIDE the same Kafka transaction (atomic
        // offset commit). That requires AckMode.MANUAL + kafkaTemplate.sendOffsetsToTransaction().
        // For this demo we use RECORD ack to keep the focus on producer transactions.
        // The THEORY doc explains the full consume-process-produce atomic pattern.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }

}
