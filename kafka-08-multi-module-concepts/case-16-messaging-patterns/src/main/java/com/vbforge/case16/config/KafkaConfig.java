package com.vbforge.case16.config;

import com.vbforge.case16.model.RequestMessage;
import com.vbforge.case16.model.ReplyMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.support.serializer.JacksonJsonDeserializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;

import java.util.Map;

// JUNIOR NOTE: This case has TWO distinct consumer factories:
//
//   1. listenerContainerFactory  — used by the SERVER-SIDE @KafkaListener that
//      receives requests, processes them, and sends replies. It deserializes
//      RequestMessage values.
//
//   2. replyListenerContainerFactory — used internally by ReplyingKafkaTemplate
//      to listen on the reply topic and correlate responses back to waiting
//      sendAndReceive() calls. It deserializes ReplyMessage values.
//
// ReplyingKafkaTemplate is Spring Kafka's built-in RPC abstraction.
// Under the hood it:
//   a) adds a KafkaHeaders.REPLY_TOPIC header to every outgoing request
//   b) adds a KafkaHeaders.CORRELATION_ID header (UUID bytes)
//   c) starts a background listener on the reply topic
//   d) when a reply arrives, matches it to the pending CompletableFuture
//      using the correlation ID header, and completes the future
//
// You never manage correlation IDs manually — the template does it.

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.groupID}")
    private String consumerGroupId;

    @Value("${kafka.consumer.reply-groupID}")
    private String replyGroupId;

    @Value("${kafka.topics.reply}")
    private String replyTopic;


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
    // RPC INFRASTRUCTURE — ReplyingKafkaTemplate
    // ===================================================================

    // JUNIOR NOTE: ReplyingKafkaTemplate<K, V, R> wraps KafkaTemplate and adds
    // the request-reply pattern on top. Type parameters:
    //   K = key type (String)
    //   V = request value type (Object — we send RequestMessage)
    //   R = reply value type (ReplyMessage — what we expect back)
    //
    // It needs its own dedicated listener container on the reply topic so it
    // can intercept replies before they reach any @KafkaListener. The container
    // is NOT annotated — it's wired directly into the template.
    @Bean
    public ReplyingKafkaTemplate<String, Object, ReplyMessage> replyingKafkaTemplate(
            ProducerFactory<String, Object> pf,
            ConcurrentMessageListenerContainer<String, ReplyMessage> replyContainer) {
        return new ReplyingKafkaTemplate<>(pf, replyContainer);
    }

    @Bean
    public ConcurrentMessageListenerContainer<String, ReplyMessage> replyContainer(
            ConcurrentKafkaListenerContainerFactory<String, ReplyMessage> replyListenerContainerFactory) {
        // JUNIOR NOTE: This container must listen on the exact reply topic.
        // ReplyingKafkaTemplate will inject itself as the listener, consuming
        // any message that arrives on this topic and matching it to a pending request.
        ConcurrentMessageListenerContainer<String, ReplyMessage> container =
                replyListenerContainerFactory.createContainer(replyTopic);
        container.getContainerProperties().setGroupId(replyGroupId);
        return container;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, ReplyMessage> replyListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, ReplyMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(replyConsumerFactory());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, ReplyMessage> replyConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, replyGroupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class,
                JacksonJsonDeserializer.TRUSTED_PACKAGES, "*",
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, ReplyMessage.class.getName()
        ));
    }


    // ===================================================================
    // SERVER-SIDE CONSUMER — receives requests, produces replies
    // ===================================================================

    // JUNIOR NOTE: This is the "server" listener factory — it receives RequestMessage
    // and needs the reply container property set so Spring Kafka knows to call
    // @SendTo on the listener method. setReplyTemplate() wires the producer
    // that will send the reply back.
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RequestMessage> listenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, RequestMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(requestConsumerFactory());
        factory.setReplyTemplate(kafkaTemplate());
        return factory;
    }

    @Bean
    public ConsumerFactory<String, RequestMessage> requestConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JacksonJsonDeserializer.class,
                JacksonJsonDeserializer.TRUSTED_PACKAGES, "*",
                JacksonJsonDeserializer.VALUE_DEFAULT_TYPE, RequestMessage.class.getName()
        ));
    }


    // ===================================================================
    // ROUTED MESSAGES CONSUMER — for header-based routing demo
    // ===================================================================

    // JUNIOR NOTE: This factory is used for the header-based routing listener.
    // It deserializes to Object (we'll cast after inspection) so a single listener
    // can receive messages from multiple topics with a RecordFilterStrategy.
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RequestMessage> routedListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, RequestMessage> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(requestConsumerFactory());

        // JUNIOR NOTE: RecordFilterStrategy lets you drop messages inside the container
        // BEFORE they reach your @KafkaListener method. Return true = filter out (skip).
        // This is useful when a single listener subscribes to multiple topics but you
        // only want to process messages that pass a certain condition — e.g. only
        // messages with a specific header value.
        //
        // Here we demonstrate it but set the filter to always pass (return false),
        // because filtering in the header-routing listener is shown via the `priority`
        // header in the @KafkaListener method body itself.
        factory.setRecordFilterStrategy(record -> false);
        return factory;
    }

}
