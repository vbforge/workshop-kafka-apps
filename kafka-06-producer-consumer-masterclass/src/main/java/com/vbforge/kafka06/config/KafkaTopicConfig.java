package com.vbforge.kafka06.config;

import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * Programmatic topic creation via {@link KafkaAdmin}.
 *
 * <p>Why not rely on {@code KAFKA_AUTO_CREATE_TOPICS_ENABLE}?
 * Auto-creation gives you no control over partition count or retention.
 * Doing it here lets us set exactly what we need and is the production pattern.
 *
 * <p>Topics are created (or left unchanged if they already exist) once Spring
 * finishes dependency injection — that is what {@code @PostConstruct} does.
 */
@Configuration
public class KafkaTopicConfig {

    // ---- Topic names injected from application.yml ----
    @Value("${kafka.topics.general}")
    private String generalTopic;

    @Value("${kafka.topics.priority}")
    private String priorityTopic;

    @Value("${kafka.topics.transactional}")
    private String transactionalTopic;

    /**
     * KafkaAdmin is auto-configured by Spring Boot.
     * We inject it here to call createOrModifyTopics() manually.
     */
    @Autowired
    private KafkaAdmin kafkaAdmin;

    /**
     * Called automatically after Spring finishes building this bean.
     * Creates all three topics if they do not already exist.
     */
    @PostConstruct
    public void initTopics() {
        kafkaAdmin.createOrModifyTopics(
                buildGeneralTopic(),
                buildPriorityTopic(),
                buildTransactionalTopic()
        );
    }

    // ---- Private builders — each documents its own purpose ----

    /**
     * General topic: used for sync / async / keyed producer demos.
     * 2 partitions so we can demonstrate key-based partition routing.
     * Retention: 2 hours (matches our dev Docker setup).
     */
    private NewTopic buildGeneralTopic() {
        return TopicBuilder.name(generalTopic)
                .partitions(2)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "7200000") // 2 hours = 7 200 000 ms
                .build();
    }

    /**
     * Priority topic: used for the message-filtering demo.
     * 3 partitions so keys spread across them naturally.
     */
    private NewTopic buildPriorityTopic() {
        return TopicBuilder.name(priorityTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }

    /**
     * Transactional topic: used for the exactly-once / atomic send demo.
     * 3 partitions matches the consumer group size in that scenario.
     */
    private NewTopic buildTransactionalTopic() {
        return TopicBuilder.name(transactionalTopic)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
