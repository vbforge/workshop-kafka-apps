package com.vbforge.kafka.bot.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;


/**
 * Kafka configuration.
 *
 * NOTE: ProducerFactory and KafkaTemplate are intentionally NOT defined here.
 * Spring Boot auto-configures them from application.yml (spring.kafka.producer.*).
 * Defining them manually here would create duplicate beans and override auto-config.
 *
 * We only define the topic bean — Spring Kafka's KafkaAdmin will create it on startup
 * if it doesn't already exist in the broker.
 */
@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic echoTopic(@Value("${app.kafka.topic}") String topicName){
        return TopicBuilder.name(topicName)
                .partitions(3)
                .replicas(1) //single broker in KRaft mode
                .build();
    }

}
