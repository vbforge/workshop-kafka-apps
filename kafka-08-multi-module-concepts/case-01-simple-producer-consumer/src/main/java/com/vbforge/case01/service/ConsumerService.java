package com.vbforge.case01.service;

import com.vbforge.case01.model.Message;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ConsumerService {

    @KafkaListener(
            topics = "${kafka.topic.test}",
            groupId = "${kafka.consumer.groupID}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(Message message) {
        log.info(">>> CONSUMER TRIGGERED <<<");
        log.info("\n===== Message Received =====");
        log.info("  ID:        {}", message.getId());
        log.info("  Content:   {}", message.getContent());
        log.info("  Timestamp: {}", message.getTimestamp());
        log.info("============================\n");
    }

    // JUNIOR NOTE: String consumer uses a SEPARATE topic and a SEPARATE consumer group
    // (and its own containerFactory with StringDeserializer).
    // If we put both on the same topic, the JSON consumer would get the plain string
    // and fail to deserialize it — and vice versa. Separate topics = no conflict.
    @KafkaListener(
            topics = "${kafka.topic.test-string}",
            groupId = "${kafka.consumer.groupID}-string",
            containerFactory = "kafkaListenerContainerFactoryString"
    )
    public void consumeString(String message) {
        log.info(">>> CONSUMER STRING TRIGGERED <<<");
        log.info("\n===== String Message Received =====");
        log.info("  Content: {}", message);
        log.info("===================================\n");
    }
}