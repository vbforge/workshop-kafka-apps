package com.vbforge.case01.service;

import com.vbforge.case01.model.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaTemplate<String, String> kafkaTemplateString;

    @Value("${kafka.topic.test}")
    private String testTopic;

    @Value("${kafka.topic.test-string}")
    private String testTopicString;

    @Value("${kafka.default.message}")
    private String defaultMessage;

    public Message send(String content) {
        if (content == null || content.isBlank()) {
            content = defaultMessage;
        }
        Message message = Message.builder()
                .id(UUID.randomUUID().toString())
                .content(content)
                .timestamp(LocalDateTime.now())
                .build();

        kafkaTemplate.send(testTopic, message.getId(), message);
        log.info(">>> PRODUCER TRIGGERED <<<");
        return message;
    }

    public String sendString(String content) {
        if (content == null || content.isBlank()) {
            content = defaultMessage;
        }
        kafkaTemplateString.send(testTopicString, "string-key", content);
        log.info(">>> PRODUCER STRING TRIGGERED <<<");
        return content;
    }
}