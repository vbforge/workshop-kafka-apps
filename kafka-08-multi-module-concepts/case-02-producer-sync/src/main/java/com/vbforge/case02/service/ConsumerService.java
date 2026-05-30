package com.vbforge.case02.service;

import com.vbforge.case02.model.MyMessageObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

// JUNIOR NOTE: The consumer in case-02 is intentionally simple — same as case-01.
// Case-02 is about the PRODUCER side (blocking send patterns).
// The consumer's job is just to prove the message arrived after the sync send completed.
//
// This is actually a great demo: because the producer blocks until the broker ACKs,
// by the time the HTTP response comes back to Postman/curl, the message is already
// committed to the Kafka log. The consumer will log it nearly immediately after.
// With async (case-01), there was no such guarantee.

@Service
@Slf4j
public class ConsumerService {

    @KafkaListener(
            topics = "${kafka.topic.sync}",
            groupId = "${kafka.consumer.groupID}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(MyMessageObject message) {
        log.info("****** Message Received *****");
        log.info(" * ID:        {}", message.getId());
        log.info(" * Content:   {}", message.getContent());
        log.info(" * Timestamp: {}", message.getTimestamp());
        log.info("******************************");
    }

}
