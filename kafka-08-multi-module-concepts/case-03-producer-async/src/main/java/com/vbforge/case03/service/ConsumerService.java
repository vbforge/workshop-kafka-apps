package com.vbforge.case03.service;

import com.vbforge.case03.model.MyMessageObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

// JUNIOR NOTE: The consumer in case-03 is intentionally simple — same as case-01/02.
// Case-03 is about the PRODUCER side (async callback patterns).
//
// The consumer here plays a critical role for the demo though:
// With async sends, the HTTP response returns BEFORE the broker ACKs.
// Watch the log timestamps when you call the endpoint:
//
//   1. [HTTP thread]   ">>> [CALLBACK] Submitting message ID: ..."  ← you see this first
//   2. [HTTP thread]   HTTP 202 goes back to Postman/curl
//   3. [Kafka I/O thread] ">>> [CALLBACK] Delivered message ID: ..."  ← callback fires after
//   4. [Consumer thread]  "****** Message Received *****"  ← consumer gets the message
//
// With case-02 sync, steps 1-2 were reversed — the HTTP response only came AFTER step 3.
// That ordering difference is the entire point of this case.


@Service
@Slf4j
public class ConsumerService {

    @KafkaListener(
            topics = "${kafka.topic.async}",
            groupId = "${kafka.consumer.groupID}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(MyMessageObject message){
        log.info("****** Message Received *****");
        log.info(" * ID:        {}", message.getId());
        log.info(" * Content:   {}", message.getContent());
        log.info(" * Timestamp: {}", message.getTimestamp());
        log.info("******************************");
    }



}
