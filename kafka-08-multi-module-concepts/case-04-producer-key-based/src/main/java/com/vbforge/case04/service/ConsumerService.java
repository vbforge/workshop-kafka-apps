package com.vbforge.case04.service;

import com.vbforge.case04.model.KeyedMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

// JUNIOR NOTE: The consumer in case-04 does one extra thing vs previous cases:
// it uses ConsumerRecord<K, V> instead of just the value type directly.
//
// ConsumerRecord gives you access to the full Kafka record metadata:
//   record.key()        → the message key (String)
//   record.partition()  → which partition this message came from
//   record.offset()     → position within that partition
//   record.value()      → the deserialized payload (KeyedMessage)
//
// This is important for this case because the whole lesson is about key → partition routing.
// Logging record.key() and record.partition() together proves the invariant:
//   "I sent 5 messages with key='user-42', and all 5 arrived from partition X."
//
// In previous cases we used the value type directly (@KafkaListener on MyMessageObject)
// because we didn't need the record metadata. Both forms are valid — use ConsumerRecord
// when you need the metadata, use the value type directly when you don't.

@Service
@Slf4j
public class ConsumerService {

    @KafkaListener(
            topics = "${kafka.topic.keyed}",
            groupId = "${kafka.consumer.groupID}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, KeyedMessage> record) {
        KeyedMessage message = record.value();
        log.info("****** Message Received *****");
        log.info(" * Kafka Key:  {}", record.key());
        log.info(" * Partition:  {}", record.partition());   // ← THE KEY OBSERVABLE
        log.info(" * Offset:     {}", record.offset());
        log.info(" * ID:         {}", message.getId());
        log.info(" * EntityKey:  {}", message.getEntityKey());
        log.info(" * Content:    {}", message.getContent());
        log.info(" * Timestamp:  {}", message.getTimestamp());
        log.info("*****************************");
    }

}














