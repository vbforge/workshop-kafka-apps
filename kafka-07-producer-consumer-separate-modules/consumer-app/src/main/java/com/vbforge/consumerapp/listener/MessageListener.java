package com.vbforge.consumerapp.listener;

import com.vbforge.consumerapp.model.MessageEvent;
import com.vbforge.consumerapp.service.MessageCacheService;
import com.vbforge.consumerapp.service.MessageProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class MessageListener {

    private final MessageProcessingService processingService;
    private final MessageCacheService cacheService;

    @KafkaListener(
//            topics = "t2-topic",
            topics = "orders-topic",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listen(
            @Payload MessageEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        try {
            log.info("Received message from partition: {}, offset: {}", partition, offset);

            // Check if message was already processed (idempotency) - only if ID exists
            if (event.getId() != null && cacheService.isAlreadyProcessed(event.getId())) {
                log.warn("Duplicate message detected: {}, skipping processing", event.getId());
                return;
            }

            // Process the message
            processingService.processMessage(event);

            log.debug("Message successfully processed: {}", event.getId() != null ? event.getId() : "NO_ID");

        } catch (Exception e) {
            log.error("Error processing message: {}, error: {}",
                    event.getId() != null ? event.getId() : "NO_ID",
                    e.getMessage(), e);
            // Exception will trigger retry mechanism
            throw e;
        }
    }

}
