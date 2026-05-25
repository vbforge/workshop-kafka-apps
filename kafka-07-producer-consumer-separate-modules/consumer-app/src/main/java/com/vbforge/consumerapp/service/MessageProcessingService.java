package com.vbforge.consumerapp.service;

import com.vbforge.consumerapp.model.MessageEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

@Slf4j
@Service
@RequiredArgsConstructor // Lombok generates constructor with all final fields
public class MessageProcessingService {

    private final MessageCacheService cacheService;

    public void processMessage(MessageEvent event) {
        log.info("===========================================");
        log.info("Processing message: {}", event.getId() != null ? event.getId() : "NO_ID");
        log.info("Message content: {}", event.getMessage());
        log.info("Sender: {}", event.getSender());
        log.info("Priority: {}", event.getPriority() != null ? event.getPriority() : "NORMAL");
        log.info("Timestamp: {}", event.getTimestamp());

        // Null-safe latency calculation
        if (event.getTimestamp() != null) {
            long latency = ChronoUnit.MILLIS.between(event.getTimestamp(), LocalDateTime.now());
            log.info("Processing latency: {} ms", latency);
        } else {
            log.info("Processing latency: N/A (timestamp not provided)");
        }

        // Simulate business logic based on priority
        String priority = event.getPriority() != null ? event.getPriority() : "NORMAL";

        if ("HIGH".equalsIgnoreCase(priority)) {
            log.warn("HIGH priority message detected - expedited processing");
            handleHighPriority(event);
        } else {
            handleNormalPriority(event);
        }

        // Mark message as processed in cache (for deduplication)
        if (event.getId() != null) {
            cacheService.markAsProcessed(event.getId());
        } else {
            log.warn("Message has no ID - cannot cache for deduplication");
        }

        log.info("Message processed successfully!");
        log.info("===========================================");
    }

    private void handleHighPriority(MessageEvent event) {
        log.info("Executing high-priority workflow");
        // Add your high-priority logic here
    }

    private void handleNormalPriority(MessageEvent event) {
        log.info("Executing normal workflow");
        // Add your normal-priority logic here
    }

}
