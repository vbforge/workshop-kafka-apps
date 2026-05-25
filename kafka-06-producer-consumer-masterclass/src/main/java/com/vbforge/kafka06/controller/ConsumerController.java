package com.vbforge.kafka06.controller;

import com.vbforge.kafka06.consumer.service.ManualConsumerService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * REST controller for the low-level (manual) Kafka consumer endpoint.
 *
 * <p>This endpoint bypasses {@code @KafkaListener} entirely and uses the raw
 * {@link org.apache.kafka.clients.consumer.KafkaConsumer} API to read from
 * a specific partition and offset on demand.
 *
 * <p>Useful for:
 * <ul>
 *   <li>Inspecting messages at a specific position (debugging)</li>
 *   <li>Replaying messages from a known offset</li>
 *   <li>Ad-hoc data retrieval without a running listener</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/consumer")
@RequiredArgsConstructor
public class ConsumerController {

    @Value("${kafka.topics.general}")
    private String generalTopic;

    private final ManualConsumerService manualConsumerService;

    /**
     * Reads messages from the general topic starting at the given partition and offset.
     *
     * <p>Example calls:
     * <pre>
     *   GET /api/consumer/manual                          → partition=0, offset=0
     *   GET /api/consumer/manual?partition=1&offset=3     → partition 1 from offset 3
     * </pre>
     *
     * @param partition partition number to read from (0-based, default 0)
     * @param offset    starting offset within the partition (default 0)
     * @return list of deserialized message values from that position
     */
    @GetMapping("/manual")
    public ResponseEntity<List<Object>> readManually(
            @RequestParam(defaultValue = "0") int partition,
            @RequestParam(defaultValue = "0") long offset
    ) {
        List<Object> messages = manualConsumerService.readMessages(generalTopic, partition, offset);
        return ResponseEntity.ok(messages);
    }
}
