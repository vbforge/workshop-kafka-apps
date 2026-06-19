package com.vbforge.case17.controller;

import com.vbforge.case17.model.ProducerResponse;
import com.vbforge.case17.service.EventConsumerService;
import com.vbforge.case17.service.EventProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class EventController {

    private final EventProducerService producerService;
    private final EventConsumerService consumerService;

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Testcontainers Case is Running!");
    }

    @PostMapping("/events/send")
    public ResponseEntity<ProducerResponse> send(
            @RequestParam(defaultValue = "5") int count,
            @RequestParam(defaultValue = "ORDER_PLACED") String type) {
        return ResponseEntity.ok(producerService.sendBatch(count, type));
    }

    @GetMapping("/events/stats")
    public ResponseEntity<Map<String, Object>> stats() {
        return ResponseEntity.ok(Map.of(
                "totalReceived", consumerService.getTotalReceived(),
                "bufferedCount", consumerService.getReceivedEvents().size()
        ));
    }

}
