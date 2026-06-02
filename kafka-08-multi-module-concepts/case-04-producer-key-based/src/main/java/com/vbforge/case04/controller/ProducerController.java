package com.vbforge.case04.controller;

import com.vbforge.case04.model.KeyedSendResult;
import com.vbforge.case04.service.ProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

// JUNIOR NOTE: Four endpoints in this case:
//
//   POST /send-keyed            → keyed send, default partitioner (murmur2)
//   POST /send-custom-partitioner → keyed send, region-based custom partitioner
//   POST /send-no-key           → keyless send, sticky/round-robin partitioning
//   GET  /health                → health check
//
// The demo flow to observe key → partition routing:
//   1. Call /send-keyed with key=user-42 five times → same partition every time
//   2. Call /send-keyed with key=user-99 → likely a different partition
//   3. Call /send-custom-partitioner with key=eu-london → partition 0
//   4. Call /send-custom-partitioner with key=us-nyc → partition 1
//   5. Call /send-no-key six times → partition varies (sticky/round-robin)
//
// All endpoints return HTTP 200 with the broker-confirmed partition in the response body.
// We use sync send (.get()) here so you can see the partition immediately in the response.

@RestController
@RequestMapping("/api/producer")
@RequiredArgsConstructor
@Slf4j
public class ProducerController {

    private final ProducerService producerService;

    // GET http://localhost:8084/api/producer/health
    @GetMapping("/health")
    public ResponseEntity<String> healthCheck() {
        log.info("Health check: OK!");
        return ResponseEntity.ok("Key-Based Producer is Running!");
    }

    // Pattern 1: keyed send — default murmur2 partitioner
    // Same key → same partition, every time
    @PostMapping("/send-keyed")
    public ResponseEntity<KeyedSendResult> sendKeyed(
            @RequestParam String key,
            @RequestParam(required = false) String content) {

        log.info(">>> /send-keyed called with key='{}'", key);
        KeyedSendResult result = producerService.sendWithKey(key, content);
        return ResponseEntity.ok(result);
    }

    // Pattern 2: keyed send — custom region-based partitioner
    // eu-* → partition 0, us-* → partition 1, asia-* → partition 2
    @PostMapping("/send-custom-partitioner")
    public ResponseEntity<KeyedSendResult> sendCustomPartitioner(
            @RequestParam String key,
            @RequestParam(required = false) String content) {

        log.info(">>> /send-custom-partitioner called with key='{}'", key);
        KeyedSendResult result = producerService.sendWithCustomPartitioner(key, content);
        return ResponseEntity.ok(result);
    }

    // Pattern 3: keyless send — sticky/round-robin partitioning
    // No ordering guarantee — partition varies across messages
    @PostMapping("/send-no-key")
    public ResponseEntity<KeyedSendResult> sendNoKey(
            @RequestParam(required = false) String content) {

        log.info(">>> /send-no-key called — no partition routing");
        KeyedSendResult result = producerService.sendWithoutKey(content);
        return ResponseEntity.ok(result);
    }



}















