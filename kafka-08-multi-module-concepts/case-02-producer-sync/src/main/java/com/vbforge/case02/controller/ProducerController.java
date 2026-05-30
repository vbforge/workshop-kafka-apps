package com.vbforge.case02.controller;

import com.vbforge.case02.model.SendResultMetadata;
import com.vbforge.case02.service.ProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

// JUNIOR NOTE: Three endpoints, each demonstrating a different flavor of blocking send:
//
//   POST /send-blocking        → .get() with NO timeout (dangerous, for learning only)
//   POST /send-with-timeout    → .get(N, SECONDS) using app-configured timeout (production pattern)
//   POST /send-custom-timeout  → .get(N, SECONDS) where N comes from the request (flexible)
//
// All three return SendResult — which includes broker-confirmed partition + offset.
// That's the key observable difference vs case-01: you actually SEE where the broker wrote it.

@Slf4j
@RestController
@RequestMapping("/api/producer")
@RequiredArgsConstructor
public class ProducerController {

    private final ProducerService producerService;

    // GET http://localhost:8082/api/producer/health
    @GetMapping("/health")
    public ResponseEntity<String> healthCheck() {
        log.info("Health check: OK!");
        return ResponseEntity.ok("Sync Producer is Running!");
    }

    // Pattern 1: Naked blocking — .get() with no timeout
    // Teaches: what "blocking" means at its most basic
    @PostMapping("/send-blocking")
    public ResponseEntity<SendResultMetadata> sendBlocking(
            @RequestParam(required = false) String content) {

        log.info(">>> /send-blocking called");
        SendResultMetadata result = producerService.sendBlocking(content);
        return ResponseEntity.ok(result);
    }

    // Pattern 2: Bounded blocking — .get(timeout, unit) from application.yml config
    // Teaches: how to make sync sends safe for production
    @PostMapping("/send-with-timeout")
    public ResponseEntity<SendResultMetadata> sendWithTimeout(
            @RequestParam(required = false) String content) {

        log.info(">>> /send-with-timeout called");
        SendResultMetadata result = producerService.sendWithTimeout(content);
        return ResponseEntity.ok(result);
    }

    // Pattern 3: Caller-controlled timeout — flexible per use-case
    // Teaches: different message priorities may warrant different SLAs
    @PostMapping("/send-custom-timeout")
    public ResponseEntity<SendResultMetadata> sendCustomTimeout(
            @RequestParam(required = false) String content,
            @RequestParam(defaultValue = "3") int timeoutSeconds) {

        log.info(">>> /send-custom-timeout called with timeout={}s", timeoutSeconds);
        SendResultMetadata result = producerService.sendWithCustomTimeout(content, timeoutSeconds);
        return ResponseEntity.ok(result);
    }

}
