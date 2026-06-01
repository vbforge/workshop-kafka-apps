package com.vbforge.case03.controller;

// JUNIOR NOTE: Three endpoints — one per async pattern:
//
//   POST /send-callback      → whenComplete() callback (recommended production pattern)
//   POST /send-split         → thenAccept() + exceptionally() (split success/failure handlers)
//   POST /send-fire-forget   → error-only callback (for non-critical high-volume events)
//
// All three return HTTP 202 Accepted (not 200 OK).
// This is the correct HTTP semantics for async acceptance:
//   200 OK       → request was processed AND completed
//   202 Accepted → request was accepted for processing, outcome is unknown at response time
//
// Returning 200 for an async send would be misleading — the message might still fail
// after the HTTP response is returned. 202 communicates "we accepted it, we'll try to deliver."

import com.vbforge.case03.model.AsyncSendReceipt;
import com.vbforge.case03.service.ProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/producer")
@Slf4j
@RequiredArgsConstructor
public class ProducerController {

    private final ProducerService producerService;

    // GET http://localhost:8083/api/producer/health
    @GetMapping("/health")
    public ResponseEntity<String> healthCheck() {
        log.info("Health check: OK!");
        return ResponseEntity.ok("Async Producer is Running!");
    }

    //POST http://localhost:8083/api/producer/send-callback
    // Pattern 1: whenComplete() — single callback for both success and failure
    // Returns 202 Accepted immediately
    @ResponseStatus(HttpStatus.ACCEPTED) //202 Accepted.
    @PostMapping("/send-callback")
    public ResponseEntity<AsyncSendReceipt> sendWithCallback(
            @RequestParam(required = false) String content) {

        log.info(">>> /send-callback called");
        AsyncSendReceipt receipt = producerService.sendFireAndCallback(content);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(receipt);
    }

    // POST http://localhost:8083/api/producer/send-split
    // Pattern 2: thenAccept() + exceptionally() — separate success and failure handlers
    // Returns 202 Accepted immediately
    @PostMapping("/send-split")
    public ResponseEntity<AsyncSendReceipt> sendWithSplitHandlers(
            @RequestParam(required = false) String content) {

        log.info(">>> /send-split called");
        AsyncSendReceipt receipt = producerService.sendWithSplitHandlers(content);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(receipt);
    }

    // POST http://localhost:8083/api/producer/send-fire-forget
    // Pattern 3: Fire-and-forget with minimal error logging
    // Returns 202 Accepted immediately
    @PostMapping("/send-fire-forget")
    public ResponseEntity<AsyncSendReceipt> sendFireAndForget(
            @RequestParam(required = false) String content) {

        log.info(">>> /send-fire-forget called");
        AsyncSendReceipt receipt = producerService.sendFireAndForget(content);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(receipt);
    }



}











