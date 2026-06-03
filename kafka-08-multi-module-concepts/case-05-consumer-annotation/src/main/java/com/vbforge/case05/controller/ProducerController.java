package com.vbforge.case05.controller;

import com.vbforge.case05.model.ProducerResponse;
import com.vbforge.case05.service.ProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

// JUNIOR NOTE: Two producer endpoints:
//
//   POST /send-bulk   → send N messages per key across all 3 partitions
//                       Default: 3 messages x 3 keys = 9 total (one per partition per batch)
//                       This is the main demo endpoint — fires enough messages to see
//                       all three consumer threads working concurrently in the logs.
//
//   POST /send-single → send one message with a specific key
//                       Useful for targeted experiments: "send 5 messages to key=alpha,
//                       confirm they all land on the same partition and same thread"

@RestController
@RequestMapping("/api/producer")
@RequiredArgsConstructor
@Slf4j
public class ProducerController {

    private final ProducerService producerService;

    // GET http://localhost:8085/api/producer/health
    @GetMapping("/health")
    public ResponseEntity<String> healthCheck() {
        log.info("Health check: OK!");
        return ResponseEntity.ok("Consumer Annotation Case is Running!");
    }

    // POST http://localhost:8085/api/producer/send-bulk
    // Bulk send — default: 3 keys × 3 messages = 9 total
    // Optional: ?keys=alpha,beta,gamma&messagesPerKey=3
    @PostMapping("/send-bulk")
    public  ResponseEntity<ProducerResponse> sendBulk(
            @RequestParam(required = false) List<String> keys,
            @RequestParam(defaultValue = "3") int messagesPerKey) {

        log.info(">>> /send-bulk called — keys={} messagesPerKey={}", keys, messagesPerKey);

        ProducerResponse response = producerService.sendBulk(keys, messagesPerKey);

        return ResponseEntity.ok(response);
    }

    // POST http://localhost:8085/api/producer/send-single
    //Single send requires key
    @PostMapping("/send-single")
    public  ResponseEntity<ProducerResponse.MessageSummary> sendSingle(
            @RequestParam String key,
            @RequestParam(required = false) String content){

        log.info(">>> /send-single called — key='{}' content='{}'", key, content);

        ProducerResponse.MessageSummary result = producerService.sendSingle(key, content);

        return ResponseEntity.ok(result);
    }




}


















