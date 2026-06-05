package com.vbforge.case11.controller;

import com.vbforge.case11.model.ProducerResponse;
import com.vbforge.case11.service.DltConsumerService;
import com.vbforge.case11.service.OrderConsumerService;
import com.vbforge.case11.service.ProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
 
import java.util.LinkedHashMap;
import java.util.Map;
 
// JUNIOR NOTE: Demo flow:
//
//   1. POST /send?failureMode=none        → processed cleanly, DLT untouched
//   2. POST /send?failureMode=transient   → retries fire, then DLT receives the record
//   3. POST /send?failureMode=non-retryable → instant DLT routing, no retries
//   4. GET  /status                       → see main-consumer vs DLT counts side by side
//
// After step 2 or 3: check logs for the DLT consumer output with kafka_dlt-* headers.
// Use Docker CLI to verify the DLT topic received the record:
//   kafka-console-consumer --topic case-11-topic.DLT --from-beginning
 
@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class DltController {
 
    private final ProducerService      producerService;
    private final OrderConsumerService orderConsumerService;
    private final DltConsumerService   dltConsumerService;
 
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("DLT Case is Running!");
    }
 
    @PostMapping("/send")
    public ResponseEntity<ProducerResponse> send(
            @RequestParam(defaultValue = "none") String failureMode,
            @RequestParam(defaultValue = "99.99") double amount) {
        return ResponseEntity.ok(producerService.send(failureMode, amount));
    }
 
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("mainConsumer_success",    orderConsumerService.getSuccessCount());
        s.put("mainConsumer_dltRouted",  orderConsumerService.getDltRoutedCount());
        s.put("dltConsumer_received",    dltConsumerService.getDltReceivedCount());
        s.put("note", "dltRouted increments when listener throws. dltReceived increments when DLT consumer processes it.");
        return ResponseEntity.ok(s);
    }
 
}