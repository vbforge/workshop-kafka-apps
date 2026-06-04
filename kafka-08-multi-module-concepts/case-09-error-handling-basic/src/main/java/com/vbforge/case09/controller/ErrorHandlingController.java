package com.vbforge.case09.controller;

import com.vbforge.case09.model.ProducerResponse;
import com.vbforge.case09.service.ProducerService;
import com.vbforge.case09.service.TaskConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

// JUNIOR NOTE: The demo flow:
//
//   1. POST /send?failureMode=none       → happy path, consumer logs "✓ Processed"
//   2. POST /send?failureMode=transient  → 3 retry attempts logged, then recovery
//   3. POST /send?failureMode=fatal      → 1 attempt, immediate recovery (no retries)
//   4. POST /send?failureMode=npe        → same as fatal — no retries
//   5. POST /send?failureMode=none       → processes cleanly, proving partition not blocked
//   6. GET  /status                      → see counts per outcome

@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class ErrorHandlingController {

    private final ProducerService     producerService;
    private final TaskConsumerService consumerService;

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Error Handling Basic Case is Running!");
    }

    @PostMapping("/send")
    public ResponseEntity<ProducerResponse> send(
            @RequestParam(required = false) String content,
            @RequestParam(defaultValue = "none") String failureMode) {
        log.info(">>> POST /send failureMode={}", failureMode);
        return ResponseEntity.ok(producerService.send(content, failureMode));
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("successCount",   consumerService.getSuccessCount());
        s.put("retryCount",     consumerService.getRetryCount());
        s.put("recoveredCount", consumerService.getRecoveredCount());
        s.put("note", "retryCount = times a transient message was ATTEMPTED (each costs 1 per attempt). " +
                "recoveredCount = times the handler ran recovery (fatal/npe/exhausted-retries).");
        return ResponseEntity.ok(s);
    }

}
