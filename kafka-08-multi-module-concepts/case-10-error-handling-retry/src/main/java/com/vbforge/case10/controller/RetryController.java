package com.vbforge.case10.controller;

import com.vbforge.case10.model.ProducerResponse;
import com.vbforge.case10.service.ProducerService;
import com.vbforge.case10.service.RetryConsumerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class RetryController {

    private final ProducerService       producerService;
    private final RetryConsumerService  consumerService;

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Error Handling Retry Case is Running!");
    }

    // JUNIOR NOTE: Three demo endpoints matching the three failure modes:
    //
    //   /send/always-fail         → watch retries until maxElapsedTime, then recovery
    //   /send/eventually-succeed  → watch retries N times, then SUCCESS (not recovery)
    //   /send/non-retryable       → instant recovery, no retry lines in logs

    @PostMapping("/send/always-fail")
    public ResponseEntity<ProducerResponse> sendAlwaysFail(
            @RequestParam(required = false) String content) {
        return ResponseEntity.ok(
                producerService.send(content, "always-fail", 0));
    }

    @PostMapping("/send/eventually-succeed")
    public ResponseEntity<ProducerResponse> sendEventuallySucceed(
            @RequestParam(required = false) String content,
            @RequestParam(defaultValue = "3") int succeedOnAttempt) {
        return ResponseEntity.ok(
                producerService.send(content, "eventually-succeed", succeedOnAttempt));
    }

    @PostMapping("/send/non-retryable")
    public ResponseEntity<ProducerResponse> sendNonRetryable(
            @RequestParam(required = false) String content) {
        return ResponseEntity.ok(
                producerService.send(content, "non-retryable", 0));
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("successCount",      consumerService.getSuccessCount());
        s.put("nonRetryableCount", consumerService.getNonRetryableCount());
        s.put("deliveryAttempts",  consumerService.getDeliveryAttempts().entrySet().stream()
                .collect(Collectors.toMap(
                        e -> e.getKey().substring(0, 8), // shorten UUID for readability
                        e -> e.getValue().get())));
        return ResponseEntity.ok(s);
    }

}
