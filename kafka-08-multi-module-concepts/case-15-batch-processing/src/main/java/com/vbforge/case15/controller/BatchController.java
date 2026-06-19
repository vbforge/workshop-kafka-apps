package com.vbforge.case15.controller;

import com.vbforge.case15.model.BatchStatus;
import com.vbforge.case15.model.ProducerResponse;
import com.vbforge.case15.service.BatchConsumerService;
import com.vbforge.case15.service.ProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class BatchController {

    private final ProducerService producerService;
    private final BatchConsumerService consumerService;

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Batch Processing Case is Running!");
    }

    // ── Producer ──

    // JUNIOR NOTE: Use count=100 or count=200 to really see batching in action.
    // With max-poll-records=50, 200 messages → ~4 batches of 50.
    // Watch logs: [BATCH #1] 50 records, [BATCH #2] 50 records, etc.
    @PostMapping("/producer/send")
    public ResponseEntity<ProducerResponse> send(
            @RequestParam(defaultValue = "100") int count) {
        return ResponseEntity.ok(producerService.sendBatch(count));
    }

    // ── Consumer stats ──

    // JUNIOR NOTE: /status is where you observe batch behaviour numerically.
    // After sending 100 messages with max-poll-records=50:
    //   batchCount:   2       (two poll() calls)
    //   totalReceived: 100
    //   avgBatchSize:  50.0
    //   recentBatches: [{batchNumber:1, size:50, ...}, {batchNumber:2, size:50, ...}]
    //
    // If records arrive unevenly (some partitions lagging), you'll see variable batch sizes.
    @GetMapping("/consumer/status")
    public ResponseEntity<BatchStatus> status() {
        return ResponseEntity.ok(consumerService.getStatus());
    }

}
