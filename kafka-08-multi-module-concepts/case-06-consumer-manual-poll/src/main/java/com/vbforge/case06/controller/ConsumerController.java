package com.vbforge.case06.controller;

import com.vbforge.case06.model.ConsumerStatus;
import com.vbforge.case06.model.ProducerResponse;
import com.vbforge.case06.service.ManualPollConsumerService;
import com.vbforge.case06.service.ProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

// JUNIOR NOTE: This controller exposes the manual poll loop's control surface over HTTP.
// This is the interactive demo: you can drive the consumer from Postman/curl in real time.
//
// Typical demo sequence:
//   1. POST /api/producer/send?count=12  → load the topic with messages
//   2. GET  /api/consumer/status         → confirm "RUNNING", see totalProcessed climbing
//   3. POST /api/consumer/pause          → consumer stops fetching new messages
//   4. POST /api/producer/send?count=5   → send 5 more while paused (they queue up in Kafka)
//   5. GET  /api/consumer/status         → confirm "PAUSED", totalProcessed unchanged
//   6. POST /api/consumer/resume         → consumer picks up where it left off + queued messages
//   7. POST /api/consumer/seek?partition=0&offset=0 → rewind partition 0 to the beginning

@Slf4j
@RestController
@RequiredArgsConstructor
public class ConsumerController {

    private final ManualPollConsumerService consumerService;
    private final ProducerService producerService;

    // GET http://localhost:8086/api/producer/health
    @GetMapping("/api/producer/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Manual Poll Consumer is Running!");
    }

    // ── Producer ──

    @PostMapping("/api/producer/send")
    public ResponseEntity<ProducerResponse> send(
            @RequestParam(defaultValue = "10") int count) {
        log.info(">>> POST /send count={}", count);
        return ResponseEntity.ok(producerService.sendBulk(count));
    }

    // ── Consumer control ──

    @GetMapping("/api/consumer/status")
    public ResponseEntity<ConsumerStatus> status() {
        return ResponseEntity.ok(consumerService.getStatus());
    }

    @PostMapping("/api/consumer/pause")
    public ResponseEntity<String> pause() {
        consumerService.pause();
        return ResponseEntity.ok("Pause signal sent — takes effect on next poll iteration");
    }

    @PostMapping("/api/consumer/resume")
    public ResponseEntity<String> resume() {
        consumerService.resume();
        return ResponseEntity.ok("Resume signal sent — takes effect on next poll iteration");
    }

    // JUNIOR NOTE: seek() lets you rewind to any offset — even offset 0 (the very beginning).
    // This is powerful for: replaying a bad batch, skipping a poison pill (seek past it),
    // or custom checkpoint recovery (seek to your last known-good offset on startup).
    // After seek, the next poll() returns records starting from the seeked offset.
    @PostMapping("/api/consumer/seek")
    public ResponseEntity<String> seek(
            @RequestParam(defaultValue = "0") int partition,
            @RequestParam(defaultValue = "0") long offset) {
        consumerService.seekToOffset(partition, offset);
        return ResponseEntity.ok("Seek signal sent — partition=" + partition + " offset=" + offset);
    }

}