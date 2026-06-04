package com.vbforge.case08.controller;

import com.vbforge.case08.model.OffsetStatus;
import com.vbforge.case08.model.ProducerResponse;
import com.vbforge.case08.service.OffsetManagementConsumerService;
import com.vbforge.case08.service.ProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class OffsetManagementController {

    private final ProducerService producerService;
    private final OffsetManagementConsumerService consumerService;

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Offset Management Case is Running!");
    }

    // ── Producer ──

    @PostMapping("/producer/send")
    public ResponseEntity<ProducerResponse> send(
            @RequestParam(defaultValue = "10") int count) {
        return ResponseEntity.ok(producerService.sendBatch(count));
    }

    // ── Consumer status ──

    // JUNIOR NOTE: /status is the core observable of this case.
    // Watch currentPositions vs committedOffsets:
    //   - After polling but before restart: positions advance, committed stays.
    //   - After seekToBeginning(): positions reset to 0, committed unchanged.
    //   - After skipOffset(): committed jumps past the skipped record.
    @GetMapping("/consumer/status")
    public ResponseEntity<OffsetStatus> status() {
        return ResponseEntity.ok(consumerService.getStatus());
    }

    // ── Offset operations ──

    @PostMapping("/consumer/seek/beginning")
    public ResponseEntity<String> seekToBeginning() {
        consumerService.seekToBeginning();
        return ResponseEntity.ok("seekToBeginning signal sent - all partitions will rewind to offset 0");
    }

    @PostMapping("/consumer/seek/end")
    public ResponseEntity<String> seekToEnd() {
        consumerService.seekToEnd();
        return ResponseEntity.ok("seekToEnd signal sent - all partitions will skip to latest offset");
    }

    @PostMapping("/consumer/seek/offset")
    public ResponseEntity<String> seekToOffset(
            @RequestParam(defaultValue = "0") int partition,
            @RequestParam(defaultValue = "0") long offset) {
        consumerService.seekToOffset(partition, offset);
        return ResponseEntity.ok("seekToOffset signal sent - partition=" + partition + " offset=" + offset);
    }

    // JUNIOR NOTE: skipOffset commits past a specific offset WITHOUT processing it.
    // Use for poison pills: "I know record at partition=0 offset=3 is bad — skip it."
    @PostMapping("/consumer/skip")
    public ResponseEntity<String> skip(
            @RequestParam(defaultValue = "0") int partition,
            @RequestParam long offset) {
        consumerService.skipOffset(partition, offset);
        return ResponseEntity.ok("Skip signal sent - partition=" + partition + " offset=" + offset + " will be committed without processing");
    }

    // ── Pause/Resume ──

    @PostMapping("/consumer/pause")
    public ResponseEntity<String> pause() {
        consumerService.pause();
        return ResponseEntity.ok("Pause signal sent");
    }

    @PostMapping("/consumer/resume")
    public ResponseEntity<String> resume() {
        consumerService.resume();
        return ResponseEntity.ok("Resume signal sent");
    }

}
