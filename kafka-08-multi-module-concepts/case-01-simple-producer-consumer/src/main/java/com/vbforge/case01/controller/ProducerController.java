package com.vbforge.case01.controller;

import com.vbforge.case01.model.Message;
import com.vbforge.case01.service.ProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/producer")
@RequiredArgsConstructor
public class ProducerController {

    private final ProducerService producerService;

    @GetMapping("/health")
    public ResponseEntity<String> healthCheck() {
        log.info("Health check: OK!");
        return ResponseEntity.ok("Producer is Running!");
    }

    @PostMapping("/send")
    public ResponseEntity<Message> sendMessage(@RequestParam(required = false) String content) {
        Message message = producerService.send(content);
        log.info(">>> Message Sent: {}", message);
        return ResponseEntity.ok(message);
    }

    @PostMapping("/send-string")
    public ResponseEntity<String> sendString(@RequestParam(required = false) String content) {
        String result = producerService.sendString(content);
        log.info(">>> String Sent: {}", result);
        return ResponseEntity.ok(result);
    }
}