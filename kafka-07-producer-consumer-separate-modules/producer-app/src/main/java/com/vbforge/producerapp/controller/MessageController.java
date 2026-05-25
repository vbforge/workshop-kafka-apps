package com.vbforge.producerapp.controller;

import com.vbforge.producerapp.service.MessageProducerService;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class MessageController {

    private final MessageProducerService producerService;

    @PostMapping
    public ResponseEntity<Map<String, String>> sendMessage(@RequestBody MessageRequest request) {
        producerService.sendMessage(request.getMessage(), request.getSender(), request.getPriority());
        return ResponseEntity.ok(Map.of(
                "status", "Message sent successfully",
                "message", request.getMessage(),
                "sender", request.getSender()
        ));
    }

    @GetMapping("/test")
    public ResponseEntity<Map<String, String>> testEndpoint() {
        return ResponseEntity.ok(Map.of(
                "status", "Producer API is running",
                "version", "1.0.0"
        ));
    }

    @Data
    public static class MessageRequest {
        private String message;
        private String sender;
        private String priority;
    }

}
