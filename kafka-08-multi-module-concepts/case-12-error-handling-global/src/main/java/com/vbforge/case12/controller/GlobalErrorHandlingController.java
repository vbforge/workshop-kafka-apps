package com.vbforge.case12.controller;

import com.vbforge.case12.model.ProducerResponse;
import com.vbforge.case12.service.GlobalDltConsumer;
import com.vbforge.case12.service.NotificationsConsumer;
import com.vbforge.case12.service.OrdersConsumer;
import com.vbforge.case12.service.PaymentsConsumer;
import com.vbforge.case12.service.ProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
 
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
 
// JUNIOR NOTE: Three send endpoints — one per topic type (orders, payments, notifications).
// All three topic listeners share the same global error handler behind the scenes.
// The demo goal: prove that a single change to globalErrorHandler() in KafkaConfig
// affects ALL listeners — no per-factory duplication needed.
//
// Demo sequence:
//   1. POST /send/orders?failureMode=none            → processed by OrdersConsumer
//   2. POST /send/payments?failureMode=transient     → retried, then routed to shared DLT
//   3. POST /send/notifications?failureMode=non-retryable → instant DLT routing
//   4. GET  /status                                  → see counts per topic + DLT breakdown
 
@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class GlobalErrorHandlingController {
 
    private final ProducerService        producerService;
    private final OrdersConsumer         ordersConsumer;
    private final PaymentsConsumer       paymentsConsumer;
    private final NotificationsConsumer  notificationsConsumer;
    private final GlobalDltConsumer      globalDltConsumer;
 
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Global Error Handler Case is Running!");
    }
 
    // Send to orders topic
    @PostMapping("/send/orders")
    public ResponseEntity<ProducerResponse> sendOrder(
            @RequestParam(defaultValue = "none") String failureMode) {
        log.info(">>> POST /send/orders failureMode={}", failureMode);
        return ResponseEntity.ok(producerService.send("orders", failureMode));
    }
 
    // Send to payments topic
    @PostMapping("/send/payments")
    public ResponseEntity<ProducerResponse> sendPayment(
            @RequestParam(defaultValue = "none") String failureMode) {
        log.info(">>> POST /send/payments failureMode={}", failureMode);
        return ResponseEntity.ok(producerService.send("payments", failureMode));
    }
 
    // Send to notifications topic
    @PostMapping("/send/notifications")
    public ResponseEntity<ProducerResponse> sendNotification(
            @RequestParam(defaultValue = "none") String failureMode) {
        log.info(">>> POST /send/notifications failureMode={}", failureMode);
        return ResponseEntity.ok(producerService.send("notifications", failureMode));
    }
 
    // Status — shows per-listener success counts + DLT breakdown by source topic
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> s = new LinkedHashMap<>();
 
        // Per-listener success counts
        Map<String, Integer> processed = new LinkedHashMap<>();
        processed.put("orders",        ordersConsumer.getProcessed());
        processed.put("payments",      paymentsConsumer.getProcessed());
        processed.put("notifications", notificationsConsumer.getProcessed());
        s.put("successfullyProcessed", processed);
 
        // DLT breakdown — how many failed records per source topic
        Map<String, Integer> dltCounts = globalDltConsumer.getCountBySourceTopic()
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get()));
        s.put("dltReceivedBySourceTopic", dltCounts);
 
        s.put("note", "All three listeners share ONE globalErrorHandler bean. " +
                "All failures route to ONE shared DLT: case-12-global.DLT");
 
        return ResponseEntity.ok(s);
    }
 
}