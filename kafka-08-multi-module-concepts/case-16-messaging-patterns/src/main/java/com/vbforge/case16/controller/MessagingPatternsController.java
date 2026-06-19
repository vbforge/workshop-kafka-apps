package com.vbforge.case16.controller;

import com.vbforge.case16.model.RpcResponse;
import com.vbforge.case16.service.HeaderRoutingService;
import com.vbforge.case16.service.RpcClientService;
import com.vbforge.case16.service.RpcServerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class MessagingPatternsController {

    private final RpcClientService rpcClientService;
    private final RpcServerService rpcServerService;
    private final HeaderRoutingService headerRoutingService;

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Messaging Patterns Case is Running!");
    }


    // ── RPC / Request-Reply ──

    // JUNIOR NOTE: This endpoint drives the full round-trip RPC demo.
    // When you call POST /api/rpc/send, this service:
    //   1. Publishes to case-16-request-topic with a correlation ID
    //   2. Blocks until a reply arrives on case-16-reply-topic (or times out)
    //   3. Returns the reply + round-trip time to the HTTP caller
    //
    // In the same JVM, RpcServerService is listening on case-16-request-topic
    // and will handle the request — so the round trip is: HTTP → Kafka → Kafka → HTTP.
    // Normally the server would be a separate service/JVM — here it's combined for demo clarity.
    @PostMapping("/rpc/send")
    public ResponseEntity<RpcResponse> rpcSend(
            @RequestParam String payload,
            @RequestParam(defaultValue = "STANDARD") String priority) {
        RpcResponse response = rpcClientService.sendAndReceive(payload, priority);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/rpc/stats")
    public ResponseEntity<Map<String, Object>> rpcStats() {
        return ResponseEntity.ok(Map.of(
                "requestsHandledByServer", rpcServerService.getRequestsHandled()
        ));
    }


    // ── Header-based routing ──

    @PostMapping("/routing/send")
    public ResponseEntity<String> routingSend(
            @RequestParam String payload,
            @RequestParam(defaultValue = "STANDARD") String priority) {
        headerRoutingService.routeMessage(payload, priority);
        return ResponseEntity.ok("Routed message with priority=" + priority);
    }

    @GetMapping("/routing/stats")
    public ResponseEntity<Map<String, Object>> routingStats() {
        return ResponseEntity.ok(Map.of(
                "priorityProcessed", headerRoutingService.getPriorityProcessed(),
                "standardProcessed", headerRoutingService.getStandardProcessed()
        ));
    }

}
