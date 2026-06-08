package com.vbforge.case13.controller;

import com.vbforge.case13.model.ProducerResponse;
import com.vbforge.case13.service.OrderConsumerService;
import com.vbforge.case13.service.ProcessedConsumerService;
import com.vbforge.case13.service.TransactionalProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

// JUNIOR NOTE: Demo flow:
//
//   1. POST /send?rollback=false         → sends to orders topic → consumer → committed transaction
//                                           both processed-topic and probe-topic receive records
//
//   2. POST /send?rollback=true          → sends to orders topic → consumer → aborted transaction
//                                           NEITHER processed-topic NOR probe-topic receive records
//                                           despite the producer having called send() for both
//
//   3. GET /status                       → compare ordersProcessed vs processedTopicReceived
//                                           in a correctly configured system these match exactly
//
// The "proof" of atomicity:
//   Send 5 committed + 3 rollbacks.
//   GET /status → ordersProcessed=5, processedTopicReceived=5, rolledBack=3
//   The 3 rolled-back records never appear in processedTopicReceived — atomicity confirmed.

@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class TransactionsController {

    private final TransactionalProducerService producerService;
    private final OrderConsumerService         orderConsumerService;
    private final ProcessedConsumerService     processedConsumerService;

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Transactions Case is Running!");
    }

    // Send an order to the orders topic — consumer picks it up and runs the transaction
    @PostMapping("/send")
    public ResponseEntity<ProducerResponse> send(
            @RequestParam(defaultValue = "99.99") double amount,
            @RequestParam(defaultValue = "false") boolean rollback) {
        log.info(">>> POST /send amount={} rollback={}", amount, rollback);
        return ResponseEntity.ok(producerService.sendOrder(amount, rollback));
    }

    // Direct commit — bypasses orders topic, calls transactional service directly
    // Useful for testing the commit path in isolation
    @PostMapping("/commit")
    public ResponseEntity<ProducerResponse> commit(
            @RequestParam(defaultValue = "99.99") double amount) {
        log.info(">>> POST /commit amount={}", amount);
        return ResponseEntity.ok(producerService.sendCommitted(amount));
    }

    // Direct rollback — bypasses orders topic, calls transactional service directly
    // Returns 500 because the method always throws — that's the point of the demo
    @PostMapping("/rollback")
    public ResponseEntity<Map<String, String>> rollback(
            @RequestParam(defaultValue = "99.99") double amount) {
        log.info(">>> POST /rollback amount={}", amount);
        try {
            producerService.sendRolledBack(amount);
            // Should never reach here
            return ResponseEntity.ok(Map.of("result", "UNEXPECTED_COMMIT"));
        } catch (RuntimeException e) {
            // JUNIOR NOTE: We catch the exception here so the HTTP response is clean.
            // In the logs you'll see the transaction abort message from KafkaTransactionManager.
            Map<String, String> body = new LinkedHashMap<>();
            body.put("result", "ROLLED_BACK");
            body.put("reason", e.getMessage());
            body.put("note", "Check processed-topic consumer — it received ZERO records for this send.");
            return ResponseEntity.status(500).body(body);
        }
    }

    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> s = new LinkedHashMap<>();

        s.put("ordersConsumed_committed",    orderConsumerService.getProcessedCount());
        s.put("ordersConsumed_rolledBack",   orderConsumerService.getRolledBackCount());
        s.put("processedTopic_received",     processedConsumerService.getReceivedCount());
        s.put("atomicityCheck",
                orderConsumerService.getProcessedCount() == processedConsumerService.getReceivedCount()
                        ? "✓ PASS — committed count matches processed-topic received count"
                        : "✗ FAIL — mismatch! check isolation.level config");
        s.put("note",
                "processedTopic_received should equal ordersConsumed_committed. " +
                "Rolled-back records must NEVER appear in processedTopic_received.");

        return ResponseEntity.ok(s);
    }

}
