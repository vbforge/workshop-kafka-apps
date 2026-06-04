package com.vbforge.case07.controller;

import com.vbforge.case07.model.ProducerResponse;
import com.vbforge.case07.service.AnalyticsConsumer;
import com.vbforge.case07.service.AuditConsumer;
import com.vbforge.case07.service.NotifyConsumer;
import com.vbforge.case07.service.ProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class ConsumerGroupsController {

    private final ProducerService    producerService;
    private final AnalyticsConsumer  analyticsConsumer;
    private final AuditConsumer      auditConsumer;
    private final NotifyConsumer     notifyConsumer;

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Consumer Groups Case is Running!");
    }

    @PostMapping("/producer/send")
    public ResponseEntity<ProducerResponse> send(
            @RequestParam(defaultValue = "9") int count) {
        return ResponseEntity.ok(producerService.sendBatch(count));
    }

    // JUNIOR NOTE: /status shows each group's independent state side by side.
    // After sending 9 events, all three groups should show processed=9 eventually —
    // proof that every group received every message independently.
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> status() {
        Map<String, Object> response = new LinkedHashMap<>();

        Map<String, Object> analytics = new LinkedHashMap<>();
        analytics.put("group", "case-07-analytics-group");
        analytics.put("countsByRegion", analyticsConsumer.getCounts());
        analytics.put("totalsByRegion", analyticsConsumer.getTotals());

        Map<String, Object> audit = new LinkedHashMap<>();
        audit.put("group", "case-07-audit-group");
        audit.put("auditLogSize", auditConsumer.getAuditLog().size());
        audit.put("lastEntries", auditConsumer.getAuditLog()
                .stream().skip(Math.max(0, auditConsumer.getAuditLog().size() - 3)).toList());

        Map<String, Object> notify = new LinkedHashMap<>();
        notify.put("group", "case-07-notify-group");
        notify.put("notificationsSent", notifyConsumer.getSent());
        notify.put("notificationsSkipped", notifyConsumer.getSkipped());

        response.put("analytics", analytics);
        response.put("audit", audit);
        response.put("notify", notify);

        return ResponseEntity.ok(response);
    }

}










