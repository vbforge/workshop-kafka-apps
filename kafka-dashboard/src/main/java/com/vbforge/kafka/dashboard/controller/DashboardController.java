package com.vbforge.kafka.dashboard.controller;

import com.vbforge.kafka.dashboard.service.AuditLogService;
import com.vbforge.kafka.dashboard.service.DashboardStatsService;
import com.vbforge.kafka.dashboard.service.ProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
 
import java.util.Map;
 
@Controller
@RequiredArgsConstructor
public class DashboardController {
 
    private final ProducerService producerService;
    private final DashboardStatsService statsService;
    private final AuditLogService auditLogService;
 
    /**
     * Main dashboard page.
     * Passes an initial stats snapshot so the page is not blank before the
     * first WebSocket push arrives (avoids a 1-second flash of empty data).
     */
    @GetMapping("/")
    public String dashboard(Model model) {
        model.addAttribute("initialStats", statsService.buildSnapshot());
        model.addAttribute("initialAudit", auditLogService.getRecent());
        return "dashboard";
    }
 
    /**
     * Send a test event from the UI form.
     * Returns JSON so the UI can confirm the send without a full page reload.
     */
    @PostMapping("/send")
    @ResponseBody
    public ResponseEntity<Map<String, String>> sendEvent(
            @RequestParam String sender,
            @RequestParam String content,
            @RequestParam(defaultValue = "INFO") String category) {
 
        if (sender == null || sender.isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("status", "error", "message", "Sender cannot be empty"));
        }
        if (content == null || content.isBlank()) {
            return ResponseEntity.badRequest()
                    .body(Map.of("status", "error", "message", "Content cannot be empty"));
        }
 
        producerService.send(sender.trim(), content.trim(), category);
 
        return ResponseEntity.ok(Map.of(
                "status", "sent",
                "sender", sender.trim(),
                "category", category
        ));
    }
 
    /**
     * REST endpoint for the current stats snapshot — useful for debugging
     * or for curl checks without opening the UI.
     *
     * Example: curl http://localhost:8080/api/stats
     */
    @GetMapping("/api/stats")
    @ResponseBody
    public ResponseEntity<?> getStats() {
        return ResponseEntity.ok(statsService.buildSnapshot());
    }
}