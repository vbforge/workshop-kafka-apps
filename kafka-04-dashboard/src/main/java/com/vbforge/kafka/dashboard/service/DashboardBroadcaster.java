package com.vbforge.kafka.dashboard.service;

import com.vbforge.kafka.dashboard.model.AuditEntry;
import com.vbforge.kafka.dashboard.model.DashboardStats;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
 
import java.util.List;
 
/**
 * Pushes live data to WebSocket subscribers on a fixed schedule.
 *
 * Two channels:
 *   /topic/stats  — full DashboardStats snapshot, every 1 second
 *   /topic/audit  — latest audit log entries, every 1 second
 *
 * LESSON — Push vs Pull:
 * The UI does NOT poll the server with HTTP requests every second.
 * Instead, the server pushes updates over the open WebSocket connection.
 * This is more efficient (no repeated HTTP overhead) and gives true real-time feel.
 *
 * The @Scheduled approach is simple and works well for dashboards.
 * In a higher-scale system you would push directly from the consumer on each
 * message instead of on a timer — but that floods the WebSocket on high throughput.
 * A timer-based approach naturally rate-limits the UI updates.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DashboardBroadcaster {
 
    private final SimpMessagingTemplate messagingTemplate;
    private final DashboardStatsService statsService;
    private final AuditLogService auditLogService;
 
    /**
     * Broadcasts the latest stats snapshot to all subscribed WebSocket clients.
     * Runs every 1000ms on the Spring task scheduler thread pool.
     */
    @Scheduled(fixedRate = 1000)
    public void broadcastStats() {
        DashboardStats snapshot = statsService.buildSnapshot();
        messagingTemplate.convertAndSend("/topic/stats", snapshot);
    }
 
    /**
     * Broadcasts the latest audit log entries.
     * Runs every 1000ms — clients update their live feed table.
     */
    @Scheduled(fixedRate = 1000)
    public void broadcastAuditLog() {
        List<AuditEntry> recent = auditLogService.getRecent();
        messagingTemplate.convertAndSend("/topic/audit", recent);
    }
}