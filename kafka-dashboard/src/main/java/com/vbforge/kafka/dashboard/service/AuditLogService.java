package com.vbforge.kafka.dashboard.service;

import com.vbforge.kafka.dashboard.model.AuditEntry;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Maintains a circular in-memory buffer of the last 50 audit log entries.
 *
 * Fed by the audit consumer group (MANUAL_IMMEDIATE ack mode).
 * Read by the WebSocket broadcaster and the initial page load endpoint.
 */
@Service
public class AuditLogService {

    private static final int MAX_ENTRIES = 50;

    private final LinkedList<AuditEntry> entries = new LinkedList<>();

    /**
     * Appends a new audit entry. Evicts the oldest if over capacity.
     * Synchronized because the audit consumer thread and the WebSocket
     * broadcaster thread both access this.
     */
    public synchronized void add(AuditEntry entry) {
        entries.addFirst(entry);
        if (entries.size() > MAX_ENTRIES) {
            entries.removeLast();
        }
    }

    /** Returns a snapshot copy — safe to iterate without holding the lock. */
    public synchronized List<AuditEntry> getRecent() {
        return new ArrayList<>(entries);
    }

}















