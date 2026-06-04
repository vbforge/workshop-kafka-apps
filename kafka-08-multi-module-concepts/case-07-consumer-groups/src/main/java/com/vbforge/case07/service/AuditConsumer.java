package com.vbforge.case07.service;

import com.vbforge.case07.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// JUNIOR NOTE: Audit consumer — second of three independent groups.
//
// Its job: maintain an immutable audit trail of every order event.
// In production this writes to an append-only audit table or an immutable log store.
// Audit consumers are often deliberately slower (they write to cold storage) —
// the point is they don't slow down the analytics or notify groups at all,
// because each group has its own independent offset tracking and thread pool.
//
// The audit log here is just an in-memory list — real impl would be a DB write.

@Service
@Slf4j
public class AuditConsumer {

    // Collections.synchronizedList because multiple consumer threads append to it
    private final List<String> auditLog = Collections.synchronizedList(new ArrayList<>());

    @KafkaListener(
            topics = "${kafka.topic.name}",
            groupId = "${kafka.consumer.auditGroupId}",
            containerFactory = "auditContainerFactory"
    )
    public void consume(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent event = record.value();

        String entry = String.format("AUDIT | partition=%d offset=%d | orderId=%s customerId=%s region=%s amount=%.2f status=%s ts=%s",
                record.partition(), record.offset(),
                event.getOrderId(), event.getCustomerId(),
                event.getRegion(), event.getAmount(),
                event.getStatus(), event.getTimestamp());

        auditLog.add(entry);
        log.info("[AUDIT] {}", entry);
    }

    public List<String> getAuditLog() { return Collections.unmodifiableList(auditLog); }

}