package com.vbforge.kafka.dashboard.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * A single entry in the raw audit log.
 * Produced by the audit consumer group — it records every event with
 * full metadata: partition, offset, and when it was consumed.
 *
 * This is what demonstrates partition awareness visually in the UI:
 * you can see "partition 0 | offset 5 | sender: Alice | content: hello"
 * and understand where in the topic this message physically lives.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AuditEntry {

    private String sender;
    private String content;
    private String category;

    /** Which partition this message came from. Key concept for this project. */
    private int partition;

    /** The offset of this message within its partition. */
    private long offset;

    /** When the audit consumer received and acknowledged this message. */
    private LocalDateTime consumedAt;

}
