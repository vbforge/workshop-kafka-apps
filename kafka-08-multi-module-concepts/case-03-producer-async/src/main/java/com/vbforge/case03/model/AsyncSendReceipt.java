package com.vbforge.case03.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// JUNIOR NOTE: In async sends the HTTP response goes back BEFORE Kafka ACKs.
// So we can't include broker-confirmed partition/offset in the immediate response —
// we simply don't have that info yet.
//
// Instead, we return an "accepted" receipt: message ID, content, and accepted timestamp.
// The actual broker confirmation is handled separately via callbacks or CompletableFuture
// chaining (logged server-side, or returned via a different mechanism like WebSocket/polling).
//
// This is the key UX difference vs case-02:
//   case-02 → HTTP response contains partition + offset (blocking until broker confirms)
//   case-03 → HTTP response contains just the acceptance receipt (non-blocking)

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AsyncSendReceipt {

    private String messageId;
    private String content;
    private LocalDateTime acceptedAt;
    private String status; // "ACCEPTED" — message handed to Kafka client buffer


}
