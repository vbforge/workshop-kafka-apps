package com.vbforge.case16.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// JUNIOR NOTE: This is the REPLY half of the RPC pair.
// The SERVER sends this to case-16-reply-topic after processing a RequestMessage.
// The CLIENT's ReplyingKafkaTemplate.sendAndReceive() call unblocks when this arrives,
// matched by the correlation ID header that was set on the original request.
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ReplyMessage {

    private String requestId;       // echoed from the original request — for tracing
    private String result;
    private boolean success;
    private String errorMessage;    // non-null only when success=false
    private LocalDateTime processedAt;

}
