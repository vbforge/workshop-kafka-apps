package com.vbforge.case16.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// JUNIOR NOTE: This is the REQUEST half of the RPC pair.
// The CLIENT sends this to case-16-request-topic.
// The SERVER receives it, does work, and sends a ReplyMessage back.
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RequestMessage {

    private String requestId;
    private String payload;
    private String priority;        // "HIGH" | "STANDARD" — used for header-based routing demo
    private LocalDateTime sentAt;

}
