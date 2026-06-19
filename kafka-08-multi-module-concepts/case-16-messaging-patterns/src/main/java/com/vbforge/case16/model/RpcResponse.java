package com.vbforge.case16.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// JUNIOR NOTE: This is what the REST controller returns to the HTTP client.
// It wraps the ReplyMessage plus round-trip timing so you can observe
// how long the full Kafka RPC round trip took.
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class RpcResponse {

    private String requestId;
    private String result;
    private boolean success;
    private long roundTripMs;       // wall-clock time from HTTP request to Kafka reply
    private LocalDateTime completedAt;

}
