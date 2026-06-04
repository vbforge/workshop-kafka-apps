package com.vbforge.case10.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// JUNIOR NOTE: succeedOnAttempt is the key addition vs case-09.
// It lets us simulate the most important retry scenario:
// "the message fails transiently N times, then succeeds on attempt N+1."
//
// Example: succeedOnAttempt=3 means:
//   Attempt 1 → throws TransientProcessingException
//   Wait initialInterval * multiplier^0 = 500ms
//   Attempt 2 → throws TransientProcessingException
//   Wait initialInterval * multiplier^1 = 1000ms
//   Attempt 3 → succeeds! returns normally. offset committed.
//
// This demonstrates that ExponentialBackOff is not just a "retry until you give up"
// mechanism — it's "retry with increasing patience until the transient condition clears."
// The downstream service that was briefly unavailable at attempt 1 is back by attempt 3.

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TaskMessage {

    private String id;
    private String content;
    private String failureMode;      // "always-fail" | "eventually-succeed" | "non-retryable"
    private int succeedOnAttempt;    // 0 = never succeed (exhaust retries); N = succeed on attempt N
    private LocalDateTime timestamp;

}
