package com.vbforge.case09.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// JUNIOR NOTE: failureMode is the key field for this case's demo.
// The producer endpoint accepts a failureMode param that gets embedded in the message.
// The consumer reads it and throws the corresponding exception type, allowing you to
// observe how DefaultErrorHandler reacts to different exception categories.
//
// Failure modes:
//   "none"         → normal processing, no error
//   "transient"    → RuntimeException — retryable, DefaultErrorHandler will retry
//   "fatal"        → a classified non-retryable exception — skipped immediately, no retries
//   "npe"          → NullPointerException — classified as non-retryable in our config

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TaskMessage {

    private String id;
    private String content;
    private String failureMode;   // "none" | "transient" | "fatal" | "npe" (exception)
    private LocalDateTime timestamp;

}
