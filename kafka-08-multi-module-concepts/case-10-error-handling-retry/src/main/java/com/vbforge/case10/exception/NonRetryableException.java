package com.vbforge.case10.exception;

// JUNIOR NOTE: Same concept as case-09's FatalProcessingException — a non-retryable marker.
// Registered in DefaultErrorHandler so the handler skips retries for this type entirely.
// The distinction from TransientProcessingException (which IS retried) is critical
// for making the backoff demo meaningful: we want to show that transient errors
// eventually succeed after backoff, while permanent errors are discarded immediately.

public class NonRetryableException extends RuntimeException {

    public NonRetryableException(String message) {
        super(message);
    }
}
