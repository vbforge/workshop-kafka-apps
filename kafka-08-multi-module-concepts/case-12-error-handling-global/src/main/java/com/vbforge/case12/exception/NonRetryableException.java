package com.vbforge.case12.exception;

// JUNIOR NOTE: Shared non-retryable marker across all three listeners.
// This is one advantage of a global error handler — one exception classification
// list covers every @KafkaListener in the application. You don't need to register
// non-retryable exceptions separately on each container factory.

public class NonRetryableException extends RuntimeException {

    public NonRetryableException(String message) {
        super(message);
    }

}