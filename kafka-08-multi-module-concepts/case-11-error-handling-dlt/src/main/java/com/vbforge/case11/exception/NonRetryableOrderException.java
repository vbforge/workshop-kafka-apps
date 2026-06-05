package com.vbforge.case11.exception;

// JUNIOR NOTE: Non-retryable marker — registered in DefaultErrorHandler so failed
// messages of this type skip the retry loop entirely and go straight to the DLT.
// Example real cases: invalid order amount, unknown customer ID, schema version mismatch.

public class NonRetryableOrderException extends RuntimeException {

    public NonRetryableOrderException(String message) {
        super(message);
    }

}