package com.vbforge.case09.exception;

// JUNIOR NOTE: We define our own exception hierarchy so we can classify errors
// for DefaultErrorHandler. This is the standard production pattern:
//
//   TransientProcessingException  → retryable (downstream service temporarily down,
//                                   DB connection timeout, network blip)
//   FatalProcessingException      → non-retryable (invalid business rule, schema mismatch,
//                                   data that can never succeed regardless of retries)
//
// DefaultErrorHandler lets you register which exception types are NOT retryable.
// Everything else is retried up to maxAttempts.
// See KafkaConfig for how these map to the handler configuration.

public class TransientProcessingException extends RuntimeException {

    public TransientProcessingException(String message) {
        super(message);
    }
}
