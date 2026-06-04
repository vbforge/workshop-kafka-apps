package com.vbforge.case09.exception;

// JUNIOR NOTE: FatalProcessingException signals that retrying will never help.
// Examples: the message payload violates a business invariant, the referenced entity
// doesn't exist and never will, the schema version is incompatible.
//
// When DefaultErrorHandler sees a non-retryable exception (registered via
// addNotRetryableExceptions), it skips straight to the recovery action
// (log + commit past it) without burning through maxAttempts.
// This prevents wasting retry budget on errors that can't self-heal.

public class FatalProcessingException extends RuntimeException {

    public FatalProcessingException(String message) {
        super(message);
    }
}
