package com.vbforge.scenario_05_manual_offset_management;

/**
 * Domain-specific exception for order processing failures.
 *
 * Using a typed exception instead of the raw java.lang.Exception allows
 * the consumer to distinguish between:
 *  - A business failure (OrderProcessingException) → don't commit, retry
 *  - An unexpected runtime error (any other Exception) → log and decide separately
 *
 * This matters in ManualCommitConsumer: only OrderProcessingException
 * suppresses the batch commit. Anything else propagates up to the outer
 * catch block and causes the consumer to stop.
 */
public class OrderProcessingException extends Exception {
 
    public OrderProcessingException(String message) {
        super(message);
    }
 
    public OrderProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}