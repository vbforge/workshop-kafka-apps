package com.vbforge.scenario_06_e_commerce_orders_app;

/**
 * Custom exception for payment processing failures.
 * Used by PaymentService to simulate payment gateway errors.
 */
public class PaymentException extends Exception {

    private final String orderId;

    // Primary constructor — always provide orderId explicitly.
    // Never parse it out of a message string; that's fragile and untestable.
    public PaymentException(String orderId, String message) {
        super(message);
        this.orderId = orderId;
    }

    public String getOrderId() { return orderId; }

    @Override
    public String toString() {
        return String.format("PaymentException{orderId='%s', message='%s'}", orderId, getMessage());
    }
}
