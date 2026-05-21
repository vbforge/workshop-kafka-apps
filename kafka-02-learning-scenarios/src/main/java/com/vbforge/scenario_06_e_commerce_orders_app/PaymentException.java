package com.vbforge.scenario_06_e_commerce_orders_app;

/**
 * Custom exception for payment processing failures.
 * Used by PaymentService to simulate payment gateway errors.
 */
public class PaymentException extends Exception {

    private final String orderId;
    private final String reason;

    public PaymentException(String message) {
        super(message);
        this.orderId = extractOrderId(message);
        this.reason = message;
    }

    public PaymentException(String message, String orderId) {
        super(message);
        this.orderId = orderId;
        this.reason = message;
    }

    private String extractOrderId(String message) {
        // Extract order ID from message if present
        if (message != null && message.contains("order: ")) {
            String[] parts = message.split("order: ");
            if (parts.length > 1) {
                return parts[1].split(" ")[0];
            }
        }
        return "unknown";
    }

    public String getOrderId() {
        return orderId;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        return String.format("PaymentException{orderId='%s', reason='%s'}", orderId, reason);
    }
}
