package com.vbforge.scenario_06_e_commerce_orders_app;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.LocalDateTime;

/**
 * Order — domain model serialized to/from JSON as the Kafka message value.
 *
 * Jackson serialization notes:
 *  - No-arg constructor required by Jackson for deserialization
 *  - @JsonIgnoreProperties: if a consumer receives an Order with extra fields
 *    (e.g. added in a later producer version), deserialization won't fail
 *  - LocalDateTime support comes from JavaTimeModule registered in Utility.getObjectMapper()
 *
 * Key is always userId (set in OrderService) so all orders for the same user
 * are routed to the same partition — per-user ordering guaranteed.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Order {

    private String orderId;
    private String userId;
    private String productId;
    private int quantity;
    private double totalAmount;
    private String status;
    private LocalDateTime timestamp;

    // Required by Jackson for deserialization
    public Order() {}

    public Order(String orderId, String userId, String productId,
                 int quantity, double totalAmount, String status) {
        this.orderId     = orderId;
        this.userId      = userId;
        this.productId   = productId;
        this.quantity    = quantity;
        this.totalAmount = totalAmount;
        this.status      = status;
        this.timestamp   = LocalDateTime.now();
    }

    public String getOrderId()      { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getUserId()       { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getProductId()    { return productId; }
    public void setProductId(String productId) { this.productId = productId; }

    public int getQuantity()        { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public double getTotalAmount()  { return totalAmount; }
    public void setTotalAmount(double totalAmount) { this.totalAmount = totalAmount; }

    public String getStatus()       { return status; }
    public void setStatus(String status) { this.status = status; }

    public LocalDateTime getTimestamp() { return timestamp; }
    public void setTimestamp(LocalDateTime timestamp) { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return String.format("Order[id=%s, user=%s, product=%s, qty=%d, amount=$%.2f, status=%s]",
                orderId, userId, productId, quantity, totalAmount, status);
    }
}
