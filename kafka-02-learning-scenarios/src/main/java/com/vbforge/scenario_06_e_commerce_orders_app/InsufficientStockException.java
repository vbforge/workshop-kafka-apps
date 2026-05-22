package com.vbforge.scenario_06_e_commerce_orders_app;

/**
 * Thrown when an order requests more stock than is available.
 * InventoryService catches this to skip commitSync() and trigger redelivery.
 */
public class InsufficientStockException extends Exception {

    private final String productId;
    private final int requested;
    private final int available;

    public InsufficientStockException(String productId, int requested, int available) {
        super(String.format("Insufficient stock for %s: requested=%d, available=%d",
                productId, requested, available));
        this.productId = productId;
        this.requested = requested;
        this.available = available;
    }

    public String getProductId() { return productId; }
    public int getRequested()    { return requested; }
    public int getAvailable()    { return available; }
}