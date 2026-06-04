package com.vbforge.case07.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// JUNIOR NOTE: Using an "OrderEvent" rather than a generic WorkMessage makes the
// multi-group fan-out story concrete:
//   analytics-group → count orders by region
//   audit-group     → write an immutable audit record
//   notify-group    → send confirmation email / push notification
//
// All three receive the SAME OrderEvent. Each does something completely different with it.
// This is the fan-out pattern — the reason consumer groups exist.

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderEvent {

    private String orderId;
    private String customerId;
    private String region;       // e.g. "EU", "US", "ASIA" — used as Kafka key
    private double amount;
    private String status;       // "PLACED", "PAID", "SHIPPED"
    private LocalDateTime timestamp;

}
