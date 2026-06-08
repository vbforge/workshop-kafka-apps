package com.vbforge.case13.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// JUNIOR NOTE: OrderMessage is the domain object flowing through the transactional pipeline.
// The scenario: an order arrives on case-13-orders-topic.
// The consumer reads it, processes it, and uses a transactional producer to write
// the result to case-13-processed-topic AND a probe record to case-13-probe-topic.
// If rollback=true, both writes are rolled back atomically — neither topic receives
// anything despite the producer having called send() for both.

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderMessage {

    private String orderId;
    private String customerId;
    private double amount;
    private boolean rollback;   // if true — triggers rollback mid-transaction
    private LocalDateTime timestamp;

}
