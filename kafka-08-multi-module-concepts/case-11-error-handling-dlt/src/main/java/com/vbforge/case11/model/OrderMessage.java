package com.vbforge.case11.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
 
import java.time.LocalDateTime;
 
// JUNIOR NOTE: Using OrderMessage makes the DLT story concrete.
// A failed payment processing attempt landing in a DLT is a real-world scenario
// with real consequences — much more meaningful than a generic "WorkMessage".
//
// failureMode drives what the consumer does:
//   "none"         → processes cleanly
//   "transient"    → fails every attempt → retries exhausted → goes to DLT
//   "non-retryable"→ fails immediately → no retries → goes to DLT instantly
 
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OrderMessage {
 
    private String orderId;
    private String customerId;
    private double amount;
    private String failureMode;   // "none" | "transient" | "non-retryable"
    private LocalDateTime timestamp;
 
}