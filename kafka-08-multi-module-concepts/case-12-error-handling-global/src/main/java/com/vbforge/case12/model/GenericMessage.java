package com.vbforge.case12.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
 
import java.time.LocalDateTime;
 
// JUNIOR NOTE: A single GenericMessage is consumed by three different listeners
// (orders, payments, notifications). This keeps the demo focused on the global
// error handler infrastructure rather than domain modelling.
// In production each topic would have its own strongly-typed model.
 
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class GenericMessage {
 
    private String id;
    private String type;          // "order" | "payment" | "notification"
    private String content;
    private String failureMode;   // "none" | "transient" | "non-retryable"
    private LocalDateTime timestamp;
 
}