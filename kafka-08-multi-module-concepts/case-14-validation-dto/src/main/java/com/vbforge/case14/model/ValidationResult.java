package com.vbforge.case14.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ValidationResult {

    // JUNIOR NOTE: This model captures both the outcome and the reasons, so the
    // REST response and the rejected-topic message carry the same structured info.
    private boolean valid;
    private String orderId;
    private List<String> violations;  // one entry per constraint violation
    private LocalDateTime processedAt;

}
