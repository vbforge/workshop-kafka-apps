package com.vbforge.case12.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
 
import java.time.LocalDateTime;
 
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ProducerResponse {
 
    private String messageId;
    private String topic;
    private String failureMode;
    private int partition;
    private long offset;
    private LocalDateTime sentAt;
 
}