package com.vbforge.case09.model;

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
    private String failureMode;
    private int partition;
    private long offset;
    private LocalDateTime sentAt;

}
