package com.vbforge.case10.model;

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
    private int succeedOnAttempt;
    private int partition;
    private long offset;
    private LocalDateTime sentAt;

}
