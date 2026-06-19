package com.vbforge.case14.model;

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

    private String orderId;
    private String topicSentTo;
    private int partition;
    private long offset;
    private LocalDateTime sentAt;

}
