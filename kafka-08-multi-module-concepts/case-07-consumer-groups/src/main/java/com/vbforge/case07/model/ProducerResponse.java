package com.vbforge.case07.model;

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
public class ProducerResponse {

    private int messagesSent;
    private List<MessageSummary> messages;
    private LocalDateTime sentAt;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class MessageSummary {
        private String orderId;
        private String region;
        private int partition;
        private long offset;
    }

}
