package com.vbforge.case05.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

// JUNIOR NOTE: This case ships a bulk-send endpoint so you can fire 9 messages
// (3 per partition) in one HTTP call and immediately observe concurrent consumption
// in the logs. Without bulk send, the demo is tedious — one curl per message.

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ProducerResponse {


    private int messagesSent;
    private List<MessageSummary> messages;
    private LocalDateTime sentAt;


    //public static class for message summary
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class MessageSummary {
        private String messageId;
        private String key;
        private int partition;
        private long offset;

    }

}
