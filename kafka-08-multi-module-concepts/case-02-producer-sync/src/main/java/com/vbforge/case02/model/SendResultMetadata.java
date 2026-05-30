package com.vbforge.case02.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SendResultMetadata {

    private MyMessageObject message;
    private int partition;
    private long offset;
    private long brokerTimestamp;
    private long sendDurationMs;
    private LocalDateTime respondedAt;

}
