package com.vbforge.case04.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

// JUNIOR NOTE: We use sync send (.get()) in this case so we can return the broker-confirmed
// partition directly in the HTTP response. That's the whole point of the demo:
// you call the endpoint with the same key multiple times and watch the same partition
// come back every time. This makes the key → partition routing tangible and observable.


@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class KeyedSendResult {

    private String messageId;
    private String key;                          // the Kafka record key used for routing
    private String content;
    private int partition;                       // broker-confirmed partition the key hashed to
    private long offset;                         // position within that partition
    private long brokerTimestamp;
    private long sendDurationMs;
    private LocalDateTime respondedAt;



}
