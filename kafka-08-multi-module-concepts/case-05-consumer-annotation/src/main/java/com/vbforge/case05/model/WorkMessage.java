package com.vbforge.case05.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class WorkMessage {

    private String id;
    private String key;       // mirrors the Kafka record key — for partition observability
    private String content;
    private LocalDateTime timestamp;

}
