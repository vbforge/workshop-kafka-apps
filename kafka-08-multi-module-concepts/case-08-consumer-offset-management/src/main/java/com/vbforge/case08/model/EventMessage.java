package com.vbforge.case08.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class EventMessage {

    private String id;
    private String content;
    private int sequenceNumber; // makes replay easy to observe in logs
    private LocalDateTime timestamp;

}
