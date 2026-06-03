package com.vbforge.case06.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;


// JUNIOR NOTE: In manual poll, YOU own the consumer loop — so YOU can expose its state.
// With @KafkaListener (case-05) the loop was inside Spring's container, invisible.
// Here we surface it: is the loop running? paused? how many records processed?
// This is one of the practical advantages of manual poll — full observability and control.

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ConsumerStatus {

    private boolean running;
    private boolean paused;
    private long totalProcessed;
    private long totalCommitted;
    private String currentState;   // "RUNNING", "PAUSED", "STOPPED"
    private LocalDateTime checkedAt;


}









