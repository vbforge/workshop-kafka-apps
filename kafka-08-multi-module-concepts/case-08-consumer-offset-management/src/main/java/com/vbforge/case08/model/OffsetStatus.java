package com.vbforge.case08.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class OffsetStatus {

    private String consumerState;           // RUNNING, PAUSED, STOPPED
    private long totalProcessed;
    private long totalCommitted;

    // JUNIOR NOTE: partition → current position map.
    // Shows exactly where the consumer is in each partition right now.
    // This is what makes offset management tangible — you can watch positions advance,
    // then see them jump back after a seekToBeginning().
    private Map<Integer, Long> currentPositions;   // partition → next-to-fetch offset
    private Map<Integer, Long> committedOffsets;   // partition → last committed offset

    private LocalDateTime checkedAt;

}
