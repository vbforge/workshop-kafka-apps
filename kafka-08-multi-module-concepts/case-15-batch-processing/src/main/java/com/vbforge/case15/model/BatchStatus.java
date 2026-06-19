package com.vbforge.case15.model;

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
public class BatchStatus {

    // JUNIOR NOTE: These counters make the batch behaviour observable via the /status endpoint.
    // Watch batchCount increase slowly (one per poll), while totalProcessed jumps in chunks.
    // That's batch processing in action — fewer commits, larger units of work.
    private long totalReceived;        // individual records received across all batches
    private long totalCommitted;       // records whose offsets have been committed
    private long batchCount;           // how many poll() batches have been processed
    private double avgBatchSize;       // totalReceived / batchCount — shows real batch sizes
    private List<BatchSummary> recentBatches;  // last N batches for observability
    private LocalDateTime checkedAt;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class BatchSummary {
        private int batchNumber;
        private int size;
        private long processingMs;       // how long the batch took to process
        private String firstOffset;      // "partition=0 offset=5"
        private String lastOffset;       // "partition=0 offset=54"
        private LocalDateTime completedAt;
    }

}
