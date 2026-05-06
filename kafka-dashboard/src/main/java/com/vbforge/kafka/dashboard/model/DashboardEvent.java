package com.vbforge.kafka.dashboard.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * The event payload published to the dashboard-events topic.
 *
 * This is serialized to JSON by the producer (JsonSerializer)
 * and deserialized back by both consumer groups (JsonDeserializer).
 *
 * Why a structured object instead of a plain String?
 * Because real Kafka applications always send structured payloads.
 * A plain String producer is fine for Project 1 (Echo Bot) but from
 * Project 2 onwards we work the way production code works.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DashboardEvent {

    /**
     * Who sent this event. Used for the "top senders" aggregation.
     */
    private String sender;

    /**
     * The message content.
     */
    private String content;

    /**
     * Category — lets us aggregate by event type (e.g. INFO, WARN, ERROR).
     */
    private String category;

    /**
     * Set by the producer at send time.
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime sentAt;

}
