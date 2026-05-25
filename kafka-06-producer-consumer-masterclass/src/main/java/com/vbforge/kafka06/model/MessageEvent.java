package com.vbforge.kafka06.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * The event object that travels through Kafka topics.
 *
 * <p>Spring Kafka serializes this to JSON on the producer side and
 * deserializes it back to a {@code MessageEvent} on the consumer side
 * using {@code JsonSerializer} / {@code JsonDeserializer}.
 *
 * <p>We use Lombok here to avoid writing boilerplate:
 * <ul>
 *   <li>{@code @Data}          — generates getters, setters, equals, hashCode, toString</li>
 *   <li>{@code @Builder}       — gives us a fluent builder: {@code MessageEvent.builder()...build()}</li>
 *   <li>{@code @NoArgsConstructor} — required by Jackson for JSON deserialization</li>
 *   <li>{@code @AllArgsConstructor} — required by {@code @Builder} internally</li>
 * </ul>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MessageEvent {

    /**
     * Logical category of the message (e.g. "order", "notification", "audit").
     * Maps to the Kafka message key in keyed-send scenarios.
     */
    private String category;

    /** Wall-clock time when this event was created on the producer side. */
    private LocalDateTime createdAt;

    /** Human-readable payload — kept as a String for simplicity in this demo. */
    private String payload;
}
