package com.vbforge.case04.model;

// JUNIOR NOTE: In Kafka, the message key and value are separate fields in the record.
// The key is used by the partitioner to decide which partition to write to.
// The value is the actual payload your consumer reads.
//
// Key → String (e.g. "user-42", "order-99", "sensor-west-1")
// Value → this object (serialized to JSON)
//
// We include the key inside the value object too (as entityKey) for observability —
// so when the consumer logs the message, you can see which key it was keyed on
// without digging into Kafka record metadata.

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class KeyedMessage {

    private String id;                     //unique massage id (UUID)
    private String entityKey;              //mirrors the Kafka record key - for observation
    private String content;
    private LocalDateTime timestamp;

}
