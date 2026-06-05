package com.vbforge.case11.service;

import com.vbforge.case11.model.OrderMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
 
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
 
// JUNIOR NOTE: This is the DLT consumer — the other half of the DLT story.
//
// When DeadLetterPublishingRecoverer publishes a failed record to the DLT, it enriches it
// with kafka_dlt-* headers. This consumer reads those headers and surfaces them in logs,
// demonstrating what operators would see in a real system.
//
// In production, a DLT consumer typically:
//   1. Alerts on-call: "ORDER processing failed — orderId=X, exception=Y, original-offset=Z"
//   2. Writes to an incident/ops database for tracking
//   3. Exposes a UI for manual review and replay decisions
//   4. Optionally auto-replays to a retry topic after a code fix is deployed
//
// The key DLT headers added by Spring Kafka's DeadLetterPublishingRecoverer:
//   kafka_dlt-original-topic         → which topic the message originally came from
//   kafka_dlt-original-partition      → which partition
//   kafka_dlt-original-offset         → exact offset — enables surgical replay
//   kafka_dlt-original-consumer-group → which group failed to process it
//   kafka_dlt-exception-fqcn          → fully qualified exception class name
//   kafka_dlt-exception-message       → exception message string
//   kafka_dlt-exception-stacktrace    → full stack trace as UTF-8 bytes (long!)
//
// @DltHandler annotation:
//   This is an alternative to a plain @KafkaListener for DLT consumers.
//   When used alongside @KafkaListener on the same class, Spring Kafka routes
//   DLT records to the @DltHandler method automatically. We use a separate class
//   here (more explicit, easier to reason about in a learning context),
//   but the @DltHandler pattern is shown in a comment below for reference.
 
@Service
@Slf4j
public class DltConsumerService {
 
    private final AtomicInteger dltReceivedCount = new AtomicInteger(0);
 
    // JUNIOR NOTE: containerFactory = "dltContainerFactory" — this is critical.
    // It MUST use the DLT factory (with dltGroupId, no error handler).
    // Using kafkaListenerContainerFactory by mistake would:
    //   a) join the main consumer group — wrong group, wrong offsets
    //   b) have an error handler that re-routes failures to DLT again — infinite loop
    @KafkaListener(
            topics = "${kafka.topic.dlt}",
            groupId = "${kafka.consumer.dltGroupID}",
            containerFactory = "dltContainerFactory"
    )
    public void consumeFromDlt(ConsumerRecord<String, OrderMessage> record) {
        int count = dltReceivedCount.incrementAndGet();
        OrderMessage order = record.value();
 
        log.error("╔══════════════════════════════════════════");
        log.error("║  DLT RECORD #{} RECEIVED", count);
        log.error("║  orderId:   {}", order.getOrderId());
        log.error("║  amount:    {}", order.getAmount());
        log.error("║  customer:  {}", order.getCustomerId());
        log.error("╠── DLT Headers ──────────────────────────");
 
        // JUNIOR NOTE: Iterate all headers — surface the kafka_dlt-* diagnostic headers.
        // Header values are raw bytes — we decode as UTF-8 strings for readability.
        // The stacktrace header can be very long; we truncate to 300 chars for logs.
        for (Header header : record.headers()) {
            String key   = header.key();
            String value = new String(header.value(), StandardCharsets.UTF_8);
 
            if (key.startsWith("kafka_dlt-")) {
                if (key.equals("kafka_dlt-exception-stacktrace")) {
                    // Truncate stack trace — it can be thousands of chars
                    log.error("║  {}: {}...", key,
                            value.length() > 300 ? value.substring(0, 300) : value);
                } else {
                    log.error("║  {}: {}", key, value);
                }
            }
        }
 
        log.error("╚══════════════════════════════════════════");
 
        // In production: persist to incident DB, trigger alert, etc.
    }
 
    // ===== @DltHandler alternative pattern (for reference) =====
    // If OrderConsumerService had both @KafkaListener and @DltHandler on the same class:
    //
    //   @DltHandler
    //   public void handleDlt(OrderMessage order, @Header KafkaHeaders.ORIGINAL_TOPIC String originalTopic) {
    //       log.error("DLT: order={} from topic={}", order.getOrderId(), originalTopic);
    //   }
    //
    // Spring Kafka then automatically routes DLT records to that method
    // when the @KafkaListener topic has a corresponding .DLT topic configured.
    // We use a separate class here for clarity.
 
    public int getDltReceivedCount() { return dltReceivedCount.get(); }
 
}