package com.vbforge.case07.service;

import com.vbforge.case07.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

// JUNIOR NOTE: Notify consumer — third of three independent groups.
//
// Its job: trigger customer notifications (email, push, SMS) for each order event.
// In production this calls a notification service or publishes to another topic.
//
// Notifications often care only about specific statuses — e.g. send email only when
// status = "PLACED" or "SHIPPED", not for internal state transitions.
// We demonstrate this with a simple filter, showing that each group can apply
// its own business logic to the same raw event stream.

@Service
@Slf4j
public class NotifyConsumer {

    private final AtomicInteger notificationsSent = new AtomicInteger(0);
    private final AtomicInteger notificationsSkipped = new AtomicInteger(0);

    @KafkaListener(
            topics = "${kafka.topic.name}",
            groupId = "${kafka.consumer.notifyGroupId}",
            containerFactory = "notifyContainerFactory"
    )
    public void consume(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent event = record.value();

        // JUNIOR NOTE: Each consumer group applies its own filtering logic.
        // The analytics group counts everything. The audit group records everything.
        // The notify group only acts on "PLACED" and "SHIPPED" events.
        // Same raw topic, three different behaviors — this is the power of groups.
        if ("PLACED".equals(event.getStatus()) || "SHIPPED".equals(event.getStatus())) {
            notificationsSent.incrementAndGet();
            log.info("[NOTIFY] OK Sending notification | orderId={} customerId={} status={} region={}",
                    event.getOrderId(), event.getCustomerId(),
                    event.getStatus(), event.getRegion());
        } else {
            notificationsSkipped.incrementAndGet();
            log.debug("[NOTIFY] FAILED Skipped status={} orderId={}", event.getStatus(), event.getOrderId());
        }
    }

    public int getSent()    { return notificationsSent.get(); }
    public int getSkipped() { return notificationsSkipped.get(); }

}