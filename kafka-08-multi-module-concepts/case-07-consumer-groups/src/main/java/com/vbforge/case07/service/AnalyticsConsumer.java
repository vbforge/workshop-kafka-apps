package com.vbforge.case07.service;

import com.vbforge.case07.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;


// JUNIOR NOTE: Analytics consumer — one of three independent groups consuming the same topic.
//
// Its job: aggregate order counts and totals by region.
// In a real system this might write to a time-series DB, update a dashboard, or feed a
// stream processor. Here we just accumulate in-memory to keep the demo simple.
//
// Key observation: this consumer's committed offsets are COMPLETELY INDEPENDENT of the
// audit and notify groups. If this consumer is slow or restarts, it processes from its
// own last committed offset — the other groups are unaffected.


@Service
@Slf4j
public class AnalyticsConsumer {

    // ConcurrentHashMap because concurrency=3 means multiple threads call consume()
    private final ConcurrentHashMap<String, Long> orderCountByRegion = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Double> orderTotalByRegion = new ConcurrentHashMap<>();

    @KafkaListener(
            topics = "${kafka.topic.name}",
            groupId = "${kafka.consumer.analyticsGroupId}",
            containerFactory = "analyticsContainerFactory"
    )
    public void consume(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent event = record.value();

        orderCountByRegion.merge(event.getRegion(), 1L, Long::sum);
        orderTotalByRegion.merge(event.getRegion(), event.getAmount(), Double::sum);

        log.info("[ANALYTICS] partition={} offset={} | orderId={} region={} amount={} | counts={}",
                record.partition(), record.offset(),
                event.getOrderId(), event.getRegion(), event.getAmount(),
                orderCountByRegion);
    }

    public ConcurrentHashMap<String, Long> getCounts() { return orderCountByRegion; }
    public ConcurrentHashMap<String, Double> getTotals() { return orderTotalByRegion; }



}













