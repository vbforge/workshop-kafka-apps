package com.vbforge.case13.service;

import com.vbforge.case13.model.OrderMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

// JUNIOR NOTE: This consumer is the PROOF that transactions work.
//
// It listens to case-13-processed-topic with isolation.level=read_committed.
// The key demo observation:
//   - When sendCommitted() runs: this consumer RECEIVES the record.
//   - When sendRolledBack() runs: this consumer NEVER receives the record,
//     even though the producer called send() and got back a real offset.
//
// That "real offset but invisible" behaviour is the whole point of Kafka transactions.
// The record exists in the broker's log but its transaction was aborted, so
// read_committed consumers skip it. They see the ABORT control batch marker and
// advance past those records without delivering them to the application.
//
// Without isolation.level=read_committed, this consumer would see the record
// BEFORE the transaction commits — and then it would "disappear" if rolled back.
// That's read_uncommitted behaviour: you process a ghost record.

@Service
@Slf4j
public class ProcessedConsumerService {

    private final AtomicInteger receivedCount = new AtomicInteger(0);

    @KafkaListener(
            topics = "${kafka.topic.processed}",
            groupId = "${kafka.consumer.groupID}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, OrderMessage> record) {
        OrderMessage order = record.value();
        int count = receivedCount.incrementAndGet();

        // JUNIOR NOTE: If you ever see a rollback=true order here,
        // it means read_committed is NOT configured correctly.
        // In a correctly configured system, only committed orders appear here.
        if (order.isRollback()) {
            log.error(">>> [PROCESSED-CONSUMER] ⚠ BUG: received a rollback=true order — " +
                      "isolation.level is probably not read_committed! orderId={}", order.getOrderId());
        } else {
            log.info(">>> [PROCESSED-CONSUMER] ✓ Received committed record #{} orderId={} amount={}",
                    count, order.getOrderId(), order.getAmount());
        }
    }

    public int getReceivedCount() { return receivedCount.get(); }

}
