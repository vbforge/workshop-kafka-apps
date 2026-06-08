package com.vbforge.case13.service;

import com.vbforge.case13.model.OrderMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

// JUNIOR NOTE: This consumer listens to case-13-orders-topic.
// It receives the initial order (sent via POST /send) and delegates to the
// transactional producer service to do the actual transactional processing.
//
// Notice: the consumer itself has no transaction config.
// The transaction is owned by TransactionalProducerService — that's where
// @Transactional lives. The consumer just triggers it.
//
// In the full consume-process-produce (CPP) transactional pattern, the consumer
// offset commit would be part of the SAME Kafka transaction as the produce steps.
// That would use sendOffsetsToTransaction() and require AckMode.MANUAL.
// This demo keeps that complexity in the THEORY doc and focuses on demonstrating
// what rollback looks like on the producer side.

@Service
@Slf4j
public class OrderConsumerService {

    private final TransactionalProducerService transactionalProducerService;
    private final AtomicInteger processedCount = new AtomicInteger(0);
    private final AtomicInteger rolledBackCount = new AtomicInteger(0);

    public OrderConsumerService(TransactionalProducerService transactionalProducerService) {
        this.transactionalProducerService = transactionalProducerService;
    }

    @KafkaListener(
            topics = "${kafka.topic.orders}",
            groupId = "${kafka.consumer.groupID}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, OrderMessage> record) {
        OrderMessage order = record.value();

        log.info(">>> [ORDER-CONSUMER] partition={} offset={} orderId={} rollback={}",
                record.partition(), record.offset(), order.getOrderId(), order.isRollback());

        if (order.isRollback()) {
            // JUNIOR NOTE: sendRolledBack() throws RuntimeException after its sends.
            // Spring's @Transactional on that method catches the exception and aborts the transaction.
            // From the consumer's perspective: this try/catch prevents the listener from
            // crashing its container — we handle the expected rollback gracefully.
            try {
                transactionalProducerService.sendRolledBack(order.getAmount());
            } catch (RuntimeException e) {
                rolledBackCount.incrementAndGet();
                log.warn(">>> [ORDER-CONSUMER] Transaction rolled back for orderId={} — expected in demo. ex={}",
                        order.getOrderId(), e.getMessage());
            }
        } else {
            transactionalProducerService.sendCommitted(order.getAmount());
            processedCount.incrementAndGet();
            log.info(">>> [ORDER-CONSUMER] ✓ Transaction committed for orderId={}", order.getOrderId());
        }
    }

    public int getProcessedCount()   { return processedCount.get(); }
    public int getRolledBackCount()  { return rolledBackCount.get(); }

}
