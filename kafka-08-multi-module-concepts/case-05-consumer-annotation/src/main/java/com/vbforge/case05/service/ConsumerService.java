package com.vbforge.case05.service;

import com.vbforge.case05.model.WorkMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;


// JUNIOR NOTE: This is THE central lesson of case-05.
//
// @KafkaListener is the Spring Kafka annotation that wires a method to consume
// from a Kafka topic. Under the hood, Spring creates a ConcurrentMessageListenerContainer
// that runs N consumer threads (N = concurrency setting in KafkaConfig).
//
// Each thread:
//   - Is an independent Kafka consumer instance within the same consumer group
//   - Owns one or more partitions (Kafka assigns them via group rebalance)
//   - Calls this method sequentially for each record it polls
//
// Key things to observe in the logs when you send 9 messages across 3 partitions:
//
//   1. THREAD NAMES: Messages from different partitions are processed by different threads.
//      Thread name pattern: "case-05-consumer-group-0-C-1", "-C-2", "-C-3"
//      Each "-C-N" is a separate consumer thread.
//
//   2. PARALLELISM: All three threads can be active simultaneously — logs from different
//      threads interleave. This is concurrent consumption in action.
//
//   3. PARTITION AFFINITY: All messages with the same key (same partition) always arrive
//      on the same thread. Within a partition, order is preserved.
//      Across partitions (different threads), order is NOT guaranteed.
//
//   4. COUNTER: The AtomicInteger totalProcessed is incremented by all threads concurrently.
//      AtomicInteger is used (not int) because multiple threads share this field.
//      This demonstrates the thread-safety concern you must handle in concurrent consumers.


@Service
@Slf4j
public class ConsumerService {

    // JUNIOR NOTE: AtomicInteger — not int, not Integer.
    // Three consumer threads call consume() concurrently. If this were a plain int,
    // two threads could read the same value, both increment locally, and write back —
    // losing one increment. AtomicInteger.incrementAndGet() is an atomic CAS operation:
    // it's guaranteed to be thread-safe without synchronized blocks.
    private final AtomicInteger totalProcessed = new AtomicInteger(0);

    @KafkaListener(
            topics = "${kafka.topic.name}",
            containerGroup = "${kafka.consumer.groupID}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumer(ConsumerRecord<String, WorkMessage> record) {

        int incrementedCounter = totalProcessed.incrementAndGet();

        // JUNIOR NOTE: Thread.currentThread().getName() is the proof of concurrent consumption.
        // With concurrency=3 and 3 partitions, you'll see three different thread names
        // processing messages simultaneously. This is what makes Kafka horizontally scalable —
        // you can increase concurrency (and partition count) to add more processing power
        // without changing any business logic.
        String threadName = Thread.currentThread().getName();

        WorkMessage messageValue = record.value();

        log.info("----------------------------------------");
        log.info(" Thread:    {}", threadName);
        log.info(" Partition: {}  |  Offset: {}", record.partition(), record.offset());
        log.info(" Key:       {}", record.key());
        log.info(" ID:        {}", messageValue.getId());
        log.info(" Content:   {}", messageValue.getContent());
        log.info(" Total processed so far: {}", incrementedCounter);
        log.info("----------------------------------------");


    }







}

























