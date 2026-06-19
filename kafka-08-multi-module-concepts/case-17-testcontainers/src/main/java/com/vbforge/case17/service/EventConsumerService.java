package com.vbforge.case17.service;

import com.vbforge.case17.model.EventMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

// JUNIOR NOTE: This service is deliberately designed to be TESTABLE.
// Two design decisions that make integration testing possible:
//
// 1. CountDownLatch for synchronization:
//    Kafka consumption is asynchronous — the @KafkaListener runs on a
//    background thread. Tests running on the main thread need to WAIT
//    for messages to arrive before asserting. CountDownLatch.await() blocks
//    the test thread until all expected messages are consumed.
//
//    resetLatch(N) lets each test configure exactly how many messages to
//    wait for. This is the standard pattern for testing Kafka consumers.
//
// 2. In-memory received list:
//    We keep a list of every received EventMessage so tests can assert
//    on the actual content: payload, type, ordering, count, etc.
//    In production you'd process and discard; in tests you need to inspect.
//
// These two features (latch + received list) are the minimal test harness
// a @KafkaListener service needs to be integration-testable without mocks.

@Service
@Slf4j
public class EventConsumerService {

    private final AtomicLong totalReceived = new AtomicLong(0);

    // JUNIOR NOTE: The list is synchronized because @KafkaListener runs on
    // a thread pool, and the test thread calls getReceivedEvents() concurrently.
    // Alternatively use CopyOnWriteArrayList — both work for this pattern.
    private final List<EventMessage> receivedEvents = Collections.synchronizedList(new ArrayList<>());

    // JUNIOR NOTE: volatile so the test thread sees the latest latch set by resetLatch().
    private volatile CountDownLatch latch = new CountDownLatch(1);

    @KafkaListener(
            topics = "${kafka.topics.events}",
            groupId = "${kafka.consumer.groupID}",
            containerFactory = "listenerContainerFactory"
    )
    public void consume(EventMessage event) {
        totalReceived.incrementAndGet();
        receivedEvents.add(event);
        log.info(">>> [CONSUMER] Received event id={} type={} seq={}",
                event.getId(), event.getType(), event.getSequenceNumber());
        latch.countDown();  // signal one more message has arrived
    }

    // ── Test helpers ──

    public void resetLatch(int expectedCount) {
        // JUNIOR NOTE: Call this BEFORE sending messages in your test.
        // CountDownLatch is not reusable — create a new one for each test scenario.
        receivedEvents.clear();
        latch = new CountDownLatch(expectedCount);
        log.debug(">>> [CONSUMER] Latch reset to {}", expectedCount);
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public List<EventMessage> getReceivedEvents() {
        return Collections.unmodifiableList(receivedEvents);
    }

    public long getTotalReceived() {
        return totalReceived.get();
    }

}
