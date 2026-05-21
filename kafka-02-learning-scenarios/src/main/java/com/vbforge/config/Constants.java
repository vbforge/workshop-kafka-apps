package com.vbforge.config;

/**
 * Central constants for all Kafka learning scenarios.
 *
 * Organized by:
 *  - Kafka connection (env-var aware)
 *  - Shared timing/polling defaults
 *  - Per-scenario topics, group IDs, and tuning values
 *
 * Add new scenario constants in the matching section as you build each one.
 * Do NOT scatter topic names or group IDs across individual scenario classes.
 */
public final class Constants {

    private Constants() {}


    // =========================================================================
    // KAFKA CONNECTION
    // Reads KAFKA_BOOTSTRAP_SERVERS env var first, falls back to localhost:9092.
    // This works for all scenarios — Docker always exposes on localhost:9092.
    // Override example:
    //   KAFKA_BOOTSTRAP_SERVERS=myhost:9092 mvn exec:java -Dexec.mainClass="..."
    // =========================================================================

    public static final String BOOTSTRAP_SERVERS = resolveBootstrapServers();

    private static String resolveBootstrapServers() {
        String fromEnv = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        return (fromEnv != null && !fromEnv.isBlank()) ? fromEnv : "localhost:9092";
    }



    // =========================================================================
    // SHARED TIMING & POLLING DEFAULTS
    // Used across all scenarios unless a scenario overrides locally.
    // =========================================================================

    public static final int DEFAULT_POLL_TIMEOUT_MS  = 1000;   // max wait per poll() call
    public static final int DEFAULT_DELAY_MS         = 500;    // delay between producer sends
    public static final int DEFAULT_MESSAGE_COUNT    = 10;     // messages per producer run
    public static final int SEND_TIMEOUT_SEC         = 30;     // sync send timeout



    // =========================================================================
    // SCENARIO 00 — Connectivity Test
    // =========================================================================

    public static final String TOPIC_TEST_CONNECTIVITY  = "topic-test-connectivity-kafka";
    public static final String TEST_CONNECTIVITY_KEY    = "test-key";
    public static final String TEST_CONNECTIVITY_VALUE  = "Hello Docker Kafka!";



    // =========================================================================
    // SCENARIO 01 — Simple Producer & Consumer
    // Async callbacks, shutdown hook + wakeup(), metrics tracking.
    // =========================================================================

    public static final String TOPIC_SIMPLE             = "topic-simple";
    public static final String CONSUMER_GROUP_SIMPLE    = "consumer-group-simple";



    // =========================================================================
    // SCENARIO 02 — Demo App (quick intro: MyProducer, MyConsumer, etc.)
    // Simple synchronous sends, no callbacks, beginner-friendly.
    // =========================================================================

    public static final String TOPIC_DEMO               = "topic-demo";
    public static final String CONSUMER_GROUP_DEMO      = "consumer-group-topic-demo";
    public static final String CONSUMER_GROUP_DEMO_2    = "consumer-group-topic-demo-2"; // experiment: different group



    // =========================================================================
    // SCENARIO 03 — Load Balancing
    // Multiple consumers in same group sharing partitions.
    // Topic needs multiple partitions to observe actual load distribution.
    // =========================================================================

    public static final String TOPIC_LOAD_BALANCE           = "topic-load-balance";        // create with 3+ partitions
    public static final String CONSUMER_GROUP_LOAD_BALANCE  = "consumer-group-load-balance";
    public static final int    LOAD_BALANCE_MESSAGE_COUNT   = 30;                           // enough to spread across partitions



    // =========================================================================
    // SCENARIO 04 — Keyed Messages
    // Same key always routes to same partition (ordering guarantee per key).
    // =========================================================================

    public static final String TOPIC_KEYED              = "topic-keyed";
    public static final String CONSUMER_GROUP_KEYED     = "consumer-group-keyed";



    // =========================================================================
    // SCENARIO 05 — Manual Offset Control
    // Auto-commit disabled; commitSync() / commitAsync() called explicitly.
    // =========================================================================

    public static final String TOPIC_MANUAL_OFFSET = "topic-manual-offset";
    public static final String CONSUMER_GROUP_MANUAL_OFFSET = "consumer-group-manual-offset";



    // =========================================================================
    // SCENARIO 06 — E-Commerce Orders Pipeline
    // Full end-to-end: order placement → payment → fulfillment topics.
    // =========================================================================

    public static final String TOPIC_ECOMMERCE_ORDERS       = "topic-ecommerce-orders";
    public static final String TOPIC_ECOMMERCE_PAYMENTS     = "topic-ecommerce-payments";
    public static final String TOPIC_ECOMMERCE_FULFILLMENT  = "topic-ecommerce-fulfillment";
    public static final String TOPIC_ECOMMERCE_DLQ          = "topic-ecommerce-dlq";       // Dead Letter Queue

    public static final String CONSUMER_GROUP_ECOMMERCE_ORDERS     = "consumer-group-ecommerce-orders";
    public static final String CONSUMER_GROUP_ECOMMERCE_PAYMENTS   = "consumer-group-ecommerce-payments";
    public static final String CONSUMER_GROUP_ECOMMERCE_FULFILL    = "consumer-group-ecommerce-fulfillment";


}
