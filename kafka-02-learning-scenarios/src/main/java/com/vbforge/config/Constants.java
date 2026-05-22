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
    public static final int GRACEFUL_SHUTDOWN_TIMEOUT_SEC = 10;
    public static final int PRODUCER_CLOSE_TIMEOUT_SEC = 5;


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
    // Full end-to-end: order placement → payment → fulfillment topics. // <-- todo
    // =========================================================================

    // ===== E-COMMERCE TOPIC =====
    public static final String TOPIC_ECOMMERCE_ORDERS       = "topic-ecommerce-orders";

    // ===== E-COMMERCE DLQ =====
    public static final String TOPIC_ECOMMERCE_DLQ     = "topic-ecommerce-orders-dlq";
    public static final int    MAX_PAYMENT_RETRIES      = 3;

    // ===== E-COMMERCE GROUPS =====
    public static final String CONSUMER_GROUP_ECOMMERCE_PAYMENTS = "payment-service-group";
    public static final String CONSUMER_GROUP_ECOMMERCE_INVENTORY = "inventory-service-group";
    public static final String CONSUMER_GROUP_ECOMMERCE_NOTIFICATIONS = "notification-service-group";
    public static final String CONSUMER_GROUP_ECOMMERCE_ANALYTICS = "analytics-service-group";

    // ===== E-COMMERCE SPECIFIC =====
    public static final int DEFAULT_ORDER_COUNT = 15;
    public static final int ORDER_PRODUCER_DELAY_MS = 1000;
    public static final int PAYMENT_PROCESSING_DELAY_MS = 500;
    public static final int INVENTORY_UPDATE_DELAY_MS = 200;
    public static final int NOTIFICATION_DELAY_MS = 100;

    // ===== INVENTORY INITIAL STOCK =====
    public static final int LAPTOP_STOCK = 50;
    public static final int PHONE_STOCK = 100;
    public static final int TABLET_STOCK = 75;
    public static final int HEADPHONES_STOCK = 200;
    public static final int MONITOR_STOCK = 30;

    // ===== USERS =====
    public static final String USER_001 = "user-001";
    public static final String USER_002 = "user-002";
    public static final String USER_003 = "user-003";
    public static final String USER_004 = "user-004";
    public static final String[] USERS    = {USER_001, USER_002, USER_003, USER_004};

    // ===== PRODUCTS =====
    public static final String LAPTOP = "laptop";
    public static final String PHONE = "phone";
    public static final String TABLET = "tablet";
    public static final String HEADPHONES = "headphones";
    public static final String MONITOR = "monitor";
    public static final String[] PRODUCTS = {LAPTOP, PHONE, TABLET, HEADPHONES, MONITOR};


}
