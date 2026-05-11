package com.vbforge.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vbforge.config.Constants.*;

public class Utility {

    private static final Logger log = LoggerFactory.getLogger(Utility.class);

    // ===================================================
    // SINGLETON OBJECT MAPPER
    // ===================================================

    // ObjectMapper is thread-safe after configuration and very expensive to instantiate.
    // Always reuse a single instance — never create one per call.
    private static final ObjectMapper OBJECT_MAPPER = buildObjectMapper();

    private static ObjectMapper buildObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules(); // Supports Java 8+ time types (java.time.*)
        return mapper;
    }

    /**
     * Returns the shared, pre-configured ObjectMapper instance.
     * Thread-safe. Use this everywhere instead of creating new instances.
     */
    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }


    // ===================================================
    // VERIFICATION CONFIGURATION SUMMARY METHOD
    // ===================================================

    /**
     * Verify Kafka connectivity (useful for debugging)
     * Logs current configuration summary. Call at startup to verify environment.
     */
    public static void verifyConfiguration() {
        log.info("=== Kafka Configuration ===");
        log.info("Bootstrap Servers: {}", BOOTSTRAP_SERVERS);
        log.info("Source:            {}", System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null ? "environment variable" : "default (localhost)");
        log.info("Active Topics:");
        log.info("  - Scenario 00: {}", TOPIC_TEST_CONNECTIVITY_KAFKA);
//        log.info("  - Scenario 01: {}", TOPIC_SIMPLE);
//        log.info("  - Scenario 02: {}", TOPIC_DEMO_APP);
//        log.info("  - Scenario 03: {}", TOPIC_LOAD_BALANCE);
//        log.info("  - Scenario 04: {}", TOPIC_KEYED);
//        log.info("  - Scenario 05: {}", TOPIC_ORDERS);
//        log.info("  - Scenario 06: {}", TOPIC_ECOMMERCE_ORDERS_APP);
        log.info("===========================");
    }

}
