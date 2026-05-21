package com.vbforge.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vbforge.config.Constants.*;

/**
 * Shared utilities used across all scenarios.
 *
 * Provides:
 *  - Singleton ObjectMapper (thread-safe, expensive to create — never instantiate per call)
 *  - verifyConfiguration() — startup sanity check, logs active Kafka settings
 */
public class Utility {

    private static final Logger log = LoggerFactory.getLogger(Utility.class);


    // =========================================================================
    // OBJECT MAPPER — singleton
    // ObjectMapper is thread-safe after configuration and expensive to construct.
    // Always use this instance; never create new ObjectMapper() in application code.
    // =========================================================================

    private static final ObjectMapper OBJECT_MAPPER = buildObjectMapper();

    private static ObjectMapper buildObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules(); // registers java.time.* support automatically
        return mapper;
    }

    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }


    // =========================================================================
    // CONFIGURATION VERIFICATION
    // Call at startup in any scenario main class to confirm the environment.
    // Uncomment topic lines below as each scenario is added.
    // =========================================================================

    /**
     * Logs current Kafka configuration to console.
     * BOOTSTRAP_SERVERS is resolved from env var KAFKA_BOOTSTRAP_SERVERS
     * with fallback to localhost:9092 — see Constants.resolveBootstrapServers().
     */
    public static void verifyConfiguration() {
        String source = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null
                ? "env: KAFKA_BOOTSTRAP_SERVERS"
                : "default (localhost:9092)";

        log.info("======== Kafka Configuration ========");
        log.info("Bootstrap Servers: {}", BOOTSTRAP_SERVERS);
        log.info("Source:            {}", source);
        log.info("Active Topics:");
        log.info("  - Scenario 00 (connectivity): {}", TOPIC_TEST_CONNECTIVITY);
        log.info("  - Scenario 01 (simple):       {}", TOPIC_SIMPLE);
        log.info("  - Scenario 02 (demo app):     {}", TOPIC_DEMO);
        log.info("  - Scenario 03 (load balance): {}", TOPIC_LOAD_BALANCE);
        log.info("  - Scenario 04 (keyed):        {}", TOPIC_KEYED);
        log.info("  - Scenario 05 (manual offset):{}", TOPIC_MANUAL_OFFSET);
        log.info("  - Scenario 06 (ecommerce):    {}", TOPIC_ECOMMERCE_ORDERS);
        log.info("===========================");
    }

}
