package com.vbforge.config;

public interface Constants {

    // ===== KAFKA CONNECTION =====
    String BOOTSTRAP_SERVERS = "localhost:9092";

    // ===== CONSTANTS FOR TEST CONNECTIVITY (KEY & VALUE) =====
    String TEST_CONNECTIVITY_KEY = "test-key";
    String TEST_CONNECTIVITY_VALUE = "Hello Docker Kafka!";

    // ===== TOPIC NAMES ACROSS APP =====
    String TOPIC_TEST_CONNECTIVITY_KAFKA = "topic-test-connectivity-kafka";

}
