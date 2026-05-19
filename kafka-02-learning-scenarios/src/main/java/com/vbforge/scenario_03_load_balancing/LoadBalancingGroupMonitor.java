package com.vbforge.scenario_03_load_balancing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.vbforge.config.Constants.*;

/**
 * LoadBalancingGroupMonitor — prints the Docker CLI commands to inspect
 * consumer group state while the scenario is running.
 *
 * Run this any time during the experiments to get the exact commands.
 * The commands themselves run inside Docker — copy-paste into a spare terminal.
 */
public class LoadBalancingGroupMonitor {

    private static final Logger logger = LoggerFactory.getLogger(LoadBalancingGroupMonitor.class);

    public static void main(String[] args) {

        logger.info("=== Consumer Group Monitor ===");
        logger.info("Copy-paste these commands into a spare terminal while consumers are running:");
        logger.info("");
        logger.info("--- Check partition assignment and consumer lag ---");
        logger.info("docker exec -it kafka-learning-broker \\");
        logger.info("  kafka-consumer-groups --bootstrap-server localhost:19092 \\");
        logger.info("  --group {} --describe", CONSUMER_GROUP_LOAD_BALANCE);
        logger.info("");
        logger.info("--- Expected output with 3 active consumers ---");
        logger.info("TOPIC               PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID");
        logger.info("{}  0          ?               ?               0    Consumer-1-xxx", TOPIC_LOAD_BALANCE);
        logger.info("{}  1          ?               ?               0    Consumer-2-xxx", TOPIC_LOAD_BALANCE);
        logger.info("{}  2          ?               ?               0    Consumer-3-xxx", TOPIC_LOAD_BALANCE);
        logger.info("");
        logger.info("--- List all consumer groups ---");
        logger.info("docker exec -it kafka-learning-broker \\");
        logger.info("  kafka-consumer-groups --bootstrap-server localhost:19092 --list");
        logger.info("");
        logger.info("--- Watch lag in real time (re-run every few seconds) ---");
        logger.info("docker exec -it kafka-learning-broker \\");
        logger.info("  kafka-consumer-groups --bootstrap-server localhost:19092 \\");
        logger.info("  --group {} --describe", CONSUMER_GROUP_LOAD_BALANCE);
        logger.info("");
        logger.info("LAG = LOG-END-OFFSET minus CURRENT-OFFSET.");
        logger.info("LAG = 0 means the consumer is fully caught up.");
        logger.info("Rising LAG means the consumer is falling behind — the producer is faster than consumers.");
    }
}