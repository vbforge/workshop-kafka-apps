package com.vbforge.case04.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

// JUNIOR NOTE: Kafka's default partitioner uses murmur2 hash of the key bytes, modulo partition count.
// That's opaque — you can't predict which partition a key will land on without running the hash.
//
// A custom partitioner lets you control routing explicitly.
// Use cases in production:
//   - Route by region: "EU-*" keys → partition 0, "US-*" keys → partition 1
//   - Route hot keys to dedicated partitions to avoid one partition being overwhelmed
//   - Compliance: PII data for certain users must go to a specific partition backed by
//     encrypted storage
//   - Priority lanes: high-priority messages get partition 0, low-priority get partition 1
//
// This custom partitioner implements a simple region-based routing:
//   Keys starting with "eu-"  → partition 0
//   Keys starting with "us-"  → partition 1
//   Keys starting with "asia-"→ partition 2
//   Everything else           → murmur2 default (spread evenly)
//
// Implements org.apache.kafka.clients.producer.Partitioner (not Spring — pure Kafka API).
// You plug it in via ProducerConfig.PARTITIONER_CLASS_CONFIG in KafkaCon

@Slf4j
public class CustomPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        int numPartitions = cluster.partitionCountForTopic(topic);

        // JUNIOR NOTE: Guard against null keys. If someone sends without a key,
        // we fall back to partition 0 rather than NPE-ing. In production you'd
        // want to log this as a warning because it breaks ordering guarantees
        // (null-keyed messages go to a random partition in the default partitioner).
        if(key == null || keyBytes == null) {
            log.warn("CustomPartitioner: null key received - routing to partition 0");
            return 0;
        }

        String keyStr = key.toString().toLowerCase();

        //region-based routing
        if(keyStr.startsWith("eu-")){   //euro
            int targetPartition = 0 % numPartitions;  // safe even if partition count < 3
            log.debug("CustomPartitioner: key='{}' -> EU partition {}", key, targetPartition);
            return targetPartition;
        }
        if(keyStr.startsWith("us-")){   //usa
            int targetPartition = 1 % numPartitions;
            log.debug("CustomPartitioner: key='{}' -> US partition {}", key, targetPartition);
            return targetPartition;
        }
        if(keyStr.startsWith("asia-")){   //asia
            int targetPartition = 2 % numPartitions;
            log.debug("CustomPartitioner: key='{}' -> ASIA partition {}", key, targetPartition);
            return targetPartition;
        }

        // JUNIOR NOTE: For all other keys, fall back to a simple murmur2-style hash.
        // Math.abs guards against negative hash codes (Integer.MIN_VALUE.abs() overflows in Java).
        // This replicates what the default partitioner does — spread unknown keys evenly.
        int hash = Math.abs(keyStr.hashCode());
        int target = hash % numPartitions;
        log.debug("CustomPartitioner: key='{}' → default hash partition {}", key, target);
        return target;

    }

    @Override
    public void close() {
        // Nothing to close — no external resources held
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // JUNIOR NOTE: configure() is called once when the producer is initialized.
        // If your partitioner needed custom config (e.g. a mapping file path from
        // producer properties), you'd read it here. We don't need anything extra.
    }

}
