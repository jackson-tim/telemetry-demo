#!/bin/bash
# Create Kafka topics for the flow pipeline
# Run from within the kafka pod or with kafka-topics.sh on path

KAFKA_BOOTSTRAP="${1:-localhost:9092}"

# ifindex-map: compacted topic — retains latest ifindex→interface mapping per key
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --create --if-not-exists \
  --topic ifindex-map \
  --partitions 1 \
  --replication-factor 1 \
  --config cleanup.policy=compact \
  --config min.compaction.lag.ms=0 \
  --config segment.ms=300000 \
  --config delete.retention.ms=86400000

# flow-telemetry-raw: raw sfacctd output
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --create --if-not-exists \
  --topic flow-telemetry-raw \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --config cleanup.policy=delete

# flow-telemetry-enriched: enriched flows
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP" \
  --create --if-not-exists \
  --topic flow-telemetry-enriched \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

echo "Topics created."
