#!/bin/bash
# ============================================================
#  Create Kafka Topics
#  Run: bash infra/kafka/topics.sh
# ============================================================

set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

echo "▸ Creating Kafka topics on ${BOOTSTRAP}..."

kafka-topics --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic metrics.raw \
  --partitions 6 \
  --replication-factor 1

kafka-topics --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic metrics.processed \
  --partitions 6 \
  --replication-factor 1

kafka-topics --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic alerts.fired \
  --partitions 3 \
  --replication-factor 1

kafka-topics --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic metrics.dlq \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000

echo "✓ All topics created."
kafka-topics --bootstrap-server "$BOOTSTRAP" --list
