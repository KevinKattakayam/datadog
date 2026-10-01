#!/bin/bash
# ============================================================
#  Create Kafka Topics
#  Run: bash infra/kafka/topics.sh
#
#  NOTE on Replication Factor (RF):
#  In local dev / single-node KRaft, REPLICATION_FACTOR=1 is used.
#  In multi-AZ production, you MUST use REPLICATION_FACTOR=3 and
#  min.insync.replicas=2 so that a single broker or AZ loss does
#  not violate durability when producers use acks=all.
# ============================================================

set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
RF="${REPLICATION_FACTOR:-1}"
MIN_ISR="${MIN_IN_SYNC_REPLICAS:-1}"

echo "▸ Creating Kafka topics on ${BOOTSTRAP} (RF=${RF}, min.insync.replicas=${MIN_ISR})..."

kafka-topics --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic metrics.raw \
  --partitions 6 \
  --replication-factor "$RF" \
  --config min.insync.replicas="$MIN_ISR"

kafka-topics --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic metrics.processed \
  --partitions 6 \
  --replication-factor "$RF" \
  --config min.insync.replicas="$MIN_ISR"

kafka-topics --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic alerts.fired \
  --partitions 3 \
  --replication-factor "$RF" \
  --config min.insync.replicas="$MIN_ISR"

kafka-topics --bootstrap-server "$BOOTSTRAP" \
  --create --if-not-exists \
  --topic metrics.dlq \
  --partitions 3 \
  --replication-factor "$RF" \
  --config retention.ms=604800000 \
  --config min.insync.replicas="$MIN_ISR"

echo "✓ All topics created."
kafka-topics --bootstrap-server "$BOOTSTRAP" --list
