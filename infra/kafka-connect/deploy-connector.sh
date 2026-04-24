#!/usr/bin/env bash
# ============================================================
#  Deploy ClickHouse Sink Connector to Kafka Connect
#  Waits for REST API readiness, then registers the connector.
# ============================================================
set -euo pipefail

CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
CONNECTOR_CONFIG="/etc/kafka-connect/clickhouse-sink.json"
MAX_RETRIES=30
RETRY_INTERVAL=5

echo "━━━━ Kafka Connect — ClickHouse Sink Deployment ━━━━"

# ── Wait for Kafka Connect REST API ──────────────────────────
echo "▸ Waiting for Kafka Connect at ${CONNECT_URL}..."
for i in $(seq 1 $MAX_RETRIES); do
    if curl -sf "${CONNECT_URL}/connectors" > /dev/null 2>&1; then
        echo "  ✓ Kafka Connect is ready (attempt ${i}/${MAX_RETRIES})"
        break
    fi
    if [ "$i" -eq "$MAX_RETRIES" ]; then
        echo "  ✗ Kafka Connect did not become ready after ${MAX_RETRIES} attempts"
        exit 1
    fi
    echo "  … attempt ${i}/${MAX_RETRIES}, retrying in ${RETRY_INTERVAL}s"
    sleep "$RETRY_INTERVAL"
done

# ── Check if connector already exists ────────────────────────
CONNECTOR_NAME=$(jq -r '.name' "$CONNECTOR_CONFIG")
echo "▸ Checking for existing connector: ${CONNECTOR_NAME}"

if curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" > /dev/null 2>&1; then
    echo "  → Connector exists. Updating configuration..."
    curl -sf -X PUT \
        -H "Content-Type: application/json" \
        -d @"$CONNECTOR_CONFIG" \
        "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config" | jq .
    echo "  ✓ Connector updated"
else
    echo "  → Connector not found. Creating..."
    curl -sf -X POST \
        -H "Content-Type: application/json" \
        -d @"$CONNECTOR_CONFIG" \
        "${CONNECT_URL}/connectors" | jq .
    echo "  ✓ Connector created"
fi

# ── Verify connector status ──────────────────────────────────
echo ""
echo "▸ Connector status:"
sleep 2
curl -sf "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | jq .

echo ""
echo "━━━━ Deployment complete ━━━━"
