#!/bin/bash
# Register Avro schemas in Confluent Schema Registry
# Run after Schema Registry is healthy

set -e

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"

echo "Waiting for Schema Registry..."
until curl -sf "$SCHEMA_REGISTRY_URL/subjects" > /dev/null 2>&1; do
  sleep 2
done
echo "Schema Registry is ready."

# Register metric schema for metrics.raw topic
echo "Registering metric schema..."
METRIC_SCHEMA=$(cat /etc/schema-registry/schemas/metric.avsc | python3 -c "import sys,json; print(json.dumps(json.dumps(json.load(sys.stdin))))")
curl -sf -X POST "$SCHEMA_REGISTRY_URL/subjects/metrics.raw-value/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "{\"schemaType\": \"AVRO\", \"schema\": $METRIC_SCHEMA}" && echo ""

# Register metric schema for metrics.processed topic
echo "Registering processed metric schema..."
curl -sf -X POST "$SCHEMA_REGISTRY_URL/subjects/metrics.processed-value/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "{\"schemaType\": \"AVRO\", \"schema\": $METRIC_SCHEMA}" && echo ""

# Register alert schema for alerts.fired topic
echo "Registering alert schema..."
ALERT_SCHEMA=$(cat /etc/schema-registry/schemas/alert.avsc | python3 -c "import sys,json; print(json.dumps(json.dumps(json.load(sys.stdin))))")
curl -sf -X POST "$SCHEMA_REGISTRY_URL/subjects/alerts.fired-value/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d "{\"schemaType\": \"AVRO\", \"schema\": $ALERT_SCHEMA}" && echo ""

echo ""
echo "All schemas registered. Listing subjects:"
curl -sf "$SCHEMA_REGISTRY_URL/subjects" | python3 -m json.tool
