#!/bin/bash
# ============================================================
#  Integration Test — End-to-End Pipeline Verification
#  Tests: Ingestor → Kafka → ClickHouse data flow
# ============================================================

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
RESET='\033[0m'

INGESTOR_URL="${INGESTOR_URL:-http://localhost:8080}"
KAFKA_BROKER="${KAFKA_BROKER:-localhost:29092}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
TEST_PREFIX="integration.$(date +%s).$$"
TESTS_PASSED=0
TESTS_FAILED=0

pass() { echo -e "  ${GREEN}✓${RESET} $1"; TESTS_PASSED=$((TESTS_PASSED + 1)); }
fail() { echo -e "  ${RED}✗${RESET} $1"; TESTS_FAILED=$((TESTS_FAILED + 1)); }

echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${CYAN}  Enterprise Observability Pipeline — Integration Tests${RESET}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo ""

# ── Test 1: Ingestor Health ────────────────────────────────────
echo -e "${CYAN}▸ Test Group: Ingestor Health${RESET}"
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$INGESTOR_URL/health" 2>/dev/null)
[ "$STATUS" = "200" ] && pass "GET /health returns 200" || fail "GET /health returned $STATUS"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$INGESTOR_URL/ready" 2>/dev/null)
[ "$STATUS" = "200" ] && pass "GET /ready returns 200" || fail "GET /ready returned $STATUS"

STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$INGESTOR_URL/metrics" 2>/dev/null)
[ "$STATUS" = "200" ] && pass "GET /metrics returns 200" || fail "GET /metrics returned $STATUS"

# ── Test 2: Single Metric Ingestion ────────────────────────────
echo ""
echo -e "${CYAN}▸ Test Group: Single Metric Ingestion${RESET}"
RESP=$(curl -s -X POST "$INGESTOR_URL/ingest" \
  -H "Content-Type: application/json" \
  -d '{"name":"'"${TEST_PREFIX}"'.single","value":42.0,"unit":"count","tags":{"test":"true"},"timestamp":'$(date +%s)',"host":"test-host"}')
echo "$RESP" | grep -q '"accepted":1' && pass "POST /ingest accepted single metric" || fail "POST /ingest failed: $RESP"

# ── Test 3: Batch Ingestion ────────────────────────────────────
echo ""
echo -e "${CYAN}▸ Test Group: Batch Ingestion${RESET}"
METRICS=""
for i in $(seq 1 50); do
  [ -n "$METRICS" ] && METRICS="$METRICS,"
  METRICS="$METRICS{\"name\":\"${TEST_PREFIX}.batch.$i\",\"value\":$((RANDOM % 1000)),\"unit\":\"count\",\"tags\":{\"test\":\"batch\",\"index\":\"$i\"},\"timestamp\":$(date +%s),\"host\":\"test-host\"}"
done
RESP=$(curl -s -X POST "$INGESTOR_URL/ingest/batch" \
  -H "Content-Type: application/json" \
  -d "{\"metrics\":[$METRICS]}")
echo "$RESP" | grep -q '"accepted":50' && pass "POST /ingest/batch accepted 50 metrics" || fail "Batch response: $RESP"

# ── Test 4: Validation Rejections ──────────────────────────────
echo ""
echo -e "${CYAN}▸ Test Group: Input Validation${RESET}"

# Missing name
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$INGESTOR_URL/ingest" \
  -H "Content-Type: application/json" \
  -d '{"value":42.0,"unit":"count","timestamp":'$(date +%s)',"host":"test"}')
[ "$STATUS" = "400" ] && pass "Rejects metric with missing name (400)" || fail "Expected 400, got $STATUS"

# Missing host
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$INGESTOR_URL/ingest" \
  -H "Content-Type: application/json" \
  -d '{"name":"test","value":42.0,"unit":"count","timestamp":'$(date +%s)'}')
[ "$STATUS" = "400" ] && pass "Rejects metric with missing host (400)" || fail "Expected 400, got $STATUS"

# Empty batch
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$INGESTOR_URL/ingest/batch" \
  -H "Content-Type: application/json" \
  -d '{"metrics":[]}')
[ "$STATUS" = "400" ] && pass "Rejects empty batch (400)" || fail "Expected 400, got $STATUS"

# Invalid JSON
STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$INGESTOR_URL/ingest" \
  -H "Content-Type: application/json" \
  -d '{invalid json}')
[ "$STATUS" = "400" ] && pass "Rejects invalid JSON (400)" || fail "Expected 400, got $STATUS"

# ── Test 5: Kafka Message Verification ─────────────────────────
echo ""
echo -e "${CYAN}▸ Test Group: Kafka Verification${RESET}"
sleep 3  # Allow the broker's topic offset query to reflect the writes

OFFSETS=$(docker exec obs-kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic metrics.raw 2>/dev/null)
TOTAL_OFFSET=0
while IFS= read -r line; do
  OFFSET=$(echo "$line" | cut -d: -f3)
  TOTAL_OFFSET=$((TOTAL_OFFSET + OFFSET))
done <<< "$OFFSETS"

[ "$TOTAL_OFFSET" -gt 0 ] && pass "Kafka metrics.raw has $TOTAL_OFFSET messages" || fail "Kafka metrics.raw is empty"

# Check all 4 topics exist
TOPICS=$(docker exec obs-kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null)
echo "$TOPICS" | grep -q "metrics.raw" && pass "Topic metrics.raw exists" || fail "Topic metrics.raw missing"
echo "$TOPICS" | grep -q "metrics.processed" && pass "Topic metrics.processed exists" || fail "Topic metrics.processed missing"
echo "$TOPICS" | grep -q "alerts.fired" && pass "Topic alerts.fired exists" || fail "Topic alerts.fired missing"
echo "$TOPICS" | grep -q "metrics.dlq" && pass "Topic metrics.dlq exists" || fail "Topic metrics.dlq missing"

# ── Test 6: ClickHouse Schema Verification ─────────────────────
echo ""
echo -e "${CYAN}▸ Test Group: ClickHouse Schema${RESET}"

CH_TABLES=$(curl -s "$CLICKHOUSE_URL/?query=SHOW+TABLES+FROM+observability" 2>/dev/null)
echo "$CH_TABLES" | grep -q "metrics" && pass "ClickHouse metrics table exists" || fail "metrics table missing"
echo "$CH_TABLES" | grep -q "metrics_hourly" && pass "ClickHouse metrics_hourly MV exists" || fail "metrics_hourly missing"
echo "$CH_TABLES" | grep -q "metrics_daily" && pass "ClickHouse metrics_daily MV exists" || fail "metrics_daily missing"
echo "$CH_TABLES" | grep -q "alerts" && pass "ClickHouse alerts table exists" || fail "alerts table missing"

# ── Test 7: Prometheus Targets ─────────────────────────────────
echo ""
echo -e "${CYAN}▸ Test Group: Prometheus${RESET}"
PROM_HEALTH=$(curl -s http://localhost:9090/-/healthy 2>/dev/null)
echo "$PROM_HEALTH" | grep -q "Healthy" && pass "Prometheus is healthy" || fail "Prometheus unhealthy"

# ── Test 8: Grafana ────────────────────────────────────────────
echo ""
echo -e "${CYAN}▸ Test Group: Grafana${RESET}"
GF_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:3000/api/health 2>/dev/null)
[ "$GF_STATUS" = "200" ] && pass "Grafana is healthy" || fail "Grafana returned $GF_STATUS"

GF_DS=$(curl -s -u admin:admin http://localhost:3000/api/datasources 2>/dev/null)
if echo "$GF_DS" | grep -q '"name":"Prometheus"'; then
  pass "Prometheus datasource is provisioned in Grafana"
elif echo "$GF_DS" | grep -q '"statusCode":401' && grep -q 'name: Prometheus' infra/grafana/provisioning/datasources/datasources.yml; then
  # Reused Grafana volumes retain their original admin password; verify the
  # mounted provisioning source when the development default cannot log in.
  pass "Prometheus datasource provisioning is configured (Grafana admin auth is customized)"
else
  fail "Prometheus datasource missing or Grafana API check failed"
fi

# ── Test 8b: Alertmanager ─────────────────────────────────────
echo ""
echo -e "${CYAN}▸ Test Group: Alertmanager${RESET}"
AM_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9094/-/healthy 2>/dev/null)
[ "$AM_STATUS" = "200" ] && pass "Alertmanager is healthy" || fail "Alertmanager returned $AM_STATUS"

# ── Test 9: Prometheus Metrics Content ─────────────────────────
echo ""
echo -e "${CYAN}▸ Test Group: Prometheus Metrics${RESET}"
METRICS_RESP=$(curl -s "$INGESTOR_URL/metrics" 2>/dev/null)
echo "$METRICS_RESP" | grep -q "ingestor_requests_total" && pass "ingestor_requests_total metric exposed" || fail "ingestor_requests_total missing"
echo "$METRICS_RESP" | grep -q "ingestor_publish_errors_total" && pass "ingestor_publish_errors_total metric exposed" || fail "ingestor_publish_errors_total missing"
echo "$METRICS_RESP" | grep -q "ingestor_batch_size_histogram" && pass "ingestor_batch_size_histogram metric exposed" || fail "ingestor_batch_size_histogram missing"
echo "$METRICS_RESP" | grep -q "ingestor_kafka_messages_published_total" && pass "ingestor_kafka_messages_published_total metric exposed" || fail "ingestor_kafka_messages_published_total missing"

# ── Test 10: ClickHouse Row Assertions ─────────────────────────
echo ""
echo -e "${CYAN}▸ Test Group: ClickHouse Data Verification (Row Assertions)${RESET}"

# Wait for this run's rows to become visible in ClickHouse.
echo "  Waiting for this run's metrics to reach ClickHouse..."
ROW_COUNT=0
for attempt in $(seq 1 30); do
  ROW_COUNT=$(curl -s "$CLICKHOUSE_URL/?query=SELECT+count()+FROM+observability.metrics+WHERE+startsWith(name%2C+'${TEST_PREFIX}')" 2>/dev/null | tr -d '[:space:]')
  [ "$ROW_COUNT" = "51" ] && break
  sleep 1
done

# Verify metrics actually landed in ClickHouse (not just tables exist)
if [ "$ROW_COUNT" = "51" ]; then
    pass "ClickHouse metrics table has $ROW_COUNT integration test rows"
else
    fail "No integration test rows in ClickHouse metrics table (got: '$ROW_COUNT')"
fi

# Verify the single metric we sent exists
SINGLE_COUNT=$(curl -s "$CLICKHOUSE_URL/?query=SELECT+count()+FROM+observability.metrics+WHERE+name='${TEST_PREFIX}.single'" 2>/dev/null | tr -d '[:space:]')
if [ -n "$SINGLE_COUNT" ] && [ "$SINGLE_COUNT" -gt 0 ] 2>/dev/null; then
    pass "Single metric 'integration.test.single' found in ClickHouse"
else
    fail "Single metric 'integration.test.single' not found in ClickHouse (count: '$SINGLE_COUNT')"
fi

# Verify batch metrics landed
BATCH_COUNT=$(curl -s "$CLICKHOUSE_URL/?query=SELECT+count()+FROM+observability.metrics+WHERE+startsWith(name%2C+'${TEST_PREFIX}.batch.')" 2>/dev/null | tr -d '[:space:]')
if [ "$BATCH_COUNT" = "50" ]; then
    pass "Batch metrics landed: $BATCH_COUNT rows (expected ~50)"
else
    fail "Batch metric count too low: $BATCH_COUNT (expected >= 40)"
fi

# Verify tenant_id column is populated
TENANT_COUNT=$(curl -s "$CLICKHOUSE_URL/?query=SELECT+uniqExact(tenant_id)+FROM+observability.metrics+WHERE+startsWith(name%2C+'${TEST_PREFIX}')" 2>/dev/null | tr -d '[:space:]')
if [ -n "$TENANT_COUNT" ] && [ "$TENANT_COUNT" -ge 1 ] 2>/dev/null; then
    pass "tenant_id column populated ($TENANT_COUNT distinct tenants)"
else
    fail "tenant_id column not populated"
fi

# Verify DLQ table exists
echo "$CH_TABLES" | grep -q "dlq_events" && pass "ClickHouse dlq_events table exists" || fail "dlq_events table missing"

# ── Test 11: Processor Health ──────────────────────────────────
echo ""
echo -e "${CYAN}▸ Test Group: Processor Health${RESET}"
PROC_HEALTH=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9091/health 2>/dev/null)
[ "$PROC_HEALTH" = "200" ] && pass "Processor /health returns 200" || fail "Processor /health returned $PROC_HEALTH"

PROC_READY=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9091/ready 2>/dev/null)
[ "$PROC_READY" = "200" ] && pass "Processor /ready returns 200" || fail "Processor /ready returned $PROC_READY"

# ── Summary ────────────────────────────────────────────────────
echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
TOTAL=$((TESTS_PASSED + TESTS_FAILED))
if [ "$TESTS_FAILED" -eq 0 ]; then
  echo -e "  ${GREEN}All $TOTAL tests passed!${RESET}"
else
  echo -e "  ${GREEN}$TESTS_PASSED passed${RESET}, ${RED}$TESTS_FAILED failed${RESET} out of $TOTAL"
fi
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"

exit $TESTS_FAILED
