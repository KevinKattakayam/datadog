#!/bin/bash
# ============================================================
#  Chaos Test — Zero data loss under kill -9
#
#  Sends exactly N numbered metrics, kills the processor
#  mid-stream, restarts it, waits for lag to drain, then
#  asserts that every metric is present in ClickHouse.
#
#  This is the headline test. A passing run proves at-least-once
#  delivery; a failing run proves the bug exists.
#
#  Usage: bash bench/chaos/kill9_no_loss.sh
# ============================================================

set -euo pipefail

SENT="${SENT:-500}"
RATE="${RATE:-500}"
KILL_AFTER_SECS="${KILL_AFTER_SECS:-2}"
INGESTOR_URL="${INGESTOR_URL:-http://localhost:8080}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-obs-kafka}"
PROCESSOR_CONTAINER="${PROCESSOR_CONTAINER:-obs-processor}"
CONSUMER_GROUP="${CONSUMER_GROUP:-processor-group-local}"
if [ "$SENT" -le 0 ] || [ "$((SENT % 50))" -ne 0 ] || [ "$RATE" -lt 50 ]; then
    echo "SENT must be a positive multiple of 50 and RATE must be at least 50" >&2
    exit 2
fi
TEST_NAME="chaos.seq.$(date +%s).$$"

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[0;33m'
RESET='\033[0m'

echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${CYAN}  Chaos Test: kill -9 with zero data loss${RESET}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo ""
echo "  Metrics to send: $SENT"
echo "  Rate:            $RATE/sec"
echo "  Kill after:      ${KILL_AFTER_SECS}s"
echo ""

clickhouse_query() {
    local encoded
    encoded=$(python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1]))' "$1")
    curl -fsS "${CLICKHOUSE_URL}/?query=${encoded}" 2>/dev/null | tr -d '\n'
}

kafka_lag() {
    docker exec "$KAFKA_CONTAINER" kafka-consumer-groups \
        --bootstrap-server localhost:9092 \
        --describe --group "$CONSUMER_GROUP" 2>/dev/null \
        | awk 'NR>1 && $NF!="" {sum+=$6} END {print sum+0}'
}

# Step 1: Fire metrics in the background. A per-run name keeps old results
# from satisfying this run's assertion.
echo -e "${YELLOW}▸ Sending $SENT numbered metrics at ${RATE}/sec...${RESET}"
SEQ=0
BATCH_SIZE=50
TOTAL_BATCHES=$((SENT / BATCH_SIZE))

fire_metrics() {
    local batch_num=0
    local ts
    ts=$(date +%s)
    while [ $batch_num -lt $TOTAL_BATCHES ]; do
        METRICS=""
        for i in $(seq 1 $BATCH_SIZE); do
            SEQ=$((batch_num * BATCH_SIZE + i))
            [ -n "$METRICS" ] && METRICS="$METRICS,"
            METRICS="${METRICS}{\"name\":\"${TEST_NAME}\",\"value\":${SEQ},\"unit\":\"count\",\"tags\":{\"seq\":\"${SEQ}\"},\"timestamp\":${ts},\"host\":\"chaos-host\"}"
        done
        STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${INGESTOR_URL}/ingest/batch" \
            -H "Content-Type: application/json" \
            -d "{\"metrics\":[${METRICS}]}" 2>/dev/null)
        if [ "$STATUS" != "202" ]; then
            echo "batch $batch_num was not fully accepted (HTTP $STATUS)" >&2
            return 1
        fi
        batch_num=$((batch_num + 1))
        # Rate limiting
        if [ $((batch_num % (RATE / BATCH_SIZE))) -eq 0 ]; then
            sleep 1
        fi
    done
}

fire_metrics &
LOADER_PID=$!

# Step 3: Wait, then kill the processor
echo -e "${YELLOW}▸ Waiting ${KILL_AFTER_SECS}s then killing processor...${RESET}"
sleep "$KILL_AFTER_SECS"

echo -e "${RED}▸ SIGKILL → ${PROCESSOR_CONTAINER}${RESET}"
docker kill -s KILL "$PROCESSOR_CONTAINER" 2>/dev/null || true

# Step 4: Wait for loader to finish, then restart
echo -e "${YELLOW}▸ Waiting for load generator to finish...${RESET}"
if ! wait "$LOADER_PID"; then
    echo "load generator failed; refusing to claim a delivery result" >&2
    exit 1
fi

echo -e "${YELLOW}▸ Restarting processor...${RESET}"
sleep 2
docker start "$PROCESSOR_CONTAINER"

# Step 5: Wait for lag to drain
echo -e "${YELLOW}▸ Waiting for consumer lag to reach zero...${RESET}"
MAX_WAIT=120
WAITED=0
DRAINED=0
RECOVERY_SECONDS=0
while [ $WAITED -lt $MAX_WAIT ]; do
    LAG=$(kafka_lag)
    if [ "$LAG" -eq 0 ] 2>/dev/null; then
        echo -e "  Lag reached zero after ${WAITED}s"
        RECOVERY_SECONDS=$WAITED
        DRAINED=1
        break
    fi
    echo -e "  Lag: $LAG (${WAITED}s elapsed)"
    sleep 5
    WAITED=$((WAITED + 5))
done
if [ "$DRAINED" -ne 1 ]; then
    echo "consumer lag did not drain within ${MAX_WAIT}s" >&2
    exit 1
fi

# Extra wait for ClickHouse to process the final batch
sleep 5

# Step 6: Assert results
echo ""
echo -e "${CYAN}▸ Verifying results...${RESET}"

UNIQUE=$(clickhouse_query "SELECT uniqExact(toUInt64(value)) FROM observability.metrics FINAL WHERE name = '${TEST_NAME}'")
TOTAL=$(clickhouse_query "SELECT count() FROM observability.metrics WHERE name = '${TEST_NAME}'")
DUPES=$((TOTAL - UNIQUE))

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo "  sent=$SENT  unique=$UNIQUE  rows=$TOTAL  dupes=$DUPES  recovery_seconds=$RECOVERY_SECONDS"

if [ "$UNIQUE" -eq "$SENT" ] 2>/dev/null; then
    echo -e "  ${GREEN}✓ PASSED — zero data loss${RESET}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    exit 0
else
    LOST=$((SENT - UNIQUE))
    echo -e "  ${RED}✗ FAILED — DATA LOSS: $LOST metrics${RESET}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    exit 1
fi
