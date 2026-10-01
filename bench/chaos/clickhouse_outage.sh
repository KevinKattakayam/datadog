#!/bin/bash
# ============================================================
#  Chaos Test — ClickHouse Outage and Recovery
#
#  Pauses ClickHouse under load, then unpauses and
#  verifies zero data loss and full recovery.
#
#  `pause` rather than `stop` matters: a paused container accepts
#  TCP connections and never responds, which exercises timeout paths.
#  A stopped one refuses instantly, which exercises nothing.
#
#  Usage: bash bench/chaos/clickhouse_outage.sh
# ============================================================

set -euo pipefail

RATE="${RATE:-50}"
OUTAGE_DURATION="${OUTAGE_DURATION:-10}"
WARMUP_SECONDS="${WARMUP_SECONDS:-5}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-5}"
INGESTOR_URL="${INGESTOR_URL:-http://localhost:8080}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-obs-kafka}"
CLICKHOUSE_CONTAINER="${CLICKHOUSE_CONTAINER:-obs-clickhouse}"
CONSUMER_GROUP="${CONSUMER_GROUP:-processor-group-local}"
if [ "$RATE" -lt 50 ] || [ "$((RATE % 50))" -ne 0 ] || [ "$OUTAGE_DURATION" -lt 1 ] || [ "$WARMUP_SECONDS" -lt 1 ] || [ "$COOLDOWN_SECONDS" -lt 1 ]; then
    echo "RATE must be a positive multiple of 50" >&2
    exit 2
fi
RUN_ID="$(date +%s).$$"
TEST_PREFIX="chaos.outage.${RUN_ID}."

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[0;33m'
RESET='\033[0m'

echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${CYAN}  Chaos Test: ClickHouse ${OUTAGE_DURATION}s Outage${RESET}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
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

# Step 1: Start sending uniquely named metrics in the background.
echo -e "${YELLOW}▸ Starting load generator at ${RATE}/sec...${RESET}"
SENT_COUNT=0
BATCH_SIZE=50
TOTAL_DURATION=$((WARMUP_SECONDS + OUTAGE_DURATION + COOLDOWN_SECONDS))

fire_continuous() {
    local end_time=$(($(date +%s) + TOTAL_DURATION))
    local batch_num=0
    while [ $(date +%s) -lt $end_time ]; do
        local ts
        ts=$(date +%s)
        METRICS=""
        for i in $(seq 1 $BATCH_SIZE); do
            local seq=$((batch_num * BATCH_SIZE + i))
            [ -n "$METRICS" ] && METRICS="$METRICS,"
            METRICS="${METRICS}{\"name\":\"${TEST_PREFIX}${seq}\",\"value\":${seq},\"unit\":\"count\",\"tags\":{\"test\":\"outage\"},\"timestamp\":${ts},\"host\":\"chaos-host\"}"
        done
        # Allow 503s — that's the ingestor telling the truth during outage
        HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${INGESTOR_URL}/ingest/batch" \
            -H "Content-Type: application/json" \
            -d "{\"metrics\":[${METRICS}]}" 2>/dev/null)
        if [ "$HTTP_CODE" = "202" ]; then
            SENT_COUNT=$((SENT_COUNT + BATCH_SIZE))
        else
            FAILED_BATCHES=$((FAILED_BATCHES + 1))
            echo "batch was not fully accepted (HTTP $HTTP_CODE)" >&2
        fi
        batch_num=$((batch_num + 1))
        if [ $((batch_num % (RATE / BATCH_SIZE))) -eq 0 ]; then
            sleep 1
        fi
    done
    echo "$SENT_COUNT,$FAILED_BATCHES" > "/tmp/chaos_sent_count_${RUN_ID}"
}

FAILED_BATCHES=0
fire_continuous &
LOADER_PID=$!

# Step 3: Wait for steady state
echo -e "${YELLOW}▸ Running for ${WARMUP_SECONDS}s to establish steady state...${RESET}"
sleep "$WARMUP_SECONDS"

# Step 4: Pause ClickHouse
echo -e "${RED}▸ PAUSING ClickHouse for ${OUTAGE_DURATION}s...${RESET}"
OUTAGE_START=$(date +%s)
docker pause "$CLICKHOUSE_CONTAINER"
cleanup() {
    docker unpause "$CLICKHOUSE_CONTAINER" >/dev/null 2>&1 || true
    rm -f "/tmp/chaos_sent_count_${RUN_ID}"
}
trap cleanup EXIT

# Record peak lag during outage, including short test runs.
PEAK_LAG=0
OUTAGE_ELAPSED=0
while [ "$OUTAGE_ELAPSED" -lt "$OUTAGE_DURATION" ]; do
    SLEEP_SECONDS=$((OUTAGE_DURATION - OUTAGE_ELAPSED))
    [ "$SLEEP_SECONDS" -gt 10 ] && SLEEP_SECONDS=10
    sleep "$SLEEP_SECONDS"
    OUTAGE_ELAPSED=$((OUTAGE_ELAPSED + SLEEP_SECONDS))
    LAG=$(kafka_lag)
    [ "$LAG" -gt "$PEAK_LAG" ] 2>/dev/null && PEAK_LAG=$LAG
    echo "  Outage ${OUTAGE_ELAPSED}s: lag=$LAG"
done

# Step 5: Unpause ClickHouse
echo -e "${GREEN}▸ UNPAUSING ClickHouse...${RESET}"
docker unpause "$CLICKHOUSE_CONTAINER"
RECOVER_START=$(date +%s)

# Step 6: Wait for loader to finish
echo -e "${YELLOW}▸ Waiting for load generator to complete...${RESET}"
wait $LOADER_PID 2>/dev/null || true

# Step 7: Wait for lag to drain
echo -e "${YELLOW}▸ Waiting for consumer lag to drain...${RESET}"
MAX_WAIT=300
WAITED=0
DRAINED=0
while [ $WAITED -lt $MAX_WAIT ]; do
    LAG=$(kafka_lag)
    if [ "$LAG" -eq 0 ] 2>/dev/null; then
        RECOVERY_TIME=$(($(date +%s) - RECOVER_START))
        DRAINED=1
        echo -e "  Lag drained to zero in ${RECOVERY_TIME}s"
        break
    fi
    echo -e "  Lag: $LAG (${WAITED}s since unpause)"
    sleep 5
    WAITED=$((WAITED + 5))
done
if [ "$DRAINED" -ne 1 ]; then
    echo "consumer lag did not drain within ${MAX_WAIT}s" >&2
    exit 1
fi

sleep 5

# Step 8: Verify
echo ""
echo -e "${CYAN}▸ Verifying results...${RESET}"

LOAD_RESULT=$(cat "/tmp/chaos_sent_count_${RUN_ID}" 2>/dev/null || echo "0,1")
IFS=, read -r SENT FAILED_BATCHES <<EOF
$LOAD_RESULT
EOF
ROWS=$(clickhouse_query "SELECT count() FROM observability.metrics WHERE startsWith(name, '${TEST_PREFIX}')")
UNIQUE=$(clickhouse_query "SELECT uniqExact(name) FROM observability.metrics FINAL WHERE startsWith(name, '${TEST_PREFIX}')")

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo "  sent_accepted=$SENT  rows=$ROWS  unique=$UNIQUE"
echo "  rejected_batches=$FAILED_BATCHES"
echo "  peak_lag=$PEAK_LAG  recovery=${RECOVERY_TIME:-timeout}s"

if [ "$SENT" -gt 0 ] && [ "$UNIQUE" -eq "$SENT" ] && [ "$FAILED_BATCHES" -eq 0 ]; then
    echo -e "  ${GREEN}✓ PASSED — data survived ClickHouse outage${RESET}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    exit 0
else
    echo -e "  ${RED}✗ FAILED — no data recovered${RESET}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    exit 1
fi
