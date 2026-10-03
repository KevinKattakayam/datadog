#!/bin/bash
# ============================================================
#  Chaos Test — Graceful restart under load, zero data loss
#
#  Sends N numbered metrics at a steady rate and, part-way through,
#  restarts one container with a normal SIGTERM (docker restart),
#  the same signal Kubernetes sends during a rolling update. Then
#  waits for the consumer to drain and asserts every metric landed.
#
#  TARGET=processor  restart the Kafka consumer under load. The
#                    load generator should see no errors; the
#                    processor must flush and commit before it exits.
#  TARGET=ingestor   restart the HTTP front door under load. Compose
#                    runs one replica with no load balancer, so requests
#                    during the restart fail and the load generator
#                    retries them. This checks that nothing the ingestor
#                    ACCEPTED is lost and that retried batches recover.
#                    It does NOT prove zero client-visible errors; that
#                    needs two or more replicas behind a Service, which
#                    only Kubernetes provides.
#
#  Usage: TARGET=processor bash bench/chaos/rolling_restart_no_loss.sh
#         TARGET=ingestor  SENT=20000 RATE=1000 bash bench/chaos/rolling_restart_no_loss.sh
# ============================================================

set -euo pipefail

TARGET="${TARGET:-processor}"
SENT="${SENT:-20000}"
RATE="${RATE:-1000}"
RESTART_AFTER_SECS="${RESTART_AFTER_SECS:-3}"
STOP_TIMEOUT_SECS="${STOP_TIMEOUT_SECS:-30}"
MAX_ATTEMPTS="${MAX_ATTEMPTS:-60}"
INGESTOR_URL="${INGESTOR_URL:-http://localhost:8080}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-obs-kafka}"
CONSUMER_GROUP="${CONSUMER_GROUP:-processor-group-local}"

case "$TARGET" in
    processor) CONTAINER="${PROCESSOR_CONTAINER:-obs-processor}" ;;
    ingestor)  CONTAINER="${INGESTOR_CONTAINER:-obs-ingestor}" ;;
    *) echo "TARGET must be processor or ingestor" >&2; exit 2 ;;
esac

BATCH_SIZE=50
if [ "$SENT" -le 0 ] || [ "$((SENT % BATCH_SIZE))" -ne 0 ] || [ "$RATE" -lt "$BATCH_SIZE" ] || [ "$((RATE % BATCH_SIZE))" -ne 0 ]; then
    echo "SENT and RATE must be positive multiples of $BATCH_SIZE" >&2
    exit 2
fi

TEST_NAME="chaos.rolling.${TARGET}.$(date +%s).$$"
STATS_FILE="$(mktemp)"
trap 'rm -f "$STATS_FILE"' EXIT

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[0;33m'
RESET='\033[0m'

echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${CYAN}  Chaos Test: graceful restart of ${TARGET} under load${RESET}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo ""
echo "  Metrics to send:  $SENT"
echo "  Rate:             $RATE/sec"
echo "  Restart after:    ${RESTART_AFTER_SECS}s (docker restart -t ${STOP_TIMEOUT_SECS})"
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

# Sends numbered batches. A batch that is not answered 202 is retried until it
# is accepted (or MAX_ATTEMPTS is hit), because a real client retries. Records
# how many retries were needed in $STATS_FILE.
fire_metrics() {
    local total_batches=$((SENT / BATCH_SIZE))
    local batches_per_sec=$((RATE / BATCH_SIZE))
    local retries=0
    local batch_num=0
    local ts
    ts=$(date +%s)

    while [ "$batch_num" -lt "$total_batches" ]; do
        local metrics="" i n
        for i in $(seq 0 $((BATCH_SIZE - 1))); do
            n=$((batch_num * BATCH_SIZE + i))
            [ -n "$metrics" ] && metrics="$metrics,"
            metrics="${metrics}{\"name\":\"${TEST_NAME}\",\"value\":${n},\"unit\":\"count\",\"timestamp\":${ts},\"host\":\"chaos-host\"}"
        done

        local attempt=1 status
        while true; do
            status=$(curl -s -o /dev/null -w "%{http_code}" -X POST "${INGESTOR_URL}/ingest/batch" \
                -H "Content-Type: application/json" \
                -d "{\"metrics\":[${metrics}]}" 2>/dev/null || true)
            [ "$status" = "202" ] && break
            retries=$((retries + 1))
            if [ "$attempt" -ge "$MAX_ATTEMPTS" ]; then
                echo "batch $batch_num never accepted (last HTTP ${status:-none})" >&2
                echo "retries=$retries failed=1" > "$STATS_FILE"
                return 1
            fi
            attempt=$((attempt + 1))
            sleep 0.5
        done

        batch_num=$((batch_num + 1))
        if [ $((batch_num % batches_per_sec)) -eq 0 ]; then
            sleep 1
        fi
    done
    echo "retries=$retries failed=0" > "$STATS_FILE"
}

echo -e "${YELLOW}▸ Sending $SENT numbered metrics at ${RATE}/sec...${RESET}"
fire_metrics &
LOADER_PID=$!

echo -e "${YELLOW}▸ Waiting ${RESTART_AFTER_SECS}s, then restarting ${CONTAINER}...${RESET}"
sleep "$RESTART_AFTER_SECS"
RESTART_BEGAN=$(date +%s)
docker restart -t "$STOP_TIMEOUT_SECS" "$CONTAINER" >/dev/null
RESTART_SECONDS=$(( $(date +%s) - RESTART_BEGAN ))
echo -e "${YELLOW}▸ ${CONTAINER} restarted in ${RESTART_SECONDS}s${RESET}"

echo -e "${YELLOW}▸ Waiting for load generator to finish...${RESET}"
if ! wait "$LOADER_PID"; then
    echo "load generator failed; refusing to claim a delivery result" >&2
    cat "$STATS_FILE" >&2 || true
    exit 1
fi
# shellcheck disable=SC1090
source "$STATS_FILE"

echo -e "${YELLOW}▸ Waiting for consumer lag to reach zero...${RESET}"
MAX_WAIT=180
WAITED=0
DRAINED=0
while [ "$WAITED" -lt "$MAX_WAIT" ]; do
    LAG=$(kafka_lag)
    if [ "$LAG" -eq 0 ] 2>/dev/null; then
        echo "  Lag reached zero after ${WAITED}s"
        DRAINED=1
        break
    fi
    echo "  Lag: $LAG (${WAITED}s elapsed)"
    sleep 5
    WAITED=$((WAITED + 5))
done
if [ "$DRAINED" -ne 1 ]; then
    echo "consumer lag did not drain within ${MAX_WAIT}s" >&2
    exit 1
fi
sleep 5

echo ""
echo -e "${CYAN}▸ Verifying results...${RESET}"
UNIQUE=$(clickhouse_query "SELECT uniqExact(toUInt64(value)) FROM observability.metrics FINAL WHERE name = '${TEST_NAME}'")
TOTAL=$(clickhouse_query "SELECT count() FROM observability.metrics WHERE name = '${TEST_NAME}'")
DUPES=$((TOTAL - UNIQUE))

echo ""
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo "  target=$TARGET  sent=$SENT  unique=$UNIQUE  rows=$TOTAL  dupes=$DUPES  client_retries=$retries  restart_seconds=$RESTART_SECONDS  drain_seconds=$WAITED"

if [ "$UNIQUE" -eq "$SENT" ] 2>/dev/null; then
    echo -e "  ${GREEN}✓ PASSED — zero data loss across a graceful restart${RESET}"
    echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
    exit 0
fi
echo -e "  ${RED}✗ FAILED — DATA LOSS: $((SENT - UNIQUE)) metrics${RESET}"
echo -e "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
exit 1
