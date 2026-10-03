#!/bin/bash
# ============================================================
#  Throughput and latency benchmark — N runs, median reported
#
#  Runs tests/load/fire_metrics.go RUNS times against a running stack
#  (`make dev` first), records achieved throughput and request latency
#  percentiles for each run, waits for the processor to drain the Kafka
#  backlog after each run, and reports the MEDIAN of every figure.
#  The full report, including the host description, is written to
#  bench/results/throughput-<timestamp>.txt.
#
#  A single run is an anecdote. The median of three or more, with the
#  spread, is a measurement. Quote only these figures, and always with
#  the host they were measured on.
#
#  Usage: RATE=5000 DURATION=60s RUNS=3 bash bench/throughput_median.sh
# ============================================================

set -euo pipefail

RATE="${RATE:-5000}"
DURATION="${DURATION:-60s}"
RUNS="${RUNS:-3}"
BATCH="${BATCH:-100}"
URL="${URL:-http://localhost:8080/ingest/batch}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-obs-kafka}"
CONSUMER_GROUP="${CONSUMER_GROUP:-processor-group-local}"
COOLDOWN_SECONDS="${COOLDOWN_SECONDS:-15}"
MAX_DRAIN_SECONDS="${MAX_DRAIN_SECONDS:-600}"
OUT_DIR="${OUT_DIR:-bench/results}"

if [ "$RUNS" -lt 3 ]; then
    echo "RUNS must be at least 3 for a median to mean anything" >&2
    exit 2
fi

STAMP="$(date +%Y%m%d-%H%M%S)"
REPORT="${OUT_DIR}/throughput-${STAMP}.txt"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$OUT_DIR"

command -v go >/dev/null || { echo "go is required to build the load tool" >&2; exit 2; }
go build -o "$WORK/fire" tests/load/fire_metrics.go

kafka_lag() {
    docker exec "$KAFKA_CONTAINER" kafka-consumer-groups \
        --bootstrap-server localhost:9092 \
        --describe --group "$CONSUMER_GROUP" 2>/dev/null \
        | awk 'NR>1 && $NF!="" {sum+=$6} END {print sum+0}'
}

# median of the numbers on stdin (mean of the middle two for an even count)
median() {
    sort -n | awk '{a[NR]=$1} END {
        if (NR==0) {print "n/a"; exit}
        if (NR%2) printf "%.1f", a[(NR+1)/2]; else printf "%.1f", (a[NR/2]+a[NR/2+1])/2 }'
}

{
    echo "# Throughput benchmark ${STAMP}"
    echo "# rate=${RATE}/s  duration=${DURATION}  runs=${RUNS}  batch=${BATCH}  url=${URL}"
    echo
    echo "## Host"
    uname -srm
    lscpu 2>/dev/null | grep -E '^(Model name|CPU\(s\)):' || true
    free -h 2>/dev/null | sed -n 1,2p || true
    docker version --format 'docker server={{.Server.Version}}' 2>/dev/null || true
    git rev-parse --short HEAD 2>/dev/null | sed 's/^/commit=/' || true
    echo
    echo "## Runs"
    printf '%-4s %12s %9s %9s %9s %7s %9s\n' run "metrics/sec" "p50_ms" "p95_ms" "p99_ms" errors "drain_s"
} > "$REPORT"

: > "$WORK/tput"; : > "$WORK/p50"; : > "$WORK/p95"; : > "$WORK/p99"; : > "$WORK/drain"
BAD=0

for run in $(seq 1 "$RUNS"); do
    echo "▸ run ${run}/${RUNS}: ${RATE}/s for ${DURATION}"
    set +e
    "$WORK/fire" --rate="$RATE" --duration="$DURATION" --batch="$BATCH" --url="$URL" > "$WORK/out.$run" 2>&1
    set -e
    tput=$(awk '/Throughput:/ {print $2}' "$WORK/out.$run")
    p50=$(awk '/Latency p50/ {print $3}' "$WORK/out.$run")
    p95=$(awk '/Latency p95/ {print $3}' "$WORK/out.$run")
    p99=$(awk '/Latency p99/ {print $3}' "$WORK/out.$run")
    errs=$(awk '/Errors:/ {print $2}' "$WORK/out.$run")
    if [ -z "$tput" ] || [ -z "$p99" ] || [ -z "$errs" ]; then
        echo "could not parse run ${run} output:" >&2
        cat "$WORK/out.$run" >&2
        exit 1
    fi
    [ "$errs" -eq 0 ] || BAD=1

    # Time for the processor to work off the backlog this run created.
    drained=0
    waited=0
    while [ "$waited" -lt "$MAX_DRAIN_SECONDS" ]; do
        lag=$(kafka_lag)
        if [ "$lag" -eq 0 ] 2>/dev/null; then drained=1; break; fi
        sleep 2
        waited=$((waited + 2))
    done
    [ "$drained" -eq 1 ] || { echo "run ${run}: lag did not drain in ${MAX_DRAIN_SECONDS}s" >&2; BAD=1; }

    echo "$tput" >> "$WORK/tput"; echo "$p50" >> "$WORK/p50"
    echo "$p95" >> "$WORK/p95"; echo "$p99" >> "$WORK/p99"; echo "$waited" >> "$WORK/drain"
    printf '%-4s %12s %9s %9s %9s %7s %9s\n' "$run" "$tput" "$p50" "$p95" "$p99" "$errs" "$waited" >> "$REPORT"
    sleep "$COOLDOWN_SECONDS"
done

{
    echo
    echo "## Median of ${RUNS} runs"
    echo "throughput_metrics_per_sec=$(median < "$WORK/tput")  (requested ${RATE})"
    echo "latency_p50_ms=$(median < "$WORK/p50")"
    echo "latency_p95_ms=$(median < "$WORK/p95")"
    echo "latency_p99_ms=$(median < "$WORK/p99")"
    echo "drain_seconds=$(median < "$WORK/drain")"
    echo "min_throughput=$(sort -n "$WORK/tput" | head -1)  max_throughput=$(sort -n "$WORK/tput" | tail -1)"
    echo
    echo "## Notes"
    echo "Latency is per ingest request (a batch of ${BATCH}), measured by the load generator."
    echo "drain_seconds is how long the processor needed to clear the Kafka backlog after the run."
    echo "If throughput is well below the requested rate, the stack was saturated at this rate."
    if [ "$BAD" -ne 0 ]; then
        echo "WARNING: at least one run had errors or did not drain. Do not quote these figures."
    fi
} >> "$REPORT"

cat "$REPORT"
echo
echo "Report written to ${REPORT}"
[ "$BAD" -eq 0 ]
