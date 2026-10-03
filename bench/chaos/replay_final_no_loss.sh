#!/bin/bash
# Proves the crash window after ClickHouse durability and before Kafka commit.
# The raw table keeps physical replay duplicates; FINAL must show every sent
# source coordinate exactly once and the gap query must return zero.
set -euo pipefail

SENT="${SENT:-100}"
INGESTOR_URL="${INGESTOR_URL:-http://localhost:8080}"
CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
PROCESSOR_CONTAINER="${PROCESSOR_CONTAINER:-obs-processor}"
CONSUMER_GROUP="${CONSUMER_GROUP:-processor-group-local}"
RUN_ID="$(date +%s).$$"
NAME="chaos.replay.${RUN_ID}"

query() {
  curl -fsS --get --data-urlencode "query=$1" "$CLICKHOUSE_URL/" | tr -d '\n'
}

cleanup() {
  PROCESSOR_FAILPOINT= docker compose up -d --force-recreate processor >/dev/null 2>&1 || true
}
trap cleanup EXIT

PROCESSOR_FAILPOINT=after_write_before_commit docker compose up -d --force-recreate processor >/dev/null
sleep 3

metrics=""
ts=$(date +%s)
for i in $(seq 0 $((SENT - 1))); do
  [ -n "$metrics" ] && metrics+="," 
  metrics+="{\"name\":\"$NAME\",\"value\":$i,\"unit\":\"count\",\"timestamp\":$ts,\"host\":\"replay-host\"}"
done
status=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$INGESTOR_URL/ingest/batch" -H 'Content-Type: application/json' -d "{\"metrics\":[${metrics}]}" )
[ "$status" = 202 ] || { echo "ingest failed with HTTP $status" >&2; exit 1; }

for _ in $(seq 1 30); do
  docker inspect -f '{{.State.Running}}' "$PROCESSOR_CONTAINER" 2>/dev/null | grep -qx false && break
  sleep 1
done
docker inspect -f '{{.State.Running}}' "$PROCESSOR_CONTAINER" | grep -qx false || { echo "failpoint did not abort processor" >&2; exit 1; }

PROCESSOR_FAILPOINT= docker compose up -d --force-recreate processor >/dev/null
for _ in $(seq 1 60); do
  lag=$(docker exec obs-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group "$CONSUMER_GROUP" 2>/dev/null | awk 'NR>1 && $NF!="" {s+=$6} END {print s+0}')
  [ "$lag" = 0 ] && break
  sleep 1
done
[ "${lag:-1}" = 0 ] || { echo "consumer lag did not drain" >&2; exit 1; }

physical=$(query "SELECT count() FROM observability.metrics WHERE name = '$NAME'")
unique=$(query "SELECT count() FROM observability.metrics FINAL WHERE name = '$NAME'")
gaps=$(query "WITH (SELECT any(kafka_partition) FROM observability.metrics FINAL WHERE name = '$NAME') AS p, (SELECT min(kafka_offset) FROM observability.metrics FINAL WHERE name = '$NAME') AS first, (SELECT max(kafka_offset) FROM observability.metrics FINAL WHERE name = '$NAME') AS last SELECT (last - first + 1) - count() FROM observability.metrics FINAL WHERE kafka_partition = p AND kafka_offset BETWEEN first AND last")
echo "sent=$SENT physical=$physical final=$unique gaps=$gaps"
[ "$physical" -gt "$SENT" ] && [ "$unique" = "$SENT" ] && [ "$gaps" = 0 ]
