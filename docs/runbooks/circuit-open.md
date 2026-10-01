# Runbook: Circuit Breaker Open

## What fired

`processor_circuit_breaker_state` = 1 (open), or the processor `/ready` endpoint returns 503 with `"circuit":"open"`.

## What it means

The ClickHouse writer has failed 5 consecutive times and the circuit breaker has opened to prevent cascading failures. The processor **pauses** the consumer loop — it does not drop data. Kafka retains all messages, and lag grows until ClickHouse recovers.

## Diagnostic commands

```bash
# Confirm circuit state
curl -s http://localhost:9091/ready
# Returns: {"status":"not_ready","clickhouse":false,"circuit":"open"}

# Check ClickHouse health directly
curl -s http://localhost:8123/?query=SELECT+1
# Should return "1\n" if healthy

# Check ClickHouse error log
docker logs obs-clickhouse --tail=50

# Check for TOO_MANY_PARTS
curl -s "http://localhost:8123/?query=SELECT+database,table,count()+AS+parts+FROM+system.parts+WHERE+active+GROUP+BY+database,table+ORDER+BY+parts+DESC"

# Check disk space
docker exec obs-clickhouse df -h /var/lib/clickhouse
```

## Resolution

1. **If ClickHouse is down:** Restart it. `docker start obs-clickhouse`. The circuit will transition to half-open on the next probe, then close after 3 successful writes.
2. **If TOO_MANY_PARTS:** ClickHouse has too many small parts from frequent inserts. Increase `PROCESSOR_FLUSH_INTERVAL_MS` to 2000–5000ms. Run `OPTIMIZE TABLE observability.metrics FINAL` to force merges.
3. **If out of disk:** Add storage or enable tiered storage to S3. Check that TTL is working: `SELECT count() FROM observability.metrics WHERE ts < now() - INTERVAL 91 DAY` should return 0.
4. **If ClickHouse is healthy but circuit is still open:** The backoff may be long (up to 60s). Wait for the probe cycle, or restart the processor to reset the circuit.

## After recovery

Monitor:
- `processor_circuit_breaker_state` returns to 0
- Consumer lag starts draining
- `/ready` returns 200
