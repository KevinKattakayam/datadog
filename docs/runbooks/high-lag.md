# Runbook: KafkaConsumerLagHigh

## What fired

`KafkaConsumerLagHigh`: Consumer group lag for `metrics.raw` exceeds 10,000 for more than 2 minutes.

## What it means

The Rust processor is consuming messages slower than they are being produced. This is usually caused by:
1. ClickHouse degradation (slow writes cause the consumer to stall)
2. Processor crash/restart (rebalance + warm-up)
3. Burst of ingest traffic exceeding processing capacity
4. Too few processor replicas for the partition count

## Diagnostic commands

```bash
# Check current lag per partition
docker exec obs-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group processor-group-local

# Check if the circuit breaker is open
curl -s http://localhost:9091/metrics | grep circuit_breaker_state
# 0 = closed (healthy), 1 = open (ClickHouse problem)

# Check processor readiness
curl -s http://localhost:9091/ready

# Check ClickHouse write latency
curl -s http://localhost:9091/metrics | grep clickhouse_write_duration
```

## Resolution

1. **If circuit breaker is open (state=1):** ClickHouse is the root cause. See [circuit-open.md](circuit-open.md).
2. **If processor just restarted:** Lag will drain naturally. Monitor `processor_rows_committed_total` rate — it should exceed the ingest rate.
3. **If sustained load:** Scale the processor (up to 6 replicas for 6 partitions). If already at max, increase partition count and replica cap together.
4. **If lag is growing after all of the above:** Check `docker logs obs-processor` for errors.

## Escalation

If lag exceeds 100,000 and is growing, page the on-call engineer.
