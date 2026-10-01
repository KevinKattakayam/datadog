# Runbook: SLO Burn Rate Alert

## What fired

One of the multi-window burn-rate alerts from `infra/prometheus/alerts/slo.yml`. These fire when error budget is being consumed faster than sustainable.

| Alert | Window | Meaning |
|-------|--------|---------|
| `SLOBurnRateHigh_1h` | 1h/5m | Acute — something broke in the last few minutes |
| `SLOBurnRateHigh_6h` | 6h/30m | Sustained — a slower degradation over hours |
| `SLOBurnRateHigh_3d` | 3d/6h | Chronic — a long-term trend toward SLO violation |

## What it means

The pipeline's error budget is being consumed at a rate that will exhaust it before the SLO window ends. The SLI is defined as the fraction of successfully ingested and persisted metrics.

## Diagnostic commands

```bash
# Check ingestor error rate
curl -s http://localhost:8080/metrics | grep 'ingestor_requests_total'

# Check processor write errors
curl -s http://localhost:9091/metrics | grep 'clickhouse_write_errors_total'

# Check end-to-end freshness
curl -s "http://localhost:8123/?query=SELECT+now()-max(ts)+AS+lag+FROM+observability.metrics"

# Check consumer lag
docker exec obs-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group processor-group-local

# Check remaining error budget in Prometheus
# (query the Prometheus UI at http://localhost:9090)
# slo:error_budget_remaining:ratio
```

## Resolution

1. **1h/5m alert (acute):** Look at what changed in the last 5 minutes. Common causes: deployment, ClickHouse outage, Kafka partition leader change.
2. **6h/30m alert (sustained):** Check for slow ClickHouse writes, high consumer lag, or elevated DLQ rate.
3. **3d/6h alert (chronic):** Check for gradual degradation — growing cardinality, increasing latencies, disk filling up.

## Decision framework

- **If error budget > 50% remaining:** Monitor, don't act.
- **If error budget 20-50% remaining:** Investigate root cause, plan fix.
- **If error budget < 20% remaining:** Act now. Consider reducing traffic or scaling.
- **If error budget exhausted:** Freeze non-critical changes. Focus on reliability.

## After recovery

- Verify burn rate alerts clear
- Check that error budget is no longer depleting
- Document the incident for the post-mortem
