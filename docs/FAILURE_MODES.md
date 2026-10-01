# Failure Modes

Every component in the pipeline can fail. This document describes what happens when it does, what the user sees, and how recovery works.

## Ingestor Failures

### Ingestor process crash (`kill -9`)

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | In-flight HTTP requests are dropped. Records already sent to Kafka via `ProduceSync` are durable. Records in-flight to Kafka are lost. |
| **User sees** | Connection reset on pending requests. Kubernetes restarts the pod. |
| **Data loss** | Only metrics whose `ProduceSync` had not completed. The client received no 202 for these, so it should retry. |
| **Recovery** | Automatic via Kubernetes pod restart. |
| **Mitigation** | Client retries on connection error. Multiple replicas behind a Service ensure other pods serve traffic. |

### Ingestor rolling restart

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | Readiness fails (draining=true), sleep past probe period, HTTP server drains, Kafka producer flushes. |
| **User sees** | Zero 5xx responses if `terminationGracePeriodSeconds` exceeds drain + shutdown budget (45s). |
| **Data loss** | None. |
| **Recovery** | Automatic. |

### Kafka unavailable (from ingestor's perspective)

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | `ProduceSync` fails. Handler returns `503 Service Unavailable` with `Retry-After: 5`. |
| **User sees** | 503 errors. `ingestor_publish_errors_total` climbs. |
| **Data loss** | None — the client knows the metric was not accepted. |
| **Recovery** | Automatic when Kafka recovers. `ProduceSync` retries internally (5 retries). |

## Processor Failures

### Processor `kill -9` mid-batch

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | Uncommitted rows in `PartitionBatch` are lost in memory. Offsets were NOT committed (commit-after-write). |
| **User sees** | Consumer lag spike. |
| **Data loss** | **Zero.** On restart, the consumer resumes from the last committed offset and replays the lost messages. `ReplacingMergeTree` deduplicates any rows that were written but whose commit didn't land. |
| **Recovery** | Automatic. Lag drains at normal processing rate. |
| **Verification** | `make chaos-kill9` is intended to send numbered metrics, kill the processor, and assert that every accepted metric appears after recovery. This script has not yet been run in this environment. |

### Processor graceful shutdown (`SIGTERM`)

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | Shutdown signal triggers `flush_all()` — writes remaining batch, commits offsets. |
| **User sees** | Brief lag spike during rebalance. |
| **Data loss** | None. If flush fails, offsets are not committed and messages replay. |
| **Recovery** | Automatic. |

### ClickHouse unavailable

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | `write_metrics` retries with jittered exponential backoff. After `max_write_attempts` failures, returns `Err`. Circuit breaker opens after 5 consecutive failures. Open circuit pauses the consumer (sleeps, doesn't drop). |
| **User sees** | Ingest continues normally (Kafka buffers). Queries return stale data. `KafkaConsumerLagHigh` alert fires. `processor_circuit_breaker_state` = 1. |
| **Data loss** | **Zero.** Kafka retains all messages. |
| **Recovery** | Automatic. When ClickHouse recovers, circuit transitions to half-open, probes succeed, closes. Backlog drains. |
| **Proof** | `make chaos-clickhouse` — pauses ClickHouse under load and asserts accepted rows recover after unpause. Set `OUTAGE_DURATION` for a longer outage. |

### ClickHouse slow (high latency)

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | Write latency increases. Retries with backoff. Flush takes longer, lag grows. |
| **User sees** | Kafka consumer lag and ClickHouse write latency increase; `KafkaConsumerLagHigh` or `ClickHouseWriteLatencyHigh` may fire. No end-to-end freshness metric is currently exported. |
| **Data loss** | None, unless `max.poll.interval.ms` (300s) is exceeded and the broker evicts the consumer. |
| **Recovery** | Automatic when latency returns to normal, or scale ClickHouse. |

### Poison message (unparseable payload)

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | Deserialization fails. Payload is sent to `metrics.dlq` with `dlq.reason` and `dlq.source_offset` headers. Offset committed only after DLQ send succeeds. |
| **User sees** | `processor_dlq_messages_total` counter increments. `DLQRateHigh` alert fires if sustained. |
| **Data loss** | The metric is not processed (it's broken), but the original bytes are preserved in the DLQ for replay after a producer-side fix. |
| **Recovery** | Manual: fix the producer, replay DLQ messages. |

## Kafka Failures

### Single broker failure (multi-broker production)

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | With RF=3 and `min.insync.replicas=2`, writes continue on remaining brokers. Partitions led by the failed broker re-elect leaders. |
| **User sees** | Brief latency spike during leader election. |
| **Data loss** | None (with `acks=all` and `min.insync.replicas=2`). |
| **Recovery** | Automatic via Kafka leader election. |

### Single broker failure (single-node dev)

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | All topics unavailable. Ingestor returns 503. Processor consumer stalls. |
| **User sees** | Complete pipeline halt. |
| **Data loss** | None — ingestor reports failure, processor doesn't commit. |
| **Recovery** | Restart the broker. |

## Anomaly Detection Failures

### Detector baseline reset (restart/rebalance)

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | Detector state is in-process. On restart, all baselines reset. `min_samples` observations must pass before detection activates. |
| **User sees** | No alerts during warm-up period (30–500 samples depending on alpha). |
| **Data loss** | No data loss, but missed anomalies during warm-up. |
| **Recovery** | Automatic after warm-up. |

### Detector LRU eviction

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | When detector cache is full, least-recently-used series are evicted. Evicted series lose their baseline. |
| **User sees** | `processor_detector_evictions_total` climbs. Evicted series go through warm-up again on next observation. |
| **Data loss** | None. Baseline quality degrades for evicted series. |
| **Recovery** | Increase `PROCESSOR_DETECTOR_CAPACITY` or investigate cardinality. |

## Infrastructure Failures

### Grafana down

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | Dashboard inaccessible. Data continues flowing normally. |
| **User sees** | Cannot view dashboards. |
| **Data loss** | None. Grafana is read-only. |
| **Recovery** | Restart Grafana. |

### Prometheus down

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | Scraping stops. Alert evaluation stops. Pipeline continues. |
| **User sees** | No alerts fire. Gaps in Prometheus TSDB. |
| **Data loss** | Prometheus metric data during the outage is lost. Pipeline data in ClickHouse is unaffected. |
| **Recovery** | Restart Prometheus. Gaps remain. |

## Diagnostic Commands

### Check consumer lag
```bash
docker exec obs-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group processor-group-local
```

### Check circuit breaker state
```bash
curl -s http://localhost:9091/metrics | grep circuit_breaker_state
```

### Check DLQ
```bash
docker exec obs-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic metrics.dlq --from-beginning --max-messages 10 \
  --property print.headers=true
```

### Query ClickHouse for freshness
```sql
SELECT now() - max(ts) AS lag FROM observability.metrics;
```

### Check processor health
```bash
curl http://localhost:9091/health  # liveness
curl http://localhost:9091/ready   # readiness (ClickHouse + circuit)
```
