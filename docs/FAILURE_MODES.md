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
| **User sees** | Zero 5xx responses, provided the pod leaves the Service before it stops accepting. The chart enforces this at render time: `drainDelaySeconds` (15) must be at least readiness `periodSeconds × failureThreshold` (5 × 2), and `terminationGracePeriodSeconds` (45) must exceed the drain plus the 25s HTTP shutdown. The previous probe settings (10 × 3 = 30s) were longer than the 15s drain. |
| **Measured (Compose, one replica)** | `/ready` returns 503 immediately on SIGTERM. `/ingest` kept returning 202 through the roughly 15 s drain, then two connection-refused probes about 0.2 s apart while the replacement started. A retrying client lost nothing (`make chaos-rolling`: 60,000 metrics, 1 client retry, 60,000 unique rows). Zero client-visible errors needs two or more replicas behind a Service; that has not been tested. |
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
| **Verification** | `make chaos-kill9` is intended to send numbered metrics, kill the processor, and assert that every accepted metric appears after recovery. Results are recorded in [bench/results](../bench/results/README.md). |

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
| **User sees** | Kafka consumer lag and ClickHouse write latency increase. `PipelineFreshnessSlow` fires when p99 ingest-to-queryable exceeds 60s, and `ClickHouseRequestTimeouts` fires if inserts hit the request deadline. |
| **Data loss** | None. Each insert is bounded by `PROCESSOR_CLICKHOUSE_TIMEOUT_MS`, so the worst-case flush (attempts × timeout + backoff) stays under `max.poll.interval.ms` (300s). If a consumer is evicted anyway, its uncommitted batch is released and replayed by the new owner. |
| **Recovery** | Automatic when latency returns to normal, or scale ClickHouse. |

### ClickHouse overloaded (`TOO_MANY_PARTS`, memory limit) or misconfigured (missing table, bad credentials)

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | The exception code is classified as retryable. The batch retries with backoff and the circuit breaker may open; offsets stay uncommitted. |
| **User sees** | Lag grows; `ProcessorCommitStalled` fires after two minutes without a commit. |
| **Data loss** | None. Before this classification, every `DB::Exception` counted as permanent, so the batch was bisected and each row routed to the DLQ. |
| **Recovery** | Automatic once ClickHouse catches up on merges, or after the operator applies the migration or fixes the Secret. |

### Consumer group rebalance mid-batch

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | The rebalance callback records revoked partitions. Before its next write, the processor releases their uncommitted records and commits only partitions it still owns. |
| **User sees** | `processor_rebalance_events_total` and `processor_revoked_records_dropped_total` increase. |
| **Data loss** | None. The new owner replays from the last commit; rows already written collapse under `FINAL`. |
| **Recovery** | Automatic. |

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

### Kafka broker killed for memory (single-node dev)

| Aspect | Behaviour |
|--------|-----------|
| **What happens** | The JVM outgrows its container's memory limit and the kernel kills it. Compose has no restart policy, so the broker stays down. Observed twice during sustained-load runs. |
| **User sees** | Ingestor returns 503. Processor consumer lag freezes at the full backlog. |
| **Data loss** | Not measured for this case; the affected runs were aborted, not verified. |
| **Detect** | `journalctl -k \| grep 'Killed process'` shows a `java` kill. `docker top obs-kafka \| grep -o -- '-Xm[sx][0-9A-Za-z]*'` shows the heap. |
| **Cause and prevention** | With no `KAFKA_HEAP_OPTS` the broker used a 1 GB heap inside a 1 GB limit. The heap is now pinned to 512 MB. |
| **Recovery** | `make down && make dev`. |

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
