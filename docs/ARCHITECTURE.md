# Architecture

## Overview

The Observability Pipeline is a metrics ingestion and anomaly-detection system designed for at-least-once delivery. The repository contains integration and chaos scripts, but a passing result must be produced in the target environment before describing the guarantee as verified. The pipeline moves data through four stages:

```
Client → Go Ingestor → Kafka (KRaft) → Rust Processor → ClickHouse
```

Kafka produce acknowledgements and processor offset commits are used as durability boundaries. A request in flight during an ingestor crash may not have reached Kafka; clients should retry when they did not receive a success response.

## Design Principles

1. **Kafka retains uncommitted work.** The processor stops consuming after a failed flush and retries the same batch before reading more records.
2. **Expose operational signals.** Circuit state, write failures, detector evictions, and DLQ counts are exported; consumer lag is available from Kafka tooling/exporter.
3. **Authenticate tenant identity at ingestion.** API-key hashes resolve the tenant; a per-process rate limiter is applied to ingestion routes. ClickHouse row policies and an enforced series budget are not implemented.
4. **Measure, don't claim.** Run the provided scripts and retain environment details before publishing benchmark results.

## Component Deep Dive

### Go Ingestor (`ingestor/`)

**Purpose:** Receive HTTP metric payloads, validate them, and publish to Kafka.

**Key decisions:**

- **`ProduceSync` with `acks=all`.** The ingestor returns `202 Accepted` only after every in-sync replica acknowledges the record. Kafka unavailable → `503 Service Unavailable` with `Retry-After`.
- **API-key authentication.** Keys are stored as SHA-256 hashes loaded from a mounted Kubernetes Secret. The plaintext key is shown once at issue time and never persisted.
- **Per-tenant rate limiting.** Each tenant gets an independent token bucket. The lock on the bucket is per-bucket (`tokenBucket.mu`), not per-registry, so one tenant's contention does not block another.
- **Shutdown ordering.** Readiness fails → sleep past probe period → `srv.Shutdown()` drains in-flight HTTP requests → Kafka producer flushes. This ordering ensures no record is produced after the flush.

**Files:**

| File | Purpose |
|------|---------|
| `cmd/server/main.go` | Wiring, middleware registration, shutdown |
| `internal/handler/ingest.go` | Single and batch ingestion with 207 partial |
| `internal/handler/health.go` | Liveness + readiness with drain support |
| `internal/producer/kafka.go` | Synchronous produce with error classification |
| `internal/middleware/auth.go` | SHA-256 API-key auth |
| `internal/middleware/tenant.go` | Per-tenant rate limiting |
| `internal/validator/metric.go` | Name, tag, host, timestamp validation |

### Rust Processor (`processor/`)

**Purpose:** Consume from Kafka, apply anomaly detection, write to ClickHouse, publish alerts.

**Key decisions:**

- **Per-partition batching with commit-after-write.** Rows accumulate in a per-partition `PartitionBatch`. On flush, all rows are written to ClickHouse *and the write must succeed* before any offset is committed. If the write fails, offsets stay put and Kafka replays the messages.
- **A failed flush pauses consumption.** The consumer retains its in-memory batch and retries on its flush timer. Kafka lag grows while ClickHouse is unavailable.
- **Retry with jittered exponential backoff.** Three replicas all failing on the same ClickHouse outage use jitter to avoid synchronized retry storms.
- **LRU-bounded detector state.** Detector baselines are keyed by `tenant|name|host` in an LRU cache. Evictions are exported as `processor_detector_evictions_total`.
- **DLQ with error context.** Unparseable payloads go to `metrics.dlq` with headers carrying the reason, source partition, source offset, and timestamp. The `dlq_events` table is currently schema-only.

**Files:**

| File | Purpose |
|------|---------|
| `src/main.rs` | Bootstrap, signal handling, HTTP metrics server |
| `src/consumer.rs` | Consumer loop with per-partition batching |
| `src/storage/clickhouse.rs` | Writer with circuit breaker and retry |
| `src/dlq.rs` | Dead-letter producer |
| `src/detector/ewma.rs` | EWMA with flat-baseline fallback |
| `src/detector/zscore.rs` | Rolling Z-score |
| `src/detector/registry.rs` | LRU-bounded detector state |
| `src/model.rs` | MetricRow, AlertRow, data types |
| `src/config.rs` | Env-var configuration |
| `src/metrics.rs` | Prometheus metric definitions |
| `src/producer.rs` | Alert/processed fan-out producer |

### ClickHouse Schema (`infra/clickhouse/schema.sql`)

- **`ReplacingMergeTree`** on `metrics` with `ORDER BY (tenant_id, name, host, ts, kafka_partition, kafka_offset)`. Kafka coordinates in the sort key give deterministic dedup after replays.
- **`AggregatingMergeTree`** rollups at hourly and daily granularity. Materialized views process each inserted block, so a replay can contribute twice; treat rollups as at-least-once aggregates until a deduplicated rollup path is implemented.
- **Tiered retention:** raw 90 days, rollups 13 months.
- **`by_host` projection** for incident-investigation queries.
- **Cardinality tracking MV** for per-tenant host and tag-combination attribution; no budget enforcement is wired.

### Kafka Topics

| Topic | Partitions | Purpose |
|-------|-----------|---------|
| `metrics.raw` | 6 | Ingestor → Processor |
| `metrics.processed` | 6 | Fan-out of processed metrics |
| `alerts.fired` | 3 | Anomaly alerts |
| `metrics.dlq` | 3 | Dead-letter queue (7-day retention) |

**Important:** In the single-node dev stack, RF=1. In production, set `REPLICATION_FACTOR=3` and `MIN_IN_SYNC_REPLICAS=2` via the environment variables in `infra/kafka/topics.sh`.

## Data Flow: What Happens to a Metric

1. Client POSTs to `/ingest/batch` with `Authorization: Bearer <key>`.
2. `auth.go` hashes the key, looks up the tenant, sets `tenant_id` in context.
3. `tenant.go` checks the per-tenant rate limit. 429 if exceeded.
4. `validator/metric.go` validates name, host, timestamp, tag lengths. 400 if invalid.
5. `producer/kafka.go` calls `ProduceSync(acks=all)`. Error → 503 + Retry-After.
6. Record lands in `metrics.raw`, key = `tenant|host`, headers include `tenant_id` and `schema_version`.
7. Processor's `ConsumerLoop` receives the record, deserialises it. Failure → DLQ.
8. `DetectorRegistry` evaluates EWMA and Z-score detectors. Anomaly → alert published to `alerts.fired`.
9. Row added to the partition's `PartitionBatch`.
10. On batch full or timer tick, all rows are written to ClickHouse in one insert.
11. Write succeeds → offsets committed (sync). Write fails → offsets NOT committed, messages replayed.
12. `ReplacingMergeTree` collapses any duplicate rows from replays at merge time.

## Trade-offs

| Decision | Cost | Benefit |
|----------|------|---------|
| Commit after write, not after send | Higher commit rate, slightly lower throughput | Uncommitted data is replayable after a crash |
| Circuit breaker pauses consumer | Lag grows during outages | No silent data deletion |
| `ProduceSync` instead of async | One broker round-trip per batch | 202 means "in Kafka" |
| LRU detector cache | Evicted series lose their baseline | Bounded memory |
| Per-process rate limiter | N replicas = N × the configured limit | No Redis dependency |
| `ReplacingMergeTree` dedup | Queries need `FINAL` or aggregation | No transaction coordinator |

## Known Limitations

- **No TLS between services.** Kafka is PLAINTEXT, ClickHouse runs without auth in dev. Production requires SASL/SCRAM + TLS.
- **No seasonality model.** A predictable weekday morning ramp scores as an anomaly.
- **Detector state is in-process.** Lost on restart or rebalance; baselines warm up again.
- **Per-process rate limiter.** Not distributed. N replicas = N × the limit.
- **No ClickHouse row policy.** Tenant IDs are stored, but query isolation must be provided by a separate access layer.
- **No end-to-end freshness metric or benchmark results are checked in.**
- **Rollup replay sensitivity.** The materialized views can count duplicate source replays even when raw table reads use `FINAL`.
- **No exactly-once.** ClickHouse cannot participate in Kafka transactions. At-least-once + idempotent writes give effectively-once at rest.
