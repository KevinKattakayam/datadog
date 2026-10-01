# Observability Pipeline

A metrics ingestion and anomaly detection pipeline built with Go, Kafka, Rust, and ClickHouse. The repository includes per-tenant API key authentication and rate limiting, a Kafka dead-letter topic, ClickHouse rollups, Prometheus metrics, Helm deployment templates, and scripts for integration and failure testing.

The processor writes each consumed batch to ClickHouse before committing its Kafka offsets. This gives the data path at-least-once delivery: a crash can cause replay, and ClickHouse deduplication uses tenant, metric, host, timestamp, partition, and offset. Short local integration and recovery smoke results are checked in; they are not a sustained-throughput benchmark or a three-run median.

## Quick start

Requirements: Docker Compose v2, Go 1.22+, and Rust stable. The local Compose stack is a single-broker development environment using PLAINTEXT Kafka and development credentials. Do not use it as a production deployment.

```bash
git clone https://github.com/Kevinbastin/datadog
cd datadog
make dev
```

Send a metric (the local Compose profile explicitly enables unauthenticated development mode):

```bash
curl -X POST http://localhost:8080/ingest \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: ignored-in-authenticated-mode' \
  -d '{"name":"api.request.duration_ms","value":42.5,"unit":"ms","timestamp":1790812800,"host":"dev-laptop"}'
```

Local mode maps service ports to loopback. Open Grafana at `http://localhost:3000` (local development credentials are `admin/admin`), or query ClickHouse at `http://localhost:8123`.

For an authenticated local request, put one line per key in a file using `sha256hex:tenant_id` format, set `API_KEYS_PATH` to that file, and set `AUTH_ALLOW_INSECURE_DEV=false`. The raw key is sent as `Authorization: Bearer <key>`; `X-Tenant-ID` is not trusted for tenant identity.

## Data path

```text
HTTP client → Go ingestor → metrics.raw (Kafka) → Rust processor → ClickHouse
                                                  ├→ metrics.processed
                                                  ├→ alerts.fired
                                                  └→ metrics.dlq
```

The ingestor validates and synchronously produces records with `acks=all`; it returns success only after Kafka acknowledges the record. The processor batches by source partition, persists the metric rows, and then synchronously commits the covered offsets. Failed ClickHouse writes leave offsets uncommitted for replay. A failed flush pauses further consumption so the in-memory batch stays bounded while Kafka retains the backlog.

The processor also publishes processed metrics and anomaly alerts to Kafka as advisory fan-out. ClickHouse is the durability gate for source offsets. A crash after a ClickHouse insert and before the offset commit can replay rows; queries over raw `ReplacingMergeTree` data should use `FINAL` or duplicate-safe aggregation. Rollups are aggregated ClickHouse views.

## HTTP API

Authenticated ingestion routes:

- `POST /ingest` — one metric; `202` after Kafka acknowledgement, `503` when Kafka cannot acknowledge.
- `POST /ingest/batch` — a batch; `202` when all valid metrics are acknowledged, `207` for partial produce failures, and `503` if none are acknowledged. Invalid records are counted as skipped in the response.

Operational routes do not require an ingestion API key:

- `GET /health` — process liveness.
- `GET /ready` — readiness, including Kafka reachability and drain state.
- `GET /metrics` — ingestor Prometheus metrics.
- Processor `GET /health`, `/ready`, and `/metrics` on port `9091` — liveness, ClickHouse/circuit readiness, and processor metrics.

Example:

```bash
curl -X POST http://localhost:8080/ingest \
  -H 'Authorization: Bearer <api-key>' \
  -H 'Content-Type: application/json' \
  -d '{"name":"cpu.usage","value":72.1,"timestamp":1790812800,"host":"prod-01"}'
```

## Storage and delivery details

Kafka topics are configured by `infra/kafka/topics.sh`; local Compose uses one broker and replication factor 1. Production topic creation should use replication factor 3 and `min.insync.replicas=2` across independent brokers/AZs. Increasing processor replicas beyond the six partitions of `metrics.raw` does not increase consumer parallelism.

ClickHouse schema is in `infra/clickhouse/schema.sql`:

- `metrics` uses `ReplacingMergeTree` with Kafka coordinates in the ordering key and 90-day raw retention.
- Hourly and daily `AggregatingMergeTree` materialized views store rollups. They process each inserted block, so replays may be counted again; only raw table reads using `FINAL` collapse replayed rows today.
- `alerts` stores anomaly records; `dlq_events` is a schema for DLQ observability, while original poison payloads are currently preserved in Kafka topic `metrics.dlq` with reason and source-coordinate headers.
- `cardinality_hourly` attributes host and tag-combination cardinality by tenant and metric.

The Compose bootstrap applies idempotent creation and additive column changes to reused local volumes. It does not rebuild an existing table to change its ordering key; migrate/rebuild older production tables deliberately before relying on tenant-first pruning.

No ClickHouse row policy is installed by the current schema. Tenant identity is authenticated at ingestion and retained in Kafka records and ClickHouse rows, but query isolation must be enforced by the consuming application or a separately configured ClickHouse user/row policy.

## Anomaly detection

The processor applies EWMA and rolling Z-score detectors to each `(tenant, metric, host)` series. Detector state uses a bounded LRU registry. EWMA handles a near-flat baseline with a relative-deviation fallback and ignores non-finite values. State is in process and resets on restart or rebalance; there is no seasonality model or persisted baseline yet.

The report-only cardinality governor attributes host and tag-combination counts and estimated payload bytes by tenant and metric. The Grafana **Cardinality and Cost** dashboard also shows detector-cache utilization and evictions. Byte values estimate uncompressed payload size; they are not disk usage or billing values. The feature does not reject metrics. See [the design note](docs/CARDINALITY_GOVERNOR.md) and [runbook](docs/runbooks/cardinality.md).

## Security and deployment

The Helm chart is under `helm/observability-pipeline/`. It configures non-root pod execution, read-only container filesystems, dropped Linux capabilities, probes, topology spread, and existing Kubernetes Secrets for API key hashes and ClickHouse processor credentials. Create `observability-api-keys` with an `api-keys` file and `observability-clickhouse-processor` with `username` and `password` keys before installing the chart; the ClickHouse account must have only the required `SELECT` and `INSERT` grants. The chart does not configure TLS or Kafka SASL/ACLs, and must be deployed only into a trusted network until those are supplied by the environment. Processor KEDA scaling is optional and disabled by default; install KEDA and enable `processor.kedaScaling` to scale from lag, keeping its maximum at or below the raw topic's six partitions.

Local Compose routes Alertmanager notifications to a no-op receiver so it starts without external credentials. The production routing example in `infra/alertmanager/alertmanager.production.example.yml` needs real secret-backed Slack and PagerDuty receiver settings before deployment.

CI configuration is in `.github/workflows/ci.yml` and targets `master`. It includes Go race tests, Rust checks, Helm rendering/lint, dependency advisory checks, and image scanning. Review workflow action versions and security findings as part of normal dependency maintenance.

## Development and verification

```bash
make test               # Go and Rust unit tests
make lint               # go vet and cargo clippy
make integration-test   # requires the Compose stack
make chaos-kill9        # SIGKILL processor and check numbered metrics
make chaos-clickhouse   # pause ClickHouse and check recovery
```

The chaos scripts require Docker access and a running local stack. [Captured smoke output and host details](bench/results/) show one local run per recovery scenario. The samples prove the scripted assertions on this machine only; they do not establish production throughput, latency, repeated-run medians, rolling-restart behaviour, or detector quality. Re-run at representative production scale before using those claims externally.

## Repository map

- `ingestor/` — Go HTTP API, auth, validation, Kafka producer.
- `processor/` — Rust Kafka consumer, detectors, ClickHouse writer, DLQ producer.
- `infra/` — ClickHouse schema, Kafka topics, Prometheus alerts, Grafana and tracing configuration.
- `helm/observability-pipeline/` — Kubernetes chart.
- `tests/` — unit-supporting integration and load tools.
- `bench/chaos/` — processor kill and ClickHouse outage scenarios.
- `docs/` — architecture, development, failure modes, and operational runbooks.

See [Architecture](docs/ARCHITECTURE.md), [Failure Modes](docs/FAILURE_MODES.md), [Development](docs/DEVELOPMENT.md), and [Interview and demo notes](docs/INTERVIEW_GUIDE.md) for implementation details and a reproducible walkthrough.
