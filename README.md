# Observability Pipeline

A metrics ingestion and anomaly detection pipeline built with Go, Kafka, Rust, and ClickHouse. The repository includes per-tenant API key authentication and rate limiting, a Kafka dead-letter topic, ClickHouse rollups, Prometheus metrics, Helm deployment templates, and scripts for integration and failure testing.

The processor writes each consumed batch to ClickHouse before committing its Kafka offsets. This gives the data path at-least-once delivery: a crash can cause replay, and ClickHouse deduplication uses tenant, metric, host, timestamp, partition, and offset. Local integration, recovery and sustained-load results from one laptop are checked in under `bench/results/` with the host described. They are not production capacity figures.

## Quick start

Requirements: Docker Compose v2, Go 1.25+, and Rust stable. The local Compose stack is a single-broker development environment using PLAINTEXT Kafka and development credentials. Do not use it as a production deployment.

```bash
git clone https://github.com/KevinKattakayam/datadog
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
                                                  ├→ alerts.fired
                                                  └→ metrics.dlq
```

The ingestor validates and synchronously produces records with `acks=all`; it returns success only after Kafka acknowledges the record. The processor batches by source partition, persists the metric rows, and then synchronously commits the covered offsets. Failed ClickHouse writes leave offsets uncommitted for replay. Only row-level data rejections (identified by ClickHouse exception code) are isolated to the DLQ. Overload, schema, auth and timeout errors retry, and every insert has a deadline. On rebalance, buffered records for revoked partitions are released to the new owner rather than committed. A failed flush pauses further consumption so the in-memory batch stays bounded while Kafka retains the backlog.

The processor publishes anomaly alerts to Kafka as advisory fan-out, each with an `alert.id` header derived from its source record, so consumers can deduplicate alerts re-published after a crash. ClickHouse is the durability gate for source offsets. A crash after a ClickHouse insert and before the offset commit can replay rows; queries over raw `ReplacingMergeTree` data should use `FINAL` or duplicate-safe aggregation. Rollups are aggregated ClickHouse views.

## HTTP API

Authenticated ingestion routes:

- `POST /ingest` — one metric. Returns `202` after Kafka acknowledges it, `400` if it is invalid, `429` over quota, and `503` when Kafka cannot acknowledge.
- `POST /ingest/batch` — up to 1000 metrics. Returns:
  - `202` when every item is acknowledged;
  - `207` for any partial result;
  - `400` when no item is valid;
  - `429` when the tenant's metrics quota cannot admit the batch;
  - `503` when Kafka acknowledges none.

  Counts partition the request (`accepted + rejected + skipped`). `errors` lists each non-accepted item by request index; resend only items with `"retryable": true`.
- Any ingest route returns `401` without a valid key and `413` for a body over `INGESTOR_MAX_BODY_MIB` (default 5). Every `429` and `503` carries `Retry-After`.

```json
{"status":"partial","accepted":2,"rejected":1,"skipped":1,
 "errors":[{"index":1,"error":"metric name must be dot-separated alphanumeric: bad..name","retryable":false},
           {"index":2,"error":"not acknowledged by kafka: NOT_ENOUGH_REPLICAS","retryable":true}]}
```

Limits are per tenant and per replica: `INGESTOR_RATE_LIMIT_RPS` (requests) and `INGESTOR_TENANT_METRICS_PER_SEC` (metrics, charged per batch item). API keys reload when the mounted file changes, or immediately on `SIGHUP`. A missing, empty or malformed file keeps the current keys.

The Go SDK in `sdk/go` implements this contract: it sends the bearer token, resends only retryable items, honours `Retry-After`, bounds its buffer and reports every outcome through `Stats()`.

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
- `alerts` uses Kafka partition and offset in a `ReplacingMergeTree` key so source-record replays collapse under `FINAL`. Existing deployments need the explicit [alerts migration](infra/clickhouse/migrations/001_alerts_replacing.sql); `dlq_events` is a schema for DLQ observability, while original poison payloads are currently preserved in Kafka topic `metrics.dlq` with reason and source-coordinate headers.
- `cardinality_hourly` attributes host and tag-combination cardinality by tenant and metric.

The Compose bootstrap applies idempotent creation and additive column changes to reused local volumes. It does not rebuild an existing table to change its ordering key; migrate/rebuild older production tables deliberately before relying on tenant-first pruning.

No ClickHouse row policy is installed by the current schema. Tenant identity is authenticated at ingestion and retained in Kafka records and ClickHouse rows, but query isolation must be enforced by the consuming application or a separately configured ClickHouse user/row policy.

## Anomaly detection

The processor applies EWMA and rolling Z-score detectors to each `(tenant, metric, host)` series. The Z-score detector scores each value against the window before adding it, uses Welford's algorithm (stable for values near 1e12), and handles flat baselines like EWMA does. Detector state uses a bounded LRU registry. EWMA handles a near-flat baseline with a relative-deviation fallback and ignores non-finite values. State is in process and resets on restart or rebalance; there is no seasonality model or persisted baseline yet.

The report-only cardinality governor attributes host and tag-combination counts and estimated payload bytes by tenant and metric. The Grafana **Cardinality and Cost** dashboard also shows detector-cache utilization and evictions. Byte values estimate uncompressed payload size; they are not disk usage or billing values. The feature does not reject metrics. See [the design note](docs/CARDINALITY_GOVERNOR.md) and [runbook](docs/runbooks/cardinality.md).

## Security and deployment

The Helm chart under `helm/observability-pipeline/` deploys the ingestor and processor only. Kafka, ClickHouse and Prometheus are external: use their operators or managed services. The chart ships a `ServiceMonitor` and a `PrometheusRule` (the same rule files Compose loads) for kube-prometheus-stack.

```bash
# 1. Secrets (names configurable in values.yaml)
kubectl -n observability create secret generic observability-api-keys \
  --from-file=api-keys=./api-keys          # lines of sha256hex:tenant_id
kubectl -n observability create secret generic observability-clickhouse-processor \
  --from-literal=username=processor --from-literal=password='...'

# 2. Install (point kafkaBootstrapServers / clickhouseUrl at your endpoints)
helm upgrade --install obs helm/observability-pipeline -n observability \
  --set serviceMonitor.enabled=true --set prometheusRule.enabled=true
```

Pods run non-root with read-only filesystems, dropped capabilities, no service-account token, and NetworkPolicies limiting egress to Kafka, ClickHouse and DNS. The chart refuses to render configurations that would fail quietly in a cluster:

- processor replicas or autoscaler maximum above `kafka.rawTopicPartitions`;
- HPA and KEDA enabled together;
- a ClickHouse URL with a path;
- an ingestor drain delay shorter than the readiness-removal time.

The chart does not configure TLS or Kafka SASL/ACLs; supply them from the environment before exposing it beyond a trusted network.

Rotating keys means updating the Secret: the ingestor picks up the new file within `API_KEYS_RELOAD_INTERVAL_SECONDS`, with no restart.

Local Compose routes Alertmanager notifications to a no-op receiver so it starts without external credentials. The production routing example in `infra/alertmanager/alertmanager.production.example.yml` needs real secret-backed Slack and PagerDuty receiver settings before deployment.

CI (`.github/workflows/ci.yml`, branch `master`) runs:

- Go race tests for the ingestor and SDK, and Rust fmt, clippy and tests;
- `promtool` rule checks and unit tests;
- Helm lint, kubeconform validation of three value sets, and tests that the chart's guards refuse bad configurations;
- govulncheck, cargo-audit, pinned Trivy image scans and SBOMs;
- a nightly full-stack integration and chaos run.

## Development and verification

```bash
make test               # Go and Rust unit tests
make sdk-test           # Go SDK delivery tests
make lint               # go vet and cargo clippy
make rules-test         # promtool check + unit tests for alerts/recording rules
make helm-lint          # chart lint (requires helm)
make integration-test   # requires the Compose stack
make chaos-kill9        # SIGKILL processor and check numbered metrics
make chaos-clickhouse   # pause ClickHouse and check recovery
```

The chaos and benchmark scripts require Docker access and a running local stack. [Results and host details](bench/results/) cover: SIGKILL mid-stream and replay deduplication (zero loss, duplicates removed); graceful processor and ingestor restarts under load (zero loss); and sustained ingest at 2,000, 5,000 and 8,000 metrics/sec (median of three 60-second runs, no errors, p99 under 8 ms per 100-metric request, processor kept up). Every figure comes from one 8-core laptop that also ran the load generator, so they describe that machine, not production capacity, and the throughput ceiling has not been found. Not measured: detector quality on real data, behaviour with several replicas on Kubernetes (including zero client-visible errors during a rolling update), and long soak runs. Detector quality on synthetic series is in [docs/DETECTOR_EVALUATION.md](docs/DETECTOR_EVALUATION.md).

## Repository map

- `ingestor/` — Go HTTP API, auth, quotas, validation, Kafka producer.
- `sdk/go/` — Go client implementing the ingest contract.
- `processor/` — Rust Kafka consumer, detectors, ClickHouse writer, DLQ producer.
- `infra/` — ClickHouse schema, Kafka topics, Prometheus alerts, Grafana and tracing configuration.
- `helm/observability-pipeline/` — Kubernetes chart.
- `tests/` — integration script, load tools, Prometheus rule tests.
- `bench/chaos/` — processor kill and ClickHouse outage scenarios.
- `docs/` — architecture, development, failure modes, runbooks, and [roadmap status](docs/ROADMAP_STATUS.md).

See [Architecture](docs/ARCHITECTURE.md), [Failure Modes](docs/FAILURE_MODES.md), [Development](docs/DEVELOPMENT.md), and [Interview and demo notes](docs/INTERVIEW_GUIDE.md) for implementation details and a reproducible walkthrough.

## License

MIT. See [LICENSE](LICENSE).
