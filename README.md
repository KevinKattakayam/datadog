# Enterprise Observability Pipeline

A production-grade distributed observability pipeline built with **Go**, **Rust**, **Apache Kafka**, **Prometheus**, **Grafana**, and **ClickHouse** — the same class of infrastructure that powers Datadog, New Relic, and Grafana Cloud.

```
git clone https://github.com/Kevinbastin/observability-pipeline
cd observability-pipeline
cp .env.example .env
make dev
open http://localhost:3000   # Grafana — admin/admin
```

---

## Architecture

```
┌─────────────────── Client Layer ───────────────────┐
│  SDK (Go)  ·  HTTP POST /ingest  ·  gRPC :50051    │
└────────────────────────┬───────────────────────────┘
                         │
                         ▼
┌─────────────────── Ingestor (Go) ──────────────────┐
│  Gin HTTP  ·  gRPC TCP server  ·  Rate Limiting    │
│  Multi-Tenancy  ·  Avro Validation  ·  Redis Cache │
└────────────────────────┬───────────────────────────┘
                         │ metrics.raw (Avro)
                         ▼
┌─────────────────── Kafka (KRaft) ──────────────────┐
│  3 Topics · 6 Partitions · RF=2 · DLQ              │
│  metrics.raw · metrics.processed · alerts.fired    │
└───────────┬────────────────────────────────────────┘
            │
            ▼
┌─────────────────── Processor (Rust) ───────────────┐
│  EWMA + Z-Score Anomaly Detection                  │
│  Circuit Breaker · Batch Writer                    │
└─────────┬──────────────────────┬───────────────────┘
          │                      │
          ▼                      ▼
┌─── ClickHouse ──┐   ┌─── Kafka Topics ──────────┐
│  MergeTree      │   │  metrics.processed         │
│  1-year TTL     │   │  alerts.fired              │
│  Hourly Rollups │   └───────────────────────────┘
└─────────────────┘
          │
          ▼
┌─────────────────── Observability Layer ────────────┐
│  Prometheus (15d TSDB)  ·  AlertManager            │
│  Grafana Dashboards  ·  Tempo  ·  Jaeger           │
│  OTEL Collector  ·  Schema Registry                │
└────────────────────────────────────────────────────┘
```

**Data flow:** Metrics travel Client → Ingestor → Kafka → Rust Processor → ClickHouse + Prometheus → Grafana. Every hop carries W3C Trace Context headers for end-to-end distributed tracing.

---

## Technology Stack

| Layer | Technology | Version | Purpose |
|---|---|---|---|
| **Ingestion** | Go + Gin | 1.22 | HTTP/gRPC metric receiver |
| **Transport** | Apache Kafka (KRaft) | 7.6.0 | Durable, partitioned message log |
| **Processing** | Rust + tokio | 1.77+ | Stream consumption + anomaly detection |
| **Short-term store** | Prometheus | 2.51.0 | 15-day TSDB + alerting |
| **Long-term store** | ClickHouse | 24.3 LTS | Columnar analytics, 1-year retention |
| **Visualization** | Grafana | 10.4.0 | Pre-provisioned dashboards |
| **Tracing** | Grafana Tempo + Jaeger | 2.4.1 / 1.55 | Distributed trace storage and UI |
| **Telemetry routing** | OpenTelemetry Collector | 0.96.0 | OTLP fan-out to Tempo and Jaeger |
| **Caching** | Redis | 7.2 | Cache-aside for recent metrics |
| **Schema governance** | Confluent Schema Registry | 7.6.0 | Avro schema enforcement |
| **Data replication** | Kafka Connect | 7.6.0 | Kafka → ClickHouse sink |
| **Alerting** | AlertManager | 0.27.0 | Slack + PagerDuty routing |
| **Kubernetes** | Helm | — | Production deployment chart |
| **Infrastructure** | Terraform | — | AWS EKS provisioning |

### Go Libraries

| Library | Purpose |
|---|---|
| `github.com/twmb/franz-go` | High-throughput Kafka producer with backpressure |
| `github.com/gin-gonic/gin` | HTTP router for `/ingest`, `/batch`, `/health` |
| `github.com/prometheus/client_golang` | Self-instrumentation via `/metrics` |
| `github.com/redis/go-redis/v9` | Redis cache-aside client |
| `go.opentelemetry.io/otel` | W3C Trace Context propagation |

### Rust Crates

| Crate | Purpose |
|---|---|
| `rdkafka` | Kafka consumer (wraps librdkafka) |
| `tokio` | Async runtime — concurrent message processing |
| `serde / serde_json` | Zero-copy metric deserialization |
| `clickhouse` | Native ClickHouse binary protocol writer |
| `tracing` | Structured JSON logs |

---

## Prerequisites

```bash
# Go 1.22+
go version

# Rust 1.77+
rustc --version

# Docker + Compose
docker compose version

# (Optional) for load testing
brew install k6        # macOS
sudo apt install k6    # Ubuntu
```

---

## Quick Start

### 1. Clone and configure

```bash
git clone https://github.com/Kevinbastin/observability-pipeline
cd observability-pipeline
cp .env.example .env
# Edit .env — set SLACK_WEBHOOK_URL and PAGERDUTY_ROUTING_KEY if you want alerts
```

### 2. Start the full stack

```bash
make dev
```

This starts 14 containers: Kafka, ClickHouse, Redis, Prometheus, Grafana, Tempo, Jaeger, AlertManager, Schema Registry, Kafka Connect, OTEL Collector, and your Go/Rust services. The first run downloads container images (~2.5 GB). Subsequent starts are instant.

### 3. Verify everything is healthy

```bash
make status
```

### 4. Send test traffic

```bash
make fire        # 1000 metrics/sec for 60s
make load-test   # k6 sustained load test (requires k6)
```

### 5. Open dashboards

| Service | URL | Credentials |
|---|---|---|
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | — |
| **AlertManager** | http://localhost:9093 | — |
| **Jaeger** | http://localhost:16686 | — |
| **Tempo** | http://localhost:3200 | — |
| **Schema Registry** | http://localhost:8081 | — |
| **Kafka Connect** | http://localhost:8083 | — |
| **ClickHouse HTTP** | http://localhost:8123 | — |

---

## API Reference

### HTTP Ingestor — `POST /ingest`

```bash
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: acme" \
  -H "traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" \
  -d '{
    "name":      "api.request.duration_ms",
    "value":     142.7,
    "unit":      "ms",
    "tags":      { "service": "checkout", "region": "us-east-1" },
    "timestamp": 1714900000,
    "host":      "prod-api-07"
  }'
```

### HTTP Ingestor — `POST /ingest/batch`

```bash
curl -X POST http://localhost:8080/ingest/batch \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: acme" \
  -d '{ "metrics": [ { "name": "cpu.usage", "value": 72.1, "timestamp": 1714900000, "host": "prod-01" } ] }'
```

### gRPC — Port `:50051`

The gRPC server accepts newline-delimited JSON (no protoc required in dev). Send metrics directly:

```bash
echo '{"name":"cpu.usage","value":85.5,"timestamp":1714900000,"host":"prod-01"}' \
  | nc localhost 50051
```

To generate production binary gRPC stubs from the proto definition:

```bash
make grpc-gen   # requires protoc + protoc-gen-go + protoc-gen-go-grpc
```

### Health Check

```bash
curl http://localhost:8080/health
# {"status":"ok","kafka":"connected","redis":"connected"}
```

---

## Kafka Topics

| Topic | Partitions | RF | Purpose |
|---|---|---|---|
| `metrics.raw` | 6 | 2 | Raw validated metrics from ingestor. Key = hostname |
| `metrics.processed` | 6 | 2 | Anomaly-annotated metrics from Rust processor |
| `alerts.fired` | 3 | 2 | Alert events for audit log and alerting service |
| `metrics.dlq` | 3 | 2 | Dead-letter queue — malformed messages, 7-day retention |

Consumer offsets are committed **only after a successful ClickHouse write**, guaranteeing at-least-once semantics with no silent data loss on crash.

---

## Anomaly Detection

The Rust processor runs two independent detectors per metric stream:

### EWMA (Exponential Weighted Moving Average)

Tracks the rolling mean with exponential decay. Flags a value as anomalous when it deviates more than `threshold_sigmas` standard deviations from the EWMA.

```
ewma_t = α × value + (1 − α) × ewma_{t-1}
variance_t = (1 − α) × (variance_{t-1} + α × (value − ewma_{t-1})²)
anomaly = |value − ewma_t| > threshold_sigmas × √variance_t
```

Configurable: `PROCESSOR_EWMA_ALPHA` (default `0.3`), `PROCESSOR_ANOMALY_THRESHOLD` (default `3.0σ`).

### Z-Score (Rolling Window)

Maintains a 5-minute sliding window of values. Computes mean and standard deviation over the window, flags values where `|z| > threshold`.

Configurable: `PROCESSOR_ZSCORE_WINDOW` (default `300` samples).

---

## ClickHouse Schema

```sql
-- Main metrics table: 1-year TTL, monthly partitioning
CREATE TABLE metrics (
    ts       DateTime,
    name     LowCardinality(String),
    value    Float64,
    host     LowCardinality(String),
    tags     Map(String, String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (name, host, ts)
TTL ts + INTERVAL 1 YEAR DELETE;

-- Hourly rollup materialized view (pre-aggregated at write time)
CREATE MATERIALIZED VIEW metrics_hourly
ENGINE = AggregatingMergeTree()
ORDER BY (name, host, hour) AS
SELECT
    toStartOfHour(ts) AS hour,
    name, host,
    avgState(value)   AS avg_value,
    maxState(value)   AS max_value
FROM metrics GROUP BY hour, name, host;
```

Query 6 months of p99 latency in milliseconds:

```sql
SELECT
    toStartOfDay(ts) AS day,
    quantile(0.99)(value) AS p99
FROM metrics
WHERE name = 'api.request.duration_ms'
  AND ts >= now() - INTERVAL 180 DAY
GROUP BY day
ORDER BY day;
```

---

## Prometheus Metrics

Every service self-instruments and exposes a `/metrics` endpoint that Prometheus scrapes every 10 seconds.

| Metric | Type | Description |
|---|---|---|
| `ingestor_requests_total` | Counter | HTTP requests received. Labels: `method`, `status_code` |
| `ingestor_publish_errors_total` | Counter | Kafka publish failures. Label: `error_type` |
| `ingestor_batch_size` | Histogram | Distribution of batch sizes. Buckets: 1, 10, 100, 500, 1000 |
| `processor_messages_consumed_total` | Counter | Kafka messages processed. Labels: `topic`, `partition` |
| `processor_anomalies_detected_total` | Counter | Anomalies detected. Labels: `metric_name`, `severity` |
| `processor_processing_latency_seconds` | Histogram | End-to-end message processing time (p50/p95/p99) |
| `clickhouse_write_duration_seconds` | Histogram | ClickHouse batch insert latency |
| `clickhouse_circuit_breaker_state` | Gauge | Circuit breaker state: 0=closed, 1=open, 2=half-open |
| `kafka_consumer_lag_sum` | Gauge | Consumer group lag. Alert fires when > 10,000 |

---

## Grafana Dashboards

Four dashboards are provisioned automatically on startup (no manual import needed):

| Dashboard | Description |
|---|---|
| **Pipeline Overview** | Throughput, error rate, p99 latency, Kafka lag — the main ops view |
| **SLO / Error Budget** | Multi-window burn rate (5m/1h/6h/3d), error budget remaining |
| **Historical Trends** | 90-day ClickHouse-backed trend analysis |
| **Distributed Tracing** | Tempo trace correlation linked from Prometheus exemplars |

---

## Alert Rules

### Pipeline Alerts (`infra/prometheus/alerts/pipeline.yml`)

| Alert | Condition | Severity |
|---|---|---|
| `IngestorDown` | Ingestor unreachable for > 1m | critical |
| `ProcessorDown` | Rust processor unreachable for > 1m | critical |
| `KafkaConsumerLagHigh` | Consumer lag > 10,000 for 2m | warning |
| `HighErrorRate` | Error rate > 5% for 5m | warning |
| `HighPublishLatency` | p99 publish latency > 500ms for 5m | warning |
| `HighAnomalyRate` | Anomaly rate > 10/min for 10m | warning |
| `ClickHouseCircuitOpen` | Circuit breaker open > 30s | critical |

### SLO Alerts (`infra/prometheus/alerts/slo.yml`)

Multi-window, multi-burn-rate alerts following the Google SRE model:

| Alert | Window | Burn Rate | Severity |
|---|---|---|---|
| `SLOFastBurn` | 5m + 1h | > 14x | critical |
| `SLOSlowBurn` | 6h + 3d | > 2x | warning |

---

## Multi-Tenancy

The ingestor supports multi-tenant operation out of the box.

**Tenant identification:** Set the `X-Tenant-ID` header on all requests. Falls back to `default` if absent.

**Rate limiting:** Each tenant has an independent token bucket. Default is `1000 req/sec`. Override per tenant in config.

**Kafka partitioning:** The tenant ID is used as the Kafka message key, ensuring all metrics from a tenant land on the same partition for locality.

**Disabling a tenant:**

```go
configs := map[string]TenantConfig{
    "suspended-tenant": {MaxRPS: 0, Enabled: false},
}
```

---

## Circuit Breaker

The Rust ClickHouse writer has a built-in circuit breaker protecting the pipeline from ClickHouse degradation:

```
State transitions:

  CLOSED ──(5 failures)──► OPEN ──(1s backoff elapsed)──► HALF_OPEN
    ▲                                                           │
    └──────────────(3 successes)────────────────────────────────┘
    
  HALF_OPEN ──(failure)──► OPEN (backoff doubles, max 60s)
```

When the circuit is OPEN, metric batches are dropped (not queued) to prevent memory exhaustion. The circuit probes ClickHouse every `base_backoff_ms` seconds with exponential growth up to `max_backoff_ms`.

---

## Kubernetes Deployment (Helm)

```bash
# Dry-run: render all templates
make helm-template

# Deploy to cluster
make helm-install

# Check rollout
kubectl rollout status deployment/ingestor -n observability
kubectl rollout status deployment/processor -n observability
```

The Helm chart deploys all 12 components with:
- **HPAs** (Horizontal Pod Autoscalers) on ingestor and processor
- **PDBs** (Pod Disruption Budgets) to maintain availability during updates
- **Resource requests and limits** tuned for production workloads
- **ConfigMaps** for Prometheus and Grafana provisioning

---

## AWS EKS Provisioning (Terraform)

```bash
# Plan infrastructure changes
make terraform-plan

# Provision (creates VPC, EKS cluster, 3 node groups, ElastiCache Redis)
make terraform-apply
```

The Terraform blueprint creates:
- VPC with public + private subnets across 3 AZs
- EKS cluster (Kubernetes 1.29) with managed node groups:
  - **General** (m5.xlarge × 2): Grafana, Prometheus, services
  - **Kafka** (r5.2xlarge × 3): Kafka brokers with local NVMe storage
  - **ClickHouse** (r5.4xlarge × 2): ClickHouse with high-memory config
- ElastiCache Redis (cache.r6g.large, 2-node cluster with replication)
- S3 bucket for Terraform remote state with DynamoDB locking

> **Before first run:** Create the S3 state bucket and DynamoDB lock table:
> ```bash
> aws s3 mb s3://observability-pipeline-tfstate --region us-east-1
> aws dynamodb create-table \
>   --table-name observability-pipeline-tflock \
>   --attribute-definitions AttributeName=LockID,AttributeType=S \
>   --key-schema AttributeName=LockID,KeyType=HASH \
>   --billing-mode PAY_PER_REQUEST
> ```

---

## Repository Structure

```
observability-pipeline/
├── ingestor/                         # Go HTTP + gRPC ingestor
│   ├── cmd/server/main.go            # Entrypoint
│   ├── internal/
│   │   ├── cache/redis.go            # Redis cache-aside layer
│   │   ├── grpc/server.go            # gRPC MetricService server
│   │   ├── handler/ingest.go         # HTTP /ingest and /batch handlers
│   │   ├── middleware/               # Rate limiting, tracing, tenant, logging
│   │   ├── model/metric.go           # Shared metric struct
│   │   ├── producer/kafka.go         # Kafka producer (franz-go)
│   │   └── validator/metric.go       # Schema validation
│   └── proto/v1/metric.proto         # Protobuf definitions
│
├── processor/                        # Rust Kafka consumer + anomaly detector
│   └── src/
│       ├── consumer.rs               # rdkafka consumer loop
│       ├── detector/
│       │   ├── ewma.rs               # EWMA anomaly detector
│       │   └── zscore.rs             # Rolling Z-score detector
│       └── storage/clickhouse.rs     # Batch writer + circuit breaker
│
├── sdk/go/                           # Go client SDK with auto-batching
│
├── infra/
│   ├── alertmanager/alertmanager.yml # Tiered routing: PagerDuty + Slack
│   ├── clickhouse/schema.sql         # MergeTree schema + materialized views
│   ├── grafana/dashboards/           # 4 pre-provisioned dashboards
│   ├── kafka-connect/                # ClickHouse sink connector + deploy script
│   ├── kafka/topics.sh               # Topic creation script
│   ├── otel-collector/config.yml     # OTLP fan-out to Tempo + Jaeger
│   ├── prometheus/alerts/            # pipeline.yml + slo.yml alert rules
│   ├── schema-registry/              # Avro schemas + registration script
│   └── tempo/tempo.yml               # Trace backend config
│
├── helm/observability-pipeline/      # Kubernetes Helm chart
├── terraform/                        # AWS EKS infrastructure
├── tests/
│   ├── integration/test_pipeline.sh  # 25 end-to-end tests
│   └── load/
│       ├── fire_metrics.go           # Go load generator
│       └── k6_script.js              # k6 sustained load test
│
├── .env.example                      # All environment variables documented
├── docker-compose.yml                # Full 14-container local stack
├── Makefile                          # Unified command interface
└── README.md
```

---

## Makefile Reference

```
make dev              Start full 14-container stack
make down             Stop all containers
make test             Run all Go + Rust unit tests
make lint             go vet + cargo clippy
make fmt              go fmt + cargo fmt
make fire             1000 metrics/sec for 60s
make load-test        k6 sustained load test
make logs             Follow all container logs
make logs-ingestor    Follow ingestor logs only
make logs-processor   Follow processor logs only
make status           Service health + Kafka topic state
make integration-test Run 25 end-to-end pipeline tests
make sdk-test         Run Go SDK unit tests
make topics           Recreate Kafka topics manually
make schema-register  Register Avro schemas in Schema Registry
make connect-deploy   Deploy Kafka Connect ClickHouse sink
make grpc-gen         Generate gRPC Go stubs from proto (needs protoc)
make helm-template    Render Helm chart templates (dry-run)
make helm-install     Deploy to Kubernetes
make helm-uninstall   Remove Helm release
make terraform-plan   Plan AWS EKS infrastructure
make terraform-apply  Provision AWS EKS
make bench            Rust processor benchmarks
make build            Build Go + Rust binaries
make clean            Stop containers, remove volumes + build artifacts
```

---

## Design Decisions

**Why Kafka over a direct HTTP chain?**
Kafka decouples producers from consumers. If the Rust processor crashes, metrics persist in the topic until recovery — no data loss. An HTTP chain drops everything in-flight on failure. Kafka also enables fan-out: multiple consumers can independently read the same topic (processor + audit logger + analytics).

**Why Rust for the processor?**
The borrow checker prevents data races at compile time — no runtime panics from concurrent access to the detector state. `tokio` gives millions of concurrent async tasks without OS threads. The result: sub-millisecond processing latency sustained under 100k metrics/sec.

**Why ClickHouse for long-term storage?**
Prometheus is designed for 15-day hot retention. ClickHouse's columnar MergeTree achieves 15–40x compression on time-series vs raw JSON. A query over 1 year of data completes in milliseconds vs minutes in PostgreSQL.

**Why a circuit breaker on ClickHouse writes?**
Under ClickHouse degradation, unlimited retries cause the processor to stall, backing up Kafka consumer lag into the millions. The circuit breaker short-circuits writes immediately when the backend is unhealthy, keeping the consumer loop running and lag bounded.

**Why per-tenant rate limiting?**
A single noisy tenant can saturate the ingestor and starve every other tenant. Independent token buckets (keyed by `X-Tenant-ID`) enforce isolation without coordination overhead or global locks.

**Why manual Kafka offset commits?**
`enable.auto.commit=false` with commit-after-write gives at-least-once semantics: if the ClickHouse write fails, the offset is not advanced and the message is reprocessed. Combined with ClickHouse's idempotent `ReplacingMergeTree`, this achieves effectively-once delivery.

---

## Known Limitations

| Limitation | Status | Path to Fix |
|---|---|---|
| Single Kafka broker in dev | By design — dev only | 3-broker StatefulSet in `k8s/kafka/` and Helm chart |
| No TLS/mTLS | Dev only | Add Kafka TLS config + gRPC TLS credentials in prod |
| gRPC uses JSON framing | Protoc-free fallback | Run `make grpc-gen` with protoc installed for binary gRPC |
| Schema Registry validation is config-only | MVP | Add `srclient` Go library + `schema_registry_converter` in Rust |
| Redis cache is not wired into HTTP handler by default | Explicit design | Inject `RedisCache` via dependency injection in `main.go` |
| Terraform state backend requires manual S3 setup | One-time | See AWS EKS section above for bootstrap commands |
| AlertManager credentials are empty by default | Security | Set `SLACK_WEBHOOK_URL` and `PAGERDUTY_ROUTING_KEY` in `.env` |

---

## Testing

### Unit Tests

```bash
make test
# Go:   41 tests across cache, gRPC, middleware, handler, validator, SDK
# Rust: 12 tests across EWMA, Z-score, and circuit breaker
```

### Integration Tests

```bash
make integration-test
# 25 end-to-end tests verifying the full Kafka → ClickHouse pipeline
```

### Load Test

```bash
make load-test
# k6 ramps to 100k metrics/sec and holds for 5 minutes
# Reports: p50/p95/p99 latency, error rate, throughput
```

### Benchmarks

```bash
make bench
# Rust processor throughput (cargo bench)
# Outputs: ns/msg at varying concurrency levels
```

---

## CI/CD

GitHub Actions runs on every push and pull request:

```
.github/workflows/ci.yml
├── go-test      — go test -race ./... + go vet
├── rust-test    — cargo test + cargo clippy
├── helm-lint    — helm lint + helm template
└── docker-build — builds ingestor + processor images
```

---

## Interview Talking Points

**Q: Why Kafka and not Redis Streams or RabbitMQ?**
Kafka is a distributed commit log — consumers can re-read from any offset, replay historical data, and scale horizontally by adding partitions. Redis Streams lose data without careful persistence config. RabbitMQ is point-to-point; adding a second consumer (e.g., audit logger) requires a new queue. Kafka handles all these cases natively.

**Q: How does Rust prevent bugs that Go/Python would miss?**
The borrow checker guarantees no data races at compile time. The `detector/ewma.rs` state is accessed from multiple tokio tasks — Rust forces synchronization to be explicit. Python or Go would require runtime mutexes and could silently race; Rust rejects the code at compile time if synchronization is missing.

**Q: How do you ensure no metric is processed twice?**
Kafka offsets are committed only after a successful ClickHouse write. On crash/restart, the processor re-reads uncommitted messages. ClickHouse's `ReplacingMergeTree` deduplicates rows with the same primary key, turning at-least-once delivery into effectively-once semantics — no transactions needed.

**Q: How does the circuit breaker improve reliability?**
Without it, ClickHouse degradation causes the processor's write goroutines to block indefinitely, stalling the consumer loop and letting Kafka lag grow unbounded. The circuit breaker trips after 5 failures, drops batches immediately (not queues them), and probes with exponential backoff. The consumer loop stays running, lag stays bounded, and recovery is automatic.

**Q: Why ClickHouse instead of TimescaleDB or InfluxDB?**
ClickHouse's columnar MergeTree achieves 15–40x compression on time-series vs JSON. Aggregation queries over billions of rows complete in milliseconds because only the columns needed are read from disk. TimescaleDB is PostgreSQL-based and I/O-bound on large scans; InfluxDB has licensing restrictions and lower throughput.

---

## License

MIT — Kevin Bastin
