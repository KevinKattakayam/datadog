# Development Guide

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Docker | 24+ | Container runtime |
| Docker Compose | v2.20+ | Multi-container orchestration |
| Go | 1.22+ | Build/test ingestor |
| Rust | stable | Build/test processor |
| Helm | 3.14+ | Kubernetes deployment |
| k6 | latest | Load testing (optional) |

Compose uses `infra/alertmanager/alertmanager.local.yml`, which intentionally
discards notifications. The production routing example is a template; replace
its empty Slack/PagerDuty settings using secret-backed deployment config before
loading it. Local alert rules still appear in Prometheus.

The Compose-only processor uses the `processor-group-local` consumer group and
`KAFKA_AUTO_OFFSET_RESET=latest`. On a reused Kafka volume, this keeps an old
development backlog from making smoke tests wait for unrelated records. New
records published after the processor starts are consumed normally. The
processor defaults to `earliest` when deployed outside Compose; retain that
setting for a new production consumer group so existing Kafka records are
replayed. Do not use the local Compose offset policy for production recovery.

## Quick Start

```bash
# Clone and start the full stack
git clone https://github.com/KevinKattakayam/datadog
cd datadog
cp .env.example .env     # if available; defaults work without it
make dev

# Verify everything is healthy
make status

# Send a test metric
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -d '{"name":"test.cpu","value":42.5,"unit":"percent","timestamp":'$(date +%s)',"host":"dev-laptop"}'

# Check it arrived in ClickHouse (after processor flushes)
curl "http://localhost:8123/?query=SELECT+*+FROM+observability.metrics+ORDER+BY+ts+DESC+LIMIT+5+FORMAT+Pretty"
```

## Project Layout

```
├── ingestor/             Go HTTP ingestor
│   ├── cmd/server/       Entrypoint
│   ├── config/           Env-var configuration
│   └── internal/
│       ├── handler/      HTTP handlers
│       ├── middleware/    Auth, rate limit, tracing, metrics
│       ├── model/        Data types
│       ├── producer/     Kafka producer
│       └── validator/    Input validation
├── processor/            Rust stream processor
│   └── src/
│       ├── consumer.rs   Consumer loop (core)
│       ├── storage/      ClickHouse writer + circuit breaker
│       ├── detector/     EWMA, Z-score, registry
│       ├── dlq.rs        Dead-letter queue
│       ├── producer.rs   Alert fan-out
│       ├── model.rs      Data types
│       ├── config.rs     Env-var configuration
│       └── metrics.rs    Prometheus metrics
├── helm/                 Kubernetes Helm chart
├── infra/                Infrastructure configs
│   ├── clickhouse/       Schema + init
│   ├── prometheus/       Scrape config, alerts, recording rules
│   ├── grafana/          Dashboards + datasource provisioning
│   ├── alertmanager/     Routing config
│   ├── otel-collector/   OTLP fan-out config
│   ├── tempo/            Trace backend config
│   └── kafka/            Topic creation scripts
├── tests/
│   ├── integration/      End-to-end pipeline test
│   └── load/             k6 + Go load generators
├── bench/
│   └── chaos/            Failure mode verification scripts
├── docs/                 Architecture, failure modes, cardinality design, runbooks
├── docker-compose.yml    Local dev stack
└── Makefile              Top-level commands
```

## Make Targets

```bash
make help                 # Show all targets
make dev                  # Start everything
make down                 # Stop everything
make test                 # Run all unit tests (Go + Rust)
make lint                 # Run linters
make integration-test     # Run end-to-end test
make chaos                # Run all chaos tests
make chaos-kill9          # Kill -9 processor, assert zero loss
make chaos-clickhouse     # Pause ClickHouse, assert recovery and zero loss
make bench-throughput     # 5-min sustained throughput test
make status               # Show service status + lag
make logs                 # Follow all container logs
make clean                # Stop + remove all volumes
```

## Running Tests

### Unit tests
```bash
# Go (with race detector)
cd ingestor && go test -v -race ./...

# Rust
cd processor && cargo test
```

### Integration test
Requires the full stack running (`make dev`):
```bash
make integration-test
```

### Chaos tests
Requires the full stack running (`make dev`):
```bash
# Prove zero data loss under kill -9
make chaos-kill9

# Prove zero data loss during ClickHouse outage
make chaos-clickhouse
```

## Configuration

All configuration is via environment variables. See:
- [ingestor/config/config.go](../ingestor/config/config.go) for ingestor config
- [processor/src/config.rs](../processor/src/config.rs) for processor config

Key variables:

| Variable | Default | Purpose |
|----------|---------|---------|
| `KAFKA_BOOTSTRAP_SERVERS` | localhost:9092 | Kafka brokers |
| `KAFKA_AUTO_OFFSET_RESET` | earliest | Start position for a new consumer group; Compose overrides to `latest` |
| `KAFKA_CONSUMER_GROUP` | processor-group | Processor group; Compose uses `processor-group-local` |
| `PROCESSOR_BATCH_SIZE` | 1000 | Rows per ClickHouse insert |
| `PROCESSOR_FLUSH_INTERVAL_MS` | 500 | Max batch wait time |
| `PROCESSOR_DETECTOR_CAPACITY` | 100000 | Max tracked metric series |
| `INGESTOR_RATE_LIMIT_RPS` | 10000 | Per-tenant rate limit |
| `INGESTOR_DRAIN_DELAY_SECONDS` | 15 | Pre-shutdown drain period |

## Debugging

### View consumer lag
```bash
docker exec obs-kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group processor-group-local
```

### Query ClickHouse
```bash
curl "http://localhost:8123/?query=SELECT+count()+FROM+observability.metrics"
```

### View processor circuit state
```bash
curl http://localhost:9091/ready
```

### Inspect DLQ
```bash
docker exec obs-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic metrics.dlq --from-beginning --max-messages 5 \
  --property print.headers=true
```

## Contributing

1. All code must pass `make lint` and `make test`.
2. CI runs on `master` — ensure your branch is up to date.
3. Chaos tests should pass before merging data-path changes.
4. Add tests for new code — the CI enforces this.
