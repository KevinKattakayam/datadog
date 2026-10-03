# Roadmap status

Each item from the four-week roadmap is mapped to the evidence that it is
done, or marked open. "Verified locally" means unit tests, linters, promtool
or kubeconform ran green. "Needs a stack run" means it depends on Docker or a
cluster and has not been re-run since this pass.

## Week 1 — Correctness

| Item | Status | Evidence |
|---|---|---|
| CI targets `master` | Done | `.github/workflows/ci.yml` |
| Commit only after durable write | Done | `processor/src/consumer.rs` `flush_all` |
| Writer retries, never clears on error | Done | `write_metrics_returns_error_after_configured_attempts_and_keeps_rows` |
| Circuit open pauses instead of dropping | Done | `consumer.rs` pause/resume; `bench/results/phase1-six-minute-outage-after.txt` |
| SIGTERM drain | Done | `processor/src/main.rs`, `consumer.rs` shutdown arm |
| Ingestor `ProduceSync`, 503 when Kafka is down | Done | `TestSingle_KafkaDownIs503WithRetryAfter` (real handler) |
| 207 partial + skipped count | Done | `TestBatch_InvalidItemsAreSkippedWithIndices` |
| Shutdown ordering + draining | Done; chart timing fixed | `cmd/server/main.go`; `obs.validate` in `_helpers.tpl` |
| DLQ with reason headers | Done | `processor/src/dlq.rs` |
| `kill -9` zero loss proof | Done | `bench/chaos/kill9_no_loss.sh`, `bench/results/` — needs a stack re-run after this pass |
| Integration tests assert ClickHouse rows | Done | `tests/integration/test_pipeline.sh` |
| Nightly chaos in CI | Done | `integration-chaos` job |

## Week 2 — Tenancy, security, detector

| Item | Status | Evidence |
|---|---|---|
| Hashed API keys, 401 without key | Done; now hot-reloadable | `auth_test.go`, `auth_reload_test.go` |
| Per-tenant limits, race fixed | Done; now also per-metric quota | `TestTenantRateLimit_ConcurrentRace`, `TestQuota_*` |
| Tenant A cannot throttle tenant B | Done | `TestQuota_TenantsAreIsolated` |
| `tenant_id` in schema, sort key, Kafka key | Done | `infra/clickhouse/schema.sql`, `producer/kafka.go` |
| Tag/host length limits | Done; plus charset/control chars | `validator/metric_test.go` |
| Bounded detector registry | Done | `processor/src/detector/registry.rs` |
| No secrets in values | Done | `helm/.../values.yaml` |
| securityContext | Done | chart; kubeconform-validated |
| Least-privilege ClickHouse users | Partial | Processor/Grafana users exist; Compose `default` user is still passwordless with access management on |
| Prometheus admin API off | Done (Compose and chart) | in-chart Prometheus removed |
| govulncheck, cargo audit, Trivy, SBOM | Done; scanners pinned | `ci.yml` |
| EWMA flat-baseline fix | Done; Z-score fixed too | `flat_baseline_then_step_is_detected` (both detectors) |
| KEDA, max ≤ partitions | Done; enforced at render time | `obs.validate` |
| Processor readiness reflects ClickHouse | Done; bounded by timeout | `health_check` |
| Topology spread, grace periods, helm lint in CI | Done | chart + `helm` CI job |

## Week 3 — Feature and benchmarks

| Item | Status | Evidence |
|---|---|---|
| Differentiating feature | Cardinality governor (report-only) | `docs/CARDINALITY_GOVERNOR.md` |
| End-to-end freshness metric on dashboard | Done | `pipeline_end_to_end_lag_seconds`, Pipeline Overview "Freshness and Durability" |
| k6 throughput, 3 runs, median | **Runner ready, not yet run** | `bench/throughput_median.sh` (`make bench-median`); load tool now reports p50/p95/p99. Mechanics tested against stand-ins only; no real figures exist yet |
| All chaos scenarios tabulated | **Partly done** | kill -9 and replay recorded in `bench/results/`; `bench/chaos/rolling_restart_no_loss.sh` (`make chaos-rolling`) is written but has not been run against the real stack |

## Week 4 — Presentation

| Item | Status | Evidence |
|---|---|---|
| Honest README | Done | `README.md` |
| Architecture, failure modes | Done | `docs/ARCHITECTURE.md`, `docs/FAILURE_MODES.md` |
| Every `runbook_url` resolves | Done | `docs/runbooks/` (now includes `freshness.md`) |
| Schema Registry: wire or delete | Deleted | see CHANGELOG "Removed" |
| Demo recording | **Open** | — |

## Still open, in priority order

1. Re-run `make integration-test`, `make chaos` and a rolling restart under
   load against this version; commit the outputs to `bench/results/`.
2. Throughput benchmark (three runs, median, with `env.txt`).
3. Compose ClickHouse `default` user: set a password, drop
   `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT`, and update the scripts that query
   ClickHouse with `curl` to authenticate.
4. TLS and SASL for Kafka and ClickHouse in the chart.
5. Detector precision/recall on real, production-shaped data. Synthetic series are done: see `docs/DETECTOR_EVALUATION.md` (`make detector-eval`).
