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
| Throughput, 3 runs, median | **Done for this laptop** | `bench/results/throughput-20261004-0647*`, `-0650*`, `-0655*`: every acknowledged metric reached ClickHouse at 2,000, 4,000 and 6,000/s; drain after a 60 s run 2 s, 8 s and 54 s. Estimated sustainable end-to-end rate about 3,200 to 3,500 rows/s; the processor is the limit. Not tuned |
| All chaos scenarios tabulated | **Done for the single-node stack** | kill -9, replay dedup, graceful processor and ingestor restart under load, in `bench/results/`. A multi-replica rolling update needs Kubernetes and is untested |

## Week 4 — Presentation

| Item | Status | Evidence |
|---|---|---|
| Honest README | Done | `README.md` |
| Architecture, failure modes | Done | `docs/ARCHITECTURE.md`, `docs/FAILURE_MODES.md` |
| Every `runbook_url` resolves | Done | `docs/runbooks/` (now includes `freshness.md`) |
| Schema Registry: wire or delete | Deleted | see CHANGELOG "Removed" |
| Demo recording | **Open** | — |

## Still open, in priority order

1. Raise the end-to-end ceiling: publish alerts without waiting on each one, give ClickHouse more memory (it was killed at 1 GiB while draining a backlog), then re-run `make bench-median` at 3,000 to 8,000/s to find the new limit.
2. Throughput benchmark (three runs, median, with `env.txt`).
3. Compose ClickHouse `default` user: set a password, drop
   `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT`, and update the scripts that query
   ClickHouse with `curl` to authenticate.
4. TLS and SASL for Kafka and ClickHouse in the chart.
5. Detector quality: measured on synthetic series and on real labelled data (NAB), see `docs/DETECTOR_EVALUATION.md`. On real data the shipped threshold is noisy (precision about 0.10). A daily seasonal baseline exists (`seasonal.rs`) and is measured, but is not wired into the processor: it needs an opt-in configuration, incident grouping, and a better-grounded strength cut-off.
