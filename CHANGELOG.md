# Changelog

## Unreleased

### Fixed
- **The Kafka broker was OOM-killed under sustained load** in Compose: with no
  heap setting it ran a 1 GB heap inside a 1 GB container limit. The heap is
  pinned to 512 MB (`KAFKA_HEAP_OPTS`). The 60,000-metric restart test that
  failed now passes.
- Benchmark and restart scripts exited silently when Kafka could not be
  queried, and waited minutes on a stuck consumer. They now say why and stop.
- The restart test sent requests in bursts, so the sub-second ingestor restart
  gap was crossed only by chance and the retry path was never exercised.
  Requests are now evenly paced.
- **EWMA false-alarmed ~37 times per 1,000 points on pure noise at 3 sigma**
  (theory: ~3). The variance estimate shared the mean's fast smoothing, so it
  averaged about 6 samples. It now uses a slower smoothing of its own. On the
  evaluation series, EWMA precision on noise-plus-spikes rose from 0.15 to
  0.65 and on level shifts from 0.35 to 0.90, with recall unchanged.

### Added
- `processor/src/detector/seasonal.rs`: a per-time-of-day baseline, with an
  online measure of how seasonal a series is and an `auto` policy in the
  evaluation harness. Not wired into the processor. On real NYC-taxi data it
  finds 5 of 5 labelled events at threshold 4 where the shipped rule finds 1,
  at the cost of many more alerts; see `docs/DETECTOR_EVALUATION.md`.
- Real-data detector evaluation against the Numenta Anomaly Benchmark
  (`make detector-eval-real`, `bench/fetch_nab.sh`, data kept out of the repo)
  and an exploratory Python prototype (`bench/detector_prototype.py`). At the
  shipped threshold the detector is noisy on real series (precision about
  0.10, 36 false alarms per 1,000 points); results and the seasonality
  finding are in `docs/DETECTOR_EVALUATION.md`.
- `docs/DETECTOR_EVALUATION.md` and `processor/src/detector/eval.rs`: seeded,
  labelled synthetic series, event-based precision/recall, regression floors
  in CI, and a documented known limitation (no seasonality). `make detector-eval`.
- `bench/chaos/rolling_restart_no_loss.sh` (`make chaos-rolling`): graceful
  restart of the processor or ingestor under load, asserting zero loss.
- `bench/throughput_median.sh` (`make bench-median`): N-run throughput and
  latency benchmark reporting medians and the host, written to `bench/results/`.
- Load generator reports request latency p50/p95/p99.

## 2.1.0 — hardening pass

### Fixed (correctness)
- **Processor routed healthy rows to the DLQ on transient ClickHouse errors.**
  Every `DB::Exception` was treated as permanent, including `TOO_MANY_PARTS`,
  `MEMORY_LIMIT_EXCEEDED`, `UNKNOWN_TABLE` and `AUTHENTICATION_FAILED`. Errors
  are now classified by exception code against an allowlist of row-level
  data errors.
- **A hung ClickHouse connection could freeze the consumer.** Inserts and
  readiness checks now have deadlines.
- **Offsets could be committed for partitions this member no longer owned**
  after a rebalance. Revoked partitions' batches are released; commits are
  restricted to the current assignment.
- **The Z-score detector could not fire with small windows.** It scored values
  against a baseline that already contained them (max 2.85σ at n=10). It also
  lost precision on large values and ignored flat baselines.
- **Ingestor readiness outlived the drain delay** in the Helm chart (30s vs
  15s), so rolling restarts could send traffic to a pod that had stopped
  accepting.
- **Helm chart did not validate** (null Service port; Prometheus mounted a
  ConfigMap and PVC the chart never created; `clickhouseUrl` had a path, so
  every insert would 404).
- **Go SDK could not work against an authenticated ingestor** and dropped
  every gauge (unit `"gauge"` is rejected by the validator).
- `pipeline:anomaly_rate:ratio5m` recording rule was always empty.
- Prometheus scraped two targets that could never be up.

### Added
- `pipeline_end_to_end_lag_seconds`, last-commit timestamp, rebalance and
  timeout metrics; freshness and stalled-commit alerts with promtool tests;
  Grafana freshness panels; freshness runbook.
- Per-tenant metrics-per-second quota, `Retry-After` on 429, 413 body limit,
  per-item batch errors with a `retryable` flag.
- API key hot reload (content polling + SIGHUP), fail-safe on bad files.
- Tag key charset and control-character validation.
- Replay-stable `alert.id` header on alerts; idempotent alert producer.
- SDK: API key, retry/backoff honouring the 207 contract and Retry-After,
  bounded buffer, `Stats()`, `InstrumentRoute`.
- Helm: ServiceMonitor, PrometheusRule, ServiceAccount, render-time guards.
- CI: SDK tests, rule tests, kubeconform validation, guard tests, pinned
  scanners, least-privilege permissions.

### Removed
- Unwired code and infrastructure: TCP/JSON "gRPC" server, Redis cache and
  its Terraform ElastiCache cluster, proto, Kafka Connect, Schema Registry,
  stale `k8s/` manifests, Bitnami subcharts, the in-chart monitoring stack.

### Changed (compatibility)
- Go module path is now `github.com/KevinKattakayam/datadog/...`.
- `processor_anomalies_detected_total` labels are `detector, severity`
  (was `metric, severity`; metric names are client-controlled).
- A batch containing invalid items now returns `207` (was `202` with a
  `skipped` count).
- Both container images run as UID 65532 (was root under Compose), matching
  the `runAsUser` the Helm chart already enforces. The ingestor runtime base
  moves from `alpine:3.19` (end of life) to `alpine:3.22`.

### Build
- librdkafka 2.12 includes `curl/curl.h` even with curl disabled. CI and the
  processor build image now install `libcurl4-openssl-dev` explicitly instead
  of relying on it being preinstalled.
- `docs/DEVELOPMENT.md` lists the real toolchain minimums (Go 1.25, Rust 1.86)
  and the native packages needed to build the processor outside Docker.
