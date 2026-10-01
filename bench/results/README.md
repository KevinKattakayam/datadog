# Validation evidence

Captured on 2026-10-01 on the host documented in [env.txt](env.txt). These are
single-run local development smoke checks, not a performance benchmark or
three-run median. The host had only about 3.9 GiB available RAM and 1.9 GiB of
swap in use at capture time.

| Check | Command / setup | Observed result |
| --- | --- | --- |
| Go vet and race suite | `cd ingestor && GOCACHE=/tmp/obs-go-build-cache go vet ./... && GOCACHE=/tmp/obs-go-build-cache go test -race ./...` | Passed |
| Rust formatting, Clippy, unit tests | `cd processor && cargo fmt -- --check && cargo clippy --locked -- -D warnings && cargo test --locked` | Passed; 15 unit tests |
| End-to-end integration | `make integration-test` | Passed; 33 assertions, including 51 run-specific ClickHouse rows and Alertmanager health |
| SIGKILL recovery | `SENT=100 RATE=50 KILL_AFTER_SECS=1 make chaos-kill9` | 100 sent, 100 unique, 100 rows, 0 duplicates; lag recovery 30s |
| ClickHouse pause recovery | `WARMUP_SECONDS=2 OUTAGE_DURATION=5 COOLDOWN_SECONDS=2 RATE=50 make chaos-clickhouse` | 450 accepted, 450 unique rows, 0 rejected batches; peak lag 384, recovery 53s |
| Combined default chaos target | `make chaos` | SIGKILL: 500/500 unique, 0 duplicates, 65s recovery. ClickHouse pause: 950 accepted/unique, 0 rejected batches, peak lag 759, 106s recovery. |

Raw outputs are [kill9-smoke.txt](kill9-smoke.txt),
[clickhouse-outage-smoke.txt](clickhouse-outage-smoke.txt), and
[chaos-default.txt](chaos-default.txt). A short test size
was used because the local processor drains this host's backlog slowly. The
100,000-record stress run and a 120-second outage were not run.
No pre-fix loss number was recorded, and no claim is made about how many records
the earlier implementation would lose.

Not measured here: sustained throughput, ingest latency percentiles,
end-to-end freshness, three-run medians/spread, rolling restarts under load,
or detector precision/recall on labelled data. The smoke scenarios do not
establish a production SLO.
