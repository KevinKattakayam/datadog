# Validation evidence

Captured on 2026-10-01 and 2026-10-02 on the host documented in [env.txt](env.txt). These are
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
| Combined default chaos target | `make chaos` | SIGKILL: 500/500 unique, 0 duplicates, 45s recovery. ClickHouse pause: 950 accepted/unique, 0 rejected batches, peak lag 761, 108s recovery. |

Raw outputs are [kill9-smoke.txt](kill9-smoke.txt),
[clickhouse-outage-smoke.txt](clickhouse-outage-smoke.txt), and
[chaos-default.txt](chaos-default.txt). A short test size
was used because the local processor drains this host's backlog slowly. The
100,000-record stress run and a 120-second outage were not run.
No pre-fix loss number was recorded, and no claim is made about how many records
the earlier implementation would lose.

Not measured here: end-to-end freshness percentiles, the throughput ceiling,
multi-replica behaviour, long soak runs, and detector quality on real data.
The 2026-10-04 sections below add three-run medians and restart-under-load
results. None of this establishes a production SLO.

## Mid-stream SIGKILL (2026-10-04)

`SENT=20000 RATE=2000 KILL_AFTER_SECS=3 make chaos-kill9` ([raw output](kill9-midstream-20k.txt)).
The processor was killed while 17,300 of 20,000 records were still unconsumed.
After restart: 20,000 sent, 20,000 unique, 20,000 rows, 0 duplicates, 10s
recovery. Single run on the host in [env.txt](env.txt); not a repeated-run
median. This run did not hit the write-before-commit window, so it does not
exercise duplicate replay; see `make chaos-replay` for that.

## Replay deduplication (2026-10-04)

`make chaos-replay` ([raw output](replay-dedup-100.txt)). The processor is
crashed on purpose after the ClickHouse write and before the Kafka commit.
Result: 100 sent, 200 physical rows (every row replayed once), 100 rows under
`FINAL`, 0 offset gaps. Duplicates stay in the raw table until a background
merge, so queries must use `FINAL` or `GROUP BY`. Single deterministic run
with a failpoint, not a random crash.

## Graceful restart under load (2026-10-04)

`make chaos-rolling`: numbered metrics are sent at a steady rate and one
container is restarted with SIGTERM (`docker restart`) part-way through. The
script then waits for the consumer to drain and checks every metric landed.

| Target | Sent | Unique rows | Duplicates | Client retries | Restart took | Raw output |
|---|---|---|---|---|---|---|
| processor | 20,000 at 1,000/s | 20,000 | 0 | 0 | 1 s | [rolling-processor.txt](rolling-processor.txt) |
| ingestor | 60,000 at 1,000/s | 60,000 | 0 | 1 | 16 s | [rolling-ingestor.txt](rolling-ingestor.txt) |

Probing the ingestor during a restart showed: `/ready` returns 503 immediately
(so a load balancer stops sending traffic); `/ingest` kept answering 202
through the roughly 15 s drain; then two connection-refused probes about 0.2 s
apart while the replacement container started. The one client retry in the
60,000-metric run is that sub-second gap. A retrying client lost nothing.

Limits: Compose runs one ingestor replica and no load balancer, so this does
**not** show zero client-visible errors. That needs two or more replicas behind
a Service on Kubernetes and has not been tested. An earlier 20,000-metric
ingestor run reported 0 retries because the old load loop sent in bursts and
rarely had a request in flight during the gap; the loop is now evenly paced.

## Sustained ingest throughput (2026-10-04)

`RATE=<n> make bench-median`: 60-second runs, three per rate, batches of 100
metrics, one request in flight per tick. Latency is per ingest request (HTTP
202, which includes the Kafka acknowledgement). `drain_s` is how long the
processor needed to clear the Kafka backlog after the run; 0 means it kept up.

| Requested | Achieved (median) | p50 | p95 | p99 | Errors | Drain | Report |
|---|---|---|---|---|---|---|---|
| 2,000/s | 2,000/s | 4.4 ms | 5.3 ms | 7.3 ms | 0 | 0 s | [throughput-20261004-011909.txt](throughput-20261004-011909.txt) |
| 5,000/s | 5,000/s | 3.9 ms | 4.8 ms | 5.8 ms | 0 | 0 s | [throughput-20261004-012732.txt](throughput-20261004-012732.txt) |
| 8,000/s | 7,999/s | 4.0 ms | 4.9 ms | 6.7 ms | 0 | 0 s | [throughput-20261004-014010.txt](throughput-20261004-014010.txt) |

Host: 8-core i5-1135G7, 7.4 GiB RAM, with the whole stack **and** the load
generator on the same machine and memory tight (swap in use). Latency did not
change as the rate quadrupled, so these runs have not found the ceiling; they
show the stack kept up at 8,000 metrics/s on this hardware, not that 8,000 is
its limit. An attempted 20,000/s run is deliberately not recorded: Kafka had
been killed for memory (below), so it measured a broken stack.

## Kafka killed for memory under load (2026-10-04)

Two sustained-load runs failed because the Kafka broker died. The kernel log
recorded two memory-cgroup kills of the `java` process (anon RSS about 1 GB).
`docker top obs-kafka` showed the broker running `-Xms1G -Xmx1G` inside a 1 GB
container limit, leaving no room for non-heap memory. With the consumer unable
to read, lag stayed frozen at the full backlog.

Fix: `KAFKA_HEAP_OPTS: "-Xms512M -Xmx512M"` in `docker-compose.yml`. After the
change the same 60,000-metric restart test that failed passed, and no further
kills were logged. The benchmark and chaos scripts now also stop with a clear
message when Kafka cannot be queried or the consumer lag is stuck, instead of
exiting silently.
