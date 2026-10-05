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

## Ingest-path throughput (2026-10-04, ingest path only)

`RATE=<n> make bench-median`: 60-second runs, three per rate, batches of 100
metrics. The ingestor answers 202 only after the Kafka write is acknowledged,
and the load generator counts a request as a success only if every metric in it
was acknowledged. So these figures describe the **ingest path** (HTTP to Kafka),
not data reaching ClickHouse.

| Requested | Achieved (median) | p50 | p95 | p99 | Errors | Report |
|---|---|---|---|---|---|---|
| 2,000/s | 2,000/s | 4.4 ms | 5.3 ms | 7.3 ms | 0 | [throughput-20261004-011909.txt](throughput-20261004-011909.txt) |
| 5,000/s | 5,000/s | 3.9 ms | 4.8 ms | 5.8 ms | 0 | [throughput-20261004-012732.txt](throughput-20261004-012732.txt) |
| 8,000/s | 7,999/s | 4.0 ms | 4.9 ms | 6.7 ms | 0 | [throughput-20261004-014010.txt](throughput-20261004-014010.txt) |

Later runs at 12,000, 20,000 and 30,000/s also reported 0 errors. The ingestor's
own counters afterwards showed 14,757,800 messages published to Kafka and no
publish errors. Those reports are not recorded here. Host: 8-core i5-1135G7,
7.4 GiB RAM, with the whole stack and the load generator on the same machine.
The ingest-path ceiling has not been found.

**Correction.** An earlier version of this page, and the repository README,
said the processor "kept up" at these rates and showed a `Drain` column of 0 s.
That was wrong. The `drain_s` column in the three reports above is **invalid**:
the script summed Kafka's LAG column, and Kafka prints `-` there for a
partition that has never had an offset committed, which was counted as zero lag.
Nothing in these reports shows that any of this data reached ClickHouse.

## Processor stall under bulk load (found and diagnosed 2026-10-04)

After the 12,000 to 30,000/s runs:

- Kafka `metrics.raw` held about 19.8 million messages in total, and five of
  its six partitions had **no committed offset** (about 19.5 million messages
  unconsumed).
- The processor had consumed about 1,000 messages since it started,
  `processor_rows_committed_total` was 0, and `processor_circuit_breaker_state`
  was 1 (open).
- ClickHouse held 326,254 rows in total, all from the correctness tests, none
  from the load runs. The dead-letter queue was empty.
- The ingestor side was healthy: 14,757,800 messages published, 0 errors.

**Cause.** The processor logged `circuit open after 5 attempts`, and ClickHouse's
error table held 90 `NO_SUCH_COLUMN_IN_TABLE` errors: `No such column tenant_id
in table observability.alerts`. The Docker volume was created before `tenant_id`
existed, and the bootstrap upgrade script added that column to `metrics` but not
to `alerts` (`CREATE TABLE IF NOT EXISTS` never repairs an old table). The load
generator injects spikes, which fire alerts, so the first batch containing one
could never be written or committed. The processor treats a missing column as
retryable on purpose, so it paused and Kafka buffered everything. The chaos tests
above never produced an alert, which is why they passed. A `CircuitBreakerOpen`
alert exists in the Prometheus rules; nobody was looking at Alertmanager.

Fixed in `infra/clickhouse/bootstrap.sh`, and `make integration-test` now checks
that every column the processor writes exists in `metrics` and `alerts`.
Recovery is **confirmed**: after adding the column to the running volume the circuit
breaker closed within about a minute, `rows_committed_total` climbed, alerts were
flushed, and the processor drained the roughly 19.5 million message backlog at
about 3,775 rows per second (seven consecutive 10-second samples, 3,670 to 3,911).

The kill -9, replay and restart results above are unaffected: each verified rows
in ClickHouse directly at about 1,000 metrics/s, with no alerts involved. The
end-to-end figures measured after the fix are in the next section.

The scripts are fixed: `kafka_lag` counts an uncommitted partition as fully
unconsumed, and `make bench-median` now waits until every acknowledged metric
is queryable in ClickHouse and reports `landed%`. It aborts, naming the lag and
the container states, if progress stops.

## End-to-end throughput, rows verified in ClickHouse (2026-10-04)

`RATE=<n> make bench-median` on a fresh stack (`docker compose down -v`, then
`make dev`), commit `829a1ac`. After each 60-second run the script waits until
every metric the ingestor acknowledged is queryable in ClickHouse. `landed%` is
the share found; `drain` is how long after the load stopped until all of them
were. Medians of three runs.

| Requested | Ingest achieved | p50 | p95 | p99 | Errors | landed% | Drain (median; runs) | Report |
|---|---|---|---|---|---|---|---|---|
| 2,000/s | 2,000/s | 4.4 ms | 6.2 ms | 8.1 ms | 0 | 100 | 2 s (2, 2, 2) | [throughput-20261004-064703.txt](throughput-20261004-064703.txt) |
| 4,000/s | 4,000/s | 4.3 ms | 6.0 ms | 7.7 ms | 0 | 100 | 8 s (8, 8, 10) | [throughput-20261004-065058.txt](throughput-20261004-065058.txt) |
| 6,000/s | 6,000/s | 3.9 ms | 5.7 ms | 7.6 ms | 0 | 100 | 54 s (54, 42, 54) | [throughput-20261004-065514.txt](throughput-20261004-065514.txt) |

Nothing was lost at any rate. Up to about 4,000 metrics/s the processor caught up
within seconds of the load ending. At 6,000/s it fell behind: a 60-second run
left a backlog that took 42 to 54 seconds to clear.

**Estimated sustainable rate: roughly 3,200 to 3,500 rows/s on this machine.**
This is an inference from the drain times (for example, at 6,000/s a backlog of
about 360,000 messages cleared in 54 s implies about 3,150/s), not a direct
measurement. It agrees with the 3,775 rows/s observed while draining the old
backlog. Only 2,000/s is clearly sustainable from these runs; whether 3,000 to
3,500/s holds over a longer run has not been tested. The ingest path, by
contrast, accepted 30,000 metrics/s with no errors, so the processor is the
limit, not the ingestor. The first run of each rate showed a latency spike
(p99 about 370 ms), which looks like warm-up.

Where the limit comes from is only partly understood. The processor waits for
Kafka to confirm each alert before it handles the next message (a 20 ms producer
batching window), and the load's alerts appeared about 21 ms apart in the log.
Raising the anomaly threshold from 3.0 to 8.0, which fires far fewer alerts,
raised the drain rate from about 3,775 to roughly 4,600 to 5,500 rows/s (one
short, noisy sample, taken while ClickHouse was near its memory limit). Alert
publishing is therefore a cost but not the only one.

**ClickHouse ran out of memory.** While draining the roughly 19.5 million
message backlog it reached 997 MiB of its 1 GiB limit and the kernel killed it
(06:43:30). It survived the 2,000 to 6,000/s runs above. The limit has not been
raised or tuned; treat 1 GiB as too small for sustained heavy inserts.

All figures come from one 8-core laptop that also ran the load generator, with
the whole stack on the same machine and little free memory.

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

## Follow-up: 3,000 and 3,500 metrics/sec

Same host and method as above (8-core i5-1135G7 laptop, 7.4 GiB RAM, load generator on the same machine,
three 60-second runs per report). Each rate was run twice: once on `829a1ac` and once on `2ac3040`
(which adds the `alerts.tenant_id` upgrade fix). All four reports are kept.

| Rate | Report | Commit | Median drain | Drain per run | Ingest p99 per run |
| --- | --- | --- | --- | --- | --- |
| 3,000/s | `throughput-20261004-070302.txt` | `829a1ac` | 2 s | 2, 2, 2 s | 8.1, 7.9, 7.8 ms |
| 3,500/s | `throughput-20261004-070655.txt` | `829a1ac` | 14 s | 20, 14, 4 s | 110.7, 36.8, 7.5 ms |
| 3,000/s | `throughput-20261004-072323.txt` | `2ac3040` | 2 s | 2, 2, 2 s | 8.6, 7.6, 7.4 ms |
| 3,500/s | `throughput-20261004-072716.txt` | `2ac3040` | 2 s | 2, 14, 2 s | 7.9, 427.2, 7.5 ms |

Every acknowledged metric reached ClickHouse in all 12 runs, with 0 errors. 3,000/s was steady. At 3,500/s
the processor sometimes fell 14 to 20 seconds behind and ingest p99 occasionally spiked, so that rate is at
the edge on this machine. The laptop had only 240 to 420 MiB of free RAM during these runs; memory pressure
is a possible cause of the variance but was not tested. `journalctl -k | grep -i 'killed process'` returned
nothing after these runs. Runs longer than 60 seconds were not done.
