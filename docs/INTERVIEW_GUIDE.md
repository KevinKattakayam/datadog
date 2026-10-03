# Interview and demo notes

These answers describe the code in this repository. They are not claims of
production certification or a multi-node benchmark.

## Resume bullet drafts

- Built a Go-to-Kafka ingestion service and Rust processor that writes batches
  to ClickHouse before synchronously committing source offsets; exercised
  recovery with the checked-in SIGKILL and ClickHouse outage scripts.
- Added tenant API-key authentication, per-tenant ingestion rate limits,
  bounded detector state, a poison-message DLQ, and least-privilege ClickHouse
  users for the local stack and Helm deployment.
- Added a report-only cardinality dashboard that attributes host and tag
  combinations by tenant and metric, with explicit limits on its byte estimate.

Add measured scale or reliability numbers only with a repeated benchmark and
its environment/output in `bench/results/`.

## Ten likely questions

1. **What happens if the processor is SIGKILLed mid-batch?** The consumer
   persists the partition batches to ClickHouse and only then synchronously
   commits the highest covered offsets. If it dies before commit, Kafka replays
   the messages. The raw `ReplacingMergeTree` can collapse replayed rows by
   Kafka coordinates when queried with `FINAL`; materialized-view rollups may
   count a replay twice.
2. **Why not exactly-once?** Kafka and ClickHouse do not share a transaction.
   The implementation chooses at-least-once delivery and deterministic raw
   row identity, and makes the rollup replay limitation explicit.
3. **Does the circuit breaker drop data?** No. Failed writes leave offsets
   uncommitted and pause further receives while the buffered batch is retried.
   Kafka is the backlog. Readiness becomes false when ClickHouse or the
   breaker is unhealthy.
4. **What happens if the deployment scales beyond six processor replicas?**
   The raw topic has six partitions, so at most six consumers can do useful
   work for that topic. Helm caps optional KEDA scaling to the configured
   partition limit.
5. **How is one tenant isolated from another?** Ingestion API-key hashes map
   credentials to a tenant, and tenant identity is copied into the Kafka key,
   headers, and ClickHouse rows. The current schema does not create ClickHouse
   row policies; query access must be scoped by a trusted application or a
   separately configured database policy.
6. **Why ClickHouse?** It is the analytical sink for high-volume metric
   history and rollups. Prometheus provides operational scraping and alerting;
   it is not the long-retention multi-tenant event store here.
7. **How do you evolve the event schema?** JSON records carry a schema-version
   header and are decoded by the processor. The unused Schema Registry/Avro
   drafts, a TCP server mislabelled as gRPC, and an unwired Redis cache were
   deleted rather than left to imply capabilities the runtime lacks. A future incompatible payload needs an explicit versioned
   migration and compatibility policy.
8. **What does deployment do to in-flight data?** The processor handles
   shutdown by attempting to flush before exit and leaves offsets uncommitted
   on failure, allowing replay. The ingestor's readiness/drain path stops
   accepting new work before shutdown. Recheck the Kubernetes rollout under a
   load test before claiming zero 5xx or zero loss during rolling restart.
9. **What does the anomaly detector miss?** EWMA and rolling Z-score do not
   model daily/weekly seasonality, and all detector state is in process. The
   bounded registry limits memory but eviction and restarts lose baselines.
   Precision/recall have not been evaluated against a labelled dataset.
10. **What is the largest production gap?** The deployment does not configure
    service-to-service TLS, Kafka SASL/ACLs, or ClickHouse row policies. The
    Compose environment is explicitly development-only; the Helm chart expects
    trusted networking and externally managed credentials.

## Five-minute recording outline

1. Show the data path and state the delivery boundary: ClickHouse write, then
   Kafka offset commit.
2. Start `make dev`; show health/readiness and send a uniquely named metric.
3. Run `make integration-test` and show its ClickHouse row assertions.
4. Run `SENT=100 RATE=50 KILL_AFTER_SECS=1 make chaos-kill9`; show sent,
   unique, rows, duplicates, and lag recovery.
5. Run `WARMUP_SECONDS=2 OUTAGE_DURATION=5 COOLDOWN_SECONDS=2 RATE=50 make
   chaos-clickhouse`; show accepted count, recovered rows, and recovery time.
6. End with the limits above. Do not quote the short local smoke runs as a
   production throughput benchmark.
