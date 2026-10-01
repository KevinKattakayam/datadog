# Cardinality and Cost Governor

## Design

The first release is report-only. It attributes approximate distinct hosts and tag combinations to `(tenant, metric, hour)`, reports active detector-cache utilization and evictions, and estimates stored payload bytes from metric names, hosts, tags, and scalar values. Operators can identify tenants and metrics creating cardinality pressure before defining a hard quota.

The byte figure is an uncompressed payload estimate, not ClickHouse `bytes_on_disk` and not a billing value. Compression, part metadata, indexes, and shared parts prevent accurate tenant-level disk attribution with the current table layout. The ClickHouse system tables can report total table storage separately, but cannot honestly apportion it to tenants.

No metrics are rejected by this feature. Detector state remains bounded by the configured LRU capacity; eviction drops a detector baseline and is surfaced as a counter. A future enforcement mode needs an explicit series identity, tenant budget source, eviction semantics, and a client-visible rejection/DLQ contract. Enabling rejection before those are designed could discard valid customer telemetry.

## Limitations

- Detector cache series are in-process and reset on restart.
- The ClickHouse hourly view sees duplicate source replays. `uniq` state is duplicate-tolerant for host/tag attribution; byte estimates are at-least-once and may be high after replay.
- Cardinality reporting is delayed until the processor flushes source rows to ClickHouse.
- Soft warning thresholds are operational signals, not tenant quotas.

## Verification

Use the `Cardinality and Cost` dashboard and query `observability.cardinality_hourly`. Compare the detector cache gauges and eviction counter to a generated workload before choosing a detector capacity. No production dataset or benchmark result is included yet.
