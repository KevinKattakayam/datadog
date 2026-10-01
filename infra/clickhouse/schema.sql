-- ClickHouse Schema for Observability Pipeline
-- tenant_id leads the sort key so every tenant-scoped query prunes to a contiguous range.

CREATE DATABASE IF NOT EXISTS observability;

CREATE TABLE IF NOT EXISTS observability.metrics
(
    ts              DateTime64(3)          CODEC(DoubleDelta, LZ4),
    tenant_id       LowCardinality(String) CODEC(ZSTD(1)),
    name            LowCardinality(String) CODEC(ZSTD(1)),
    host            LowCardinality(String) CODEC(ZSTD(1)),
    value           Float64                CODEC(Gorilla, LZ4),
    unit            LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    tags            Map(String, String)    CODEC(ZSTD(1)),
    anomaly_score   Float64                DEFAULT 0.0 CODEC(Gorilla, LZ4),
    is_anomaly      UInt8                  DEFAULT 0   CODEC(T64, LZ4),
    kafka_partition UInt16                 DEFAULT 0,
    kafka_offset    UInt64                 DEFAULT 0,
    ingested_at     DateTime64(3)          DEFAULT now64(3) CODEC(DoubleDelta, LZ4)
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (tenant_id, name, host, ts, kafka_partition, kafka_offset)
TTL toDateTime(ts) + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192;

-- Hourly aggregate rollup. Materialized views process inserted blocks, so a
-- replay can be counted again here even though raw metrics deduplicate at read
-- time with FINAL. Treat these rollups as at-least-once aggregates.
CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_hourly
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (tenant_id, name, host, hour)
TTL toDateTime(hour) + INTERVAL 400 DAY DELETE
AS SELECT
    toStartOfHour(ts) AS hour, tenant_id, name, host,
    avgState(value) AS avg_value, minState(value) AS min_value,
    maxState(value) AS max_value, countState() AS sample_count,
    quantileState(0.50)(value) AS p50_value,
    quantileState(0.95)(value) AS p95_value,
    quantileState(0.99)(value) AS p99_value
FROM observability.metrics GROUP BY hour, tenant_id, name, host;

-- Daily aggregate rollup; same replay caveat as metrics_hourly.
CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_daily
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, name, host, day)
TTL toDateTime(day) + INTERVAL 400 DAY DELETE
AS SELECT
    toStartOfDay(ts) AS day, tenant_id, name, host,
    avgState(value) AS avg_value, minState(value) AS min_value,
    maxState(value) AS max_value, countState() AS sample_count
FROM observability.metrics GROUP BY day, tenant_id, name, host;

-- Alerts table
CREATE TABLE IF NOT EXISTS observability.alerts
(
    ts            DateTime64(3)          CODEC(DoubleDelta, LZ4),
    tenant_id     LowCardinality(String) CODEC(ZSTD(1)),
    metric_name   LowCardinality(String) CODEC(ZSTD(1)),
    host          LowCardinality(String) CODEC(ZSTD(1)),
    value         Float64                CODEC(Gorilla, LZ4),
    anomaly_score Float64                CODEC(Gorilla, LZ4),
    detector_type LowCardinality(String) CODEC(ZSTD(1)),
    severity      LowCardinality(String) CODEC(ZSTD(1)),
    tags          Map(String, String)    CODEC(ZSTD(1))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (tenant_id, metric_name, host, ts)
TTL toDateTime(ts) + INTERVAL 1 YEAR DELETE
SETTINGS index_granularity = 8192;

-- DLQ events for queryable/alertable dead-letter tracking
CREATE TABLE IF NOT EXISTS observability.dlq_events
(
    ts               DateTime64(3)          CODEC(DoubleDelta, LZ4),
    reason           LowCardinality(String) CODEC(ZSTD(1)),
    detail           String                 CODEC(ZSTD(3)),
    source_partition UInt16,
    source_offset    UInt64,
    payload_sample   String                 CODEC(ZSTD(3))
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts)
ORDER BY (reason, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY DELETE;

-- Projection for host-first access pattern (incident investigation)
-- Costs storage, saves a full partition scan on "show me everything on prod-api-07"
ALTER TABLE observability.metrics ADD PROJECTION IF NOT EXISTS by_host (
    SELECT * ORDER BY (tenant_id, host, ts)
);

-- Cardinality tracking materialized view
CREATE MATERIALIZED VIEW IF NOT EXISTS observability.cardinality_hourly
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (tenant_id, name, hour)
TTL toDateTime(hour) + INTERVAL 90 DAY DELETE
AS SELECT
    toStartOfHour(ts) AS hour, tenant_id, name,
    uniqState(host) AS hosts,
    uniqState(cityHash64(toString(tags))) AS tag_combos,
    sumState(toUInt64(length(tenant_id) + length(name) + length(host) + length(toString(tags)) + 32))
        AS estimated_payload_bytes
FROM observability.metrics GROUP BY hour, tenant_id, name;
