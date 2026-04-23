-- ClickHouse Schema for Observability Pipeline

CREATE TABLE IF NOT EXISTS observability.metrics
(
    ts              DateTime64(3)          CODEC(DoubleDelta, LZ4),
    name            LowCardinality(String) CODEC(ZSTD(1)),
    host            LowCardinality(String) CODEC(ZSTD(1)),
    value           Float64                CODEC(Gorilla, LZ4),
    unit            LowCardinality(String) DEFAULT '' CODEC(ZSTD(1)),
    tags            Map(String, String)    CODEC(ZSTD(1)),
    anomaly_score   Float64                DEFAULT 0.0 CODEC(Gorilla, LZ4),
    is_anomaly      UInt8                  DEFAULT 0   CODEC(T64, LZ4),
    kafka_partition UInt16                 DEFAULT 0,
    kafka_offset    UInt64                 DEFAULT 0
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (name, host, ts, kafka_partition, kafka_offset)
TTL toDateTime(ts) + INTERVAL 1 YEAR DELETE
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_hourly
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (name, host, hour)
AS SELECT
    toStartOfHour(ts) AS hour, name, host,
    avgState(value) AS avg_value, minState(value) AS min_value,
    maxState(value) AS max_value, countState() AS sample_count,
    quantileState(0.50)(value) AS p50_value,
    quantileState(0.95)(value) AS p95_value,
    quantileState(0.99)(value) AS p99_value
FROM observability.metrics GROUP BY hour, name, host;

CREATE MATERIALIZED VIEW IF NOT EXISTS observability.metrics_daily
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (name, host, day)
AS SELECT
    toStartOfDay(ts) AS day, name, host,
    avgState(value) AS avg_value, minState(value) AS min_value,
    maxState(value) AS max_value, countState() AS sample_count
FROM observability.metrics GROUP BY day, name, host;

CREATE TABLE IF NOT EXISTS observability.alerts
(
    ts            DateTime64(3)          CODEC(DoubleDelta, LZ4),
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
ORDER BY (metric_name, host, ts)
TTL toDateTime(ts) + INTERVAL 1 YEAR DELETE
SETTINGS index_granularity = 8192;
