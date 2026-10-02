-- Rebuild alerts so Kafka replay coordinates are its deduplication key.
--
-- Run during a maintenance window after taking a backup. The bootstrap script
-- adds the two columns to existing local volumes but deliberately does not
-- rebuild a populated table or change its engine automatically.

CREATE TABLE observability.alerts_replacing
(
    ts              DateTime64(3)          CODEC(DoubleDelta, LZ4),
    tenant_id       LowCardinality(String) CODEC(ZSTD(1)),
    metric_name     LowCardinality(String) CODEC(ZSTD(1)),
    host            LowCardinality(String) CODEC(ZSTD(1)),
    value           Float64                CODEC(Gorilla, LZ4),
    anomaly_score   Float64                CODEC(Gorilla, LZ4),
    detector_type   LowCardinality(String) CODEC(ZSTD(1)),
    severity        LowCardinality(String) CODEC(ZSTD(1)),
    tags            Map(String, String)    CODEC(ZSTD(1)),
    kafka_partition UInt16,
    kafka_offset    UInt64
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (tenant_id, metric_name, host, ts, kafka_partition, kafka_offset)
TTL toDateTime(ts) + INTERVAL 1 YEAR DELETE
SETTINGS index_granularity = 8192;

INSERT INTO observability.alerts_replacing
SELECT
    ts, tenant_id, metric_name, host, value, anomaly_score, detector_type,
    severity, tags, kafka_partition, kafka_offset
FROM observability.alerts;

RENAME TABLE observability.alerts TO observability.alerts_legacy,
             observability.alerts_replacing TO observability.alerts;

-- Validate first, then drop the legacy table manually:
-- DROP TABLE observability.alerts_legacy;
