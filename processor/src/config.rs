// Configuration loaded from environment variables.

use std::env;

#[derive(Clone)]
pub struct Config {
    // Kafka
    pub kafka_brokers: String,
    pub kafka_topic_raw: String,
    pub kafka_topic_processed: String,
    pub kafka_topic_alerts: String,
    pub kafka_topic_dlq: String,
    pub kafka_consumer_group: String,
    pub kafka_auto_offset_reset: String,

    // ClickHouse
    pub clickhouse_url: String,
    pub clickhouse_user: String,
    pub clickhouse_password: String,

    // Processor
    pub metrics_port: u16,
    pub batch_size: usize,
    pub flush_interval_ms: u64,

    // Anomaly detection
    pub ewma_alpha: f64,
    pub zscore_window: usize,
    pub anomaly_threshold: f64,
    pub detector_capacity: usize,

    // Write retry
    pub max_write_attempts: u32,
}

impl Config {
    pub fn from_env() -> Self {
        let offset_reset = env_or("KAFKA_AUTO_OFFSET_RESET", "earliest");
        let offset_reset = match offset_reset.as_str() {
            "earliest" | "latest" | "error" => offset_reset,
            _ => "earliest".to_string(),
        };
        let alpha = env_or("PROCESSOR_EWMA_ALPHA", "0.3")
            .parse::<f64>()
            .unwrap_or(0.3);
        let threshold = env_or("PROCESSOR_ANOMALY_THRESHOLD", "3.0")
            .parse::<f64>()
            .unwrap_or(3.0);
        Config {
            kafka_brokers: env_or("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic_raw: env_or("KAFKA_TOPIC_RAW", "metrics.raw"),
            kafka_topic_processed: env_or("KAFKA_TOPIC_PROCESSED", "metrics.processed"),
            kafka_topic_alerts: env_or("KAFKA_TOPIC_ALERTS", "alerts.fired"),
            kafka_topic_dlq: env_or("KAFKA_TOPIC_DLQ", "metrics.dlq"),
            kafka_consumer_group: env_or("KAFKA_CONSUMER_GROUP", "processor-group"),
            kafka_auto_offset_reset: offset_reset,
            clickhouse_url: env_or("CLICKHOUSE_URL", "http://localhost:8123"),
            clickhouse_user: env_or("CLICKHOUSE_USER", "default"),
            clickhouse_password: env_or("CLICKHOUSE_PASSWORD", ""),
            metrics_port: parse_or("PROCESSOR_METRICS_PORT", 9091u16).max(1),
            batch_size: parse_or("PROCESSOR_BATCH_SIZE", 1000usize).clamp(1, 100_000),
            flush_interval_ms: parse_or("PROCESSOR_FLUSH_INTERVAL_MS", 500u64).clamp(50, 60_000),
            ewma_alpha: if alpha.is_finite() {
                alpha.clamp(0.01, 0.99)
            } else {
                0.3
            },
            zscore_window: parse_or("PROCESSOR_ZSCORE_WINDOW", 300usize).clamp(10, 100_000),
            anomaly_threshold: if threshold.is_finite() {
                threshold.clamp(0.1, 100.0)
            } else {
                3.0
            },
            detector_capacity: parse_or("PROCESSOR_DETECTOR_CAPACITY", 100_000usize)
                .clamp(1000, 1_000_000),
            max_write_attempts: parse_or("PROCESSOR_MAX_WRITE_ATTEMPTS", 5u32).clamp(1, 20),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}

fn parse_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
