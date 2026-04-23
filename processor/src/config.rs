// Configuration loaded from environment variables.

use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    // Kafka
    pub kafka_brokers: String,
    pub kafka_topic_raw: String,
    pub kafka_topic_processed: String,
    pub kafka_topic_alerts: String,
    pub kafka_topic_dlq: String,
    pub kafka_consumer_group: String,

    // ClickHouse
    pub clickhouse_url: String,

    // Processor
    pub metrics_port: u16,
    pub batch_size: usize,
    pub flush_interval_ms: u64,

    // Anomaly detection
    pub ewma_alpha: f64,
    pub zscore_window: usize,
    pub anomaly_threshold: f64,
}

impl Config {
    pub fn from_env() -> Self {
        Config {
            kafka_brokers: env_or("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_topic_raw: env_or("KAFKA_TOPIC_RAW", "metrics.raw"),
            kafka_topic_processed: env_or("KAFKA_TOPIC_PROCESSED", "metrics.processed"),
            kafka_topic_alerts: env_or("KAFKA_TOPIC_ALERTS", "alerts.fired"),
            kafka_topic_dlq: env_or("KAFKA_TOPIC_DLQ", "metrics.dlq"),
            kafka_consumer_group: env_or("KAFKA_CONSUMER_GROUP", "processor-group"),
            clickhouse_url: env_or("CLICKHOUSE_URL", "http://localhost:8123/observability"),
            metrics_port: env_or("PROCESSOR_METRICS_PORT", "9091")
                .parse()
                .unwrap_or(9091),
            batch_size: env_or("PROCESSOR_BATCH_SIZE", "1000")
                .parse()
                .unwrap_or(1000),
            flush_interval_ms: env_or("PROCESSOR_FLUSH_INTERVAL_MS", "500")
                .parse()
                .unwrap_or(500),
            ewma_alpha: env_or("PROCESSOR_EWMA_ALPHA", "0.3")
                .parse()
                .unwrap_or(0.3),
            zscore_window: env_or("PROCESSOR_ZSCORE_WINDOW", "300")
                .parse()
                .unwrap_or(300),
            anomaly_threshold: env_or("PROCESSOR_ANOMALY_THRESHOLD", "3.0")
                .parse()
                .unwrap_or(3.0),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.to_string())
}
