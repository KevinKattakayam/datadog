// Prometheus metrics for the Rust processor.

use prometheus::{
    register_counter, register_gauge, register_histogram, Counter, Gauge, Histogram,
};
use once_cell::sync::Lazy;

// We use once_cell::sync::Lazy for static initialization
// Since prometheus 0.13 uses lazy_static internally, we match the pattern

pub static MESSAGES_CONSUMED: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "processor_messages_consumed_total",
        "Total Kafka messages consumed by the processor"
    )
    .unwrap()
});

pub static ANOMALIES_DETECTED: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "processor_anomalies_detected_total",
        "Total anomalies detected by the processor"
    )
    .unwrap()
});

pub static PROCESSING_LATENCY: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "processor_processing_latency_seconds",
        "Time to process a single message",
        vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1]
    )
    .unwrap()
});

pub static CLICKHOUSE_WRITE_DURATION: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!(
        "processor_clickhouse_write_duration_seconds",
        "Time to write a batch to ClickHouse",
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
    )
    .unwrap()
});

pub static CLICKHOUSE_BATCH_SIZE: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "processor_clickhouse_batch_size",
        "Size of the last ClickHouse batch insert"
    )
    .unwrap()
});

pub static CLICKHOUSE_WRITE_ERRORS: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "processor_clickhouse_write_errors_total",
        "Total ClickHouse write failures"
    )
    .unwrap()
});

pub static DLQ_MESSAGES: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "processor_dlq_messages_total",
        "Total messages sent to dead-letter queue"
    )
    .unwrap()
});

pub static ALERTS_FIRED: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "processor_alerts_fired_total",
        "Total alerts published to alerts.fired topic"
    )
    .unwrap()
});

/// Initialize all metrics (force lazy static evaluation).
pub fn init() {
    Lazy::force(&MESSAGES_CONSUMED);
    Lazy::force(&ANOMALIES_DETECTED);
    Lazy::force(&PROCESSING_LATENCY);
    Lazy::force(&CLICKHOUSE_WRITE_DURATION);
    Lazy::force(&CLICKHOUSE_BATCH_SIZE);
    Lazy::force(&CLICKHOUSE_WRITE_ERRORS);
    Lazy::force(&DLQ_MESSAGES);
    Lazy::force(&ALERTS_FIRED);
}
