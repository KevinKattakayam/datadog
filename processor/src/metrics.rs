// Prometheus metrics for the Rust processor.

use once_cell::sync::Lazy;
use prometheus::{
    register_counter, register_counter_vec, register_gauge, register_histogram, Counter,
    CounterVec, Gauge, Histogram,
};

pub static MESSAGES_CONSUMED: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "processor_messages_consumed_total",
        "Total Kafka messages consumed by the processor",
        &["partition"]
    )
    .unwrap()
});

pub static ANOMALIES_DETECTED: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "processor_anomalies_detected_total",
        "Total anomalies detected by the processor",
        &["metric", "severity"]
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

pub static DLQ_MESSAGES: Lazy<CounterVec> = Lazy::new(|| {
    register_counter_vec!(
        "processor_dlq_messages_total",
        "Total messages sent to dead-letter queue",
        &["reason"]
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

pub static ROWS_COMMITTED: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "processor_rows_committed_total",
        "Total metric rows durably written and offsets committed"
    )
    .unwrap()
});

pub static CIRCUIT_STATE: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "processor_circuit_breaker_state",
        "Circuit breaker state: 0=closed, 1=open, 2=half_open"
    )
    .unwrap()
});

pub static FANOUT_ERRORS: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "processor_fanout_errors_total",
        "Total errors publishing anomaly alerts"
    )
    .unwrap()
});

pub static DETECTOR_EVICTIONS: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "processor_detector_evictions_total",
        "Total detector state evictions from LRU cache"
    )
    .unwrap()
});

pub static DETECTOR_SERIES_TRACKED: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "processor_detector_series_tracked",
        "Number of distinct metric series with active detector state"
    )
    .unwrap()
});

pub static DETECTOR_SERIES_CAPACITY: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!(
        "processor_detector_series_capacity",
        "Configured maximum number of detector series held in the LRU cache"
    )
    .unwrap()
});

pub static NON_FINITE_VALUES: Lazy<Counter> = Lazy::new(|| {
    register_counter!(
        "processor_non_finite_values_total",
        "Total NaN or Inf values rejected by detectors"
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
    Lazy::force(&ROWS_COMMITTED);
    Lazy::force(&CIRCUIT_STATE);
    Lazy::force(&FANOUT_ERRORS);
    Lazy::force(&DETECTOR_EVICTIONS);
    Lazy::force(&DETECTOR_SERIES_TRACKED);
    Lazy::force(&DETECTOR_SERIES_CAPACITY);
    Lazy::force(&NON_FINITE_VALUES);
}
