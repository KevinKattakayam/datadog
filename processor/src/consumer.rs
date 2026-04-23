// Kafka consumer loop — core of the Rust processor.
//
// Consumes from metrics.raw, runs anomaly detection, publishes alerts,
// and sends processed metrics to ClickHouse via channel.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::detector::ewma::EwmaDetector;
use crate::detector::zscore::ZScoreDetector;
use crate::detector::AnomalyDetector;
use crate::model::{Alert, AlertRow, MetricRow, ProcessedMetric, RawMetric};
use crate::producer::AlertProducer;

/// Run the main consumer loop.
pub async fn run(
    config: Arc<Config>,
    metric_tx: mpsc::Sender<MetricRow>,
    alert_tx: mpsc::Sender<AlertRow>,
    alert_producer: Arc<AlertProducer>,
) -> Result<()> {
    // Create Kafka consumer
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("group.id", &config.kafka_consumer_group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "30000")
        .set("max.poll.interval.ms", "300000")
        .create()?;

    consumer.subscribe(&[&config.kafka_topic_raw])?;

    info!(
        topic = %config.kafka_topic_raw,
        group = %config.kafka_consumer_group,
        "Kafka consumer started"
    );

    // Per-metric detectors (keyed by metric name + host)
    let mut ewma_detectors: HashMap<String, EwmaDetector> = HashMap::new();
    let mut zscore_detectors: HashMap<String, ZScoreDetector> = HashMap::new();

    // Main consume loop
    loop {
        match consumer.recv().await {
            Ok(msg) => {
                let start = Instant::now();
                let partition = msg.partition() as u16;
                let offset = msg.offset() as u64;

                // Deserialize the message
                let payload = match msg.payload() {
                    Some(p) => p,
                    None => {
                        warn!(partition = partition, offset = offset, "Empty message payload");
                        consumer.commit_message(&msg, CommitMode::Async)?;
                        continue;
                    }
                };

                let raw_metric: RawMetric = match serde_json::from_slice(payload) {
                    Ok(m) => m,
                    Err(e) => {
                        warn!(
                            error = %e,
                            partition = partition,
                            offset = offset,
                            "Failed to deserialize metric — sending to DLQ"
                        );
                        crate::metrics::DLQ_MESSAGES.inc();
                        consumer.commit_message(&msg, CommitMode::Async)?;
                        continue;
                    }
                };

                crate::metrics::MESSAGES_CONSUMED.inc();

                // Run anomaly detection
                let detector_key = format!("{}:{}", raw_metric.name, raw_metric.host);

                // EWMA detection
                let ewma = ewma_detectors
                    .entry(detector_key.clone())
                    .or_insert_with(|| {
                        EwmaDetector::new(config.ewma_alpha, config.anomaly_threshold)
                    });
                let ewma_result = ewma.detect(&raw_metric);

                // Z-score detection
                let zscore = zscore_detectors
                    .entry(detector_key)
                    .or_insert_with(|| {
                        ZScoreDetector::new(config.zscore_window, config.anomaly_threshold)
                    });
                let zscore_result = zscore.detect(&raw_metric);

                // Use the higher score / any anomaly flag
                let is_anomaly = ewma_result.is_anomaly || zscore_result.is_anomaly;
                let anomaly_score = ewma_result.score.max(zscore_result.score);
                let detector_type = if ewma_result.score > zscore_result.score {
                    ewma_result.detector_type.clone()
                } else {
                    zscore_result.detector_type.clone()
                };

                // Build processed metric
                let processed = ProcessedMetric {
                    name: raw_metric.name.clone(),
                    value: raw_metric.value,
                    unit: raw_metric.unit.clone(),
                    tags: raw_metric.tags.clone(),
                    timestamp: raw_metric.timestamp,
                    host: raw_metric.host.clone(),
                    anomaly_score,
                    is_anomaly,
                    detector_type: detector_type.clone(),
                };

                // Publish processed metric to Kafka
                if let Err(e) = alert_producer.publish_processed(&processed).await {
                    error!(error = %e, "Failed to publish processed metric");
                }

                // Send to ClickHouse writer
                let metric_row = MetricRow::from_processed(&processed, partition, offset);
                if let Err(e) = metric_tx.send(metric_row).await {
                    error!(error = %e, "Failed to send metric to ClickHouse writer");
                }

                // Fire alert if anomaly detected
                if is_anomaly {
                    crate::metrics::ANOMALIES_DETECTED.inc();

                    let severity = if anomaly_score > config.anomaly_threshold * 1.5 {
                        crate::model::AlertSeverity::Critical
                    } else {
                        crate::model::AlertSeverity::Warning
                    };

                    let alert = Alert {
                        timestamp: raw_metric.timestamp,
                        metric_name: raw_metric.name.clone(),
                        host: raw_metric.host.clone(),
                        value: raw_metric.value,
                        anomaly_score,
                        detector_type,
                        severity,
                        tags: raw_metric.tags.clone(),
                    };

                    // Publish alert to Kafka
                    if let Err(e) = alert_producer.publish_alert(&alert).await {
                        error!(error = %e, "Failed to publish alert");
                    }

                    // Send alert to ClickHouse
                    let alert_row = AlertRow::from_alert(&alert);
                    if let Err(e) = alert_tx.send(alert_row).await {
                        error!(error = %e, "Failed to send alert to ClickHouse writer");
                    }

                    info!(
                        metric = %raw_metric.name,
                        host = %raw_metric.host,
                        value = raw_metric.value,
                        score = anomaly_score,
                        "Anomaly detected"
                    );
                }

                // Record processing latency
                let elapsed = start.elapsed();
                crate::metrics::PROCESSING_LATENCY.observe(elapsed.as_secs_f64());

                debug!(
                    metric = %raw_metric.name,
                    latency_us = elapsed.as_micros(),
                    is_anomaly = is_anomaly,
                    "Message processed"
                );

                // Commit offset AFTER successful processing
                consumer.commit_message(&msg, CommitMode::Async)?;
            }
            Err(e) => {
                error!(error = %e, "Kafka consumer error");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}
