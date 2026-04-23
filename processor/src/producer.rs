// Kafka producer for publishing alerts and processed metrics to downstream topics.

use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tracing::{error, info};

use crate::model::{Alert, ProcessedMetric};

/// Kafka producer for publishing to alerts.fired and metrics.processed topics.
pub struct AlertProducer {
    producer: FutureProducer,
    alerts_topic: String,
    processed_topic: String,
}

impl AlertProducer {
    /// Create a new alert producer.
    pub fn new(brokers: &str, alerts_topic: &str, processed_topic: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("queue.buffering.max.messages", "10000")
            .set("queue.buffering.max.ms", "100")
            .create()?;

        info!(
            brokers = brokers,
            alerts_topic = alerts_topic,
            processed_topic = processed_topic,
            "Alert producer initialized"
        );

        Ok(AlertProducer {
            producer,
            alerts_topic: alerts_topic.to_string(),
            processed_topic: processed_topic.to_string(),
        })
    }

    /// Publish an alert to the alerts.fired topic.
    pub async fn publish_alert(&self, alert: &Alert) -> Result<()> {
        let payload = serde_json::to_string(alert)?;
        let key = format!("{}-{}", alert.metric_name, alert.host);

        let record = FutureRecord::to(&self.alerts_topic)
            .key(&key)
            .payload(&payload);

        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok(_) => {
                crate::metrics::ALERTS_FIRED.inc();
                Ok(())
            }
            Err((e, _)) => {
                error!(error = %e, metric = %alert.metric_name, "Failed to publish alert");
                Err(e.into())
            }
        }
    }

    /// Publish a processed metric to the metrics.processed topic.
    pub async fn publish_processed(&self, metric: &ProcessedMetric) -> Result<()> {
        let payload = serde_json::to_string(metric)?;
        let key = metric.host.clone();

        let record = FutureRecord::to(&self.processed_topic)
            .key(&key)
            .payload(&payload);

        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok(_) => Ok(()),
            Err((e, _)) => {
                error!(error = %e, metric = %metric.name, "Failed to publish processed metric");
                Err(e.into())
            }
        }
    }
}
