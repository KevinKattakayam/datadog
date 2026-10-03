// Kafka producer for publishing anomaly alerts to downstream consumers.
//
// Delivery contract: at-least-once. Alerts are published before the source
// offset is committed, so a crash between the two re-runs detection on the
// replayed records and can publish the same alert twice. Every alert carries
// an `alert.id` header derived from its source Kafka coordinates; that ID is
// identical on replay, so consumers (Alertmanager bridges, pagers, the
// ClickHouse `alerts` table) can deduplicate on it.

use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tracing::{error, info};

use crate::model::Alert;

/// Kafka producer for publishing to alerts.fired.
pub struct AlertProducer {
    producer: FutureProducer,
    alerts_topic: String,
}

/// Stable identity of an alert across replays: the source record's position.
pub fn alert_id(source_topic: &str, partition: i32, offset: i64) -> String {
    format!("{source_topic}:{partition}:{offset}")
}

impl AlertProducer {
    /// Create a new alert producer.
    pub fn new(brokers: &str, alerts_topic: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            // An alert that is lost is worse than one that is late.
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", "5000")
            .set("queue.buffering.max.messages", "10000")
            .set("queue.buffering.max.ms", "20")
            .set("compression.type", "lz4")
            .create()?;

        info!(
            brokers = brokers,
            alerts_topic = alerts_topic,
            "Alert producer initialized"
        );

        Ok(AlertProducer {
            producer,
            alerts_topic: alerts_topic.to_string(),
        })
    }

    /// Publish an alert to the alerts topic, tagged with its replay-stable ID.
    pub async fn publish_alert(&self, alert: &Alert, id: &str) -> Result<()> {
        let payload = serde_json::to_string(alert)?;
        let tenant = alert.tenant_id.as_deref().unwrap_or("default");
        // Tenant first: all of one tenant's alerts for a series stay ordered on
        // one partition, and two tenants with the same host name never collide.
        let key = format!("{}|{}|{}", tenant, alert.metric_name, alert.host);

        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "alert.id",
                value: Some(id),
            })
            .insert(Header {
                key: "tenant_id",
                value: Some(tenant),
            });

        let record = FutureRecord::to(&self.alerts_topic)
            .key(&key)
            .payload(&payload)
            .headers(headers);

        match self.producer.send(record, Duration::from_secs(5)).await {
            Ok(_) => {
                crate::metrics::ALERTS_FIRED.inc();
                Ok(())
            }
            Err((e, _)) => {
                error!(error = %e, metric = %alert.metric_name, alert_id = id, "Failed to publish alert");
                Err(e.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alert_id_is_stable_for_the_same_source_record() {
        assert_eq!(
            alert_id("metrics.raw", 3, 42),
            alert_id("metrics.raw", 3, 42)
        );
        assert_ne!(
            alert_id("metrics.raw", 3, 42),
            alert_id("metrics.raw", 3, 43)
        );
        assert_eq!(alert_id("metrics.raw", 3, 42), "metrics.raw:3:42");
    }
}
