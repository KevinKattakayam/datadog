// Dead-letter producer. A message reaches here only when it cannot be
// parsed; the original bytes are preserved verbatim so the payload can be
// replayed after a producer-side fix.

use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::config::ClientConfig;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use tracing::warn;

#[derive(Debug, Clone)]
pub enum DlqReason {
    EmptyPayload,
    Deserialize(String),
    ValueNotFinite,
}

impl DlqReason {
    pub fn code(&self) -> &'static str {
        match self {
            DlqReason::EmptyPayload => "empty_payload",
            DlqReason::Deserialize(_) => "deserialize_error",
            DlqReason::ValueNotFinite => "value_not_finite",
        }
    }

    fn detail(&self) -> String {
        match self {
            DlqReason::Deserialize(d) => d.chars().take(512).collect(),
            _ => String::new(),
        }
    }
}

pub struct DlqProducer {
    producer: FutureProducer,
    topic: String,
}

impl DlqProducer {
    pub fn new(brokers: &str, topic: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("compression.type", "lz4")
            .set("message.timeout.ms", "30000")
            .create()
            .context("failed to create DLQ producer")?;
        Ok(DlqProducer {
            producer,
            topic: topic.to_string(),
        })
    }

    /// Awaited to completion. The caller must not commit the source offset
    /// until this returns Ok, or a poison message would vanish silently —
    /// which is exactly the bug this file exists to fix.
    pub async fn send(
        &self,
        payload: &[u8],
        reason: DlqReason,
        source_partition: i32,
        source_offset: i64,
    ) -> Result<()> {
        let detail = reason.detail();
        let part = source_partition.to_string();
        let off = source_offset.to_string();
        let now = chrono::Utc::now().to_rfc3339();

        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "dlq.reason",
                value: Some(reason.code()),
            })
            .insert(Header {
                key: "dlq.detail",
                value: Some(&detail),
            })
            .insert(Header {
                key: "dlq.source_topic",
                value: Some("metrics.raw"),
            })
            .insert(Header {
                key: "dlq.source_partition",
                value: Some(&part),
            })
            .insert(Header {
                key: "dlq.source_offset",
                value: Some(&off),
            })
            .insert(Header {
                key: "dlq.timestamp",
                value: Some(&now),
            });

        let key = format!("{source_partition}:{source_offset}");
        let record = FutureRecord::to(&self.topic)
            .key(&key)
            .payload(payload)
            .headers(headers);

        self.producer
            .send(record, Duration::from_secs(30))
            .await
            .map_err(|(e, _)| e)
            .context("DLQ send failed")?;

        crate::metrics::DLQ_MESSAGES
            .with_label_values(&[reason.code()])
            .inc();
        warn!(
            reason = reason.code(),
            source_partition, source_offset, "routed to DLQ"
        );
        Ok(())
    }
}
