// Kafka consumer loop. Owns batching, ClickHouse persistence and offset
// commits so that an offset is NEVER committed before its row is durable.
//
// The restructure: delete the mpsc channel for metrics. The consumer
// accumulates rows in a per-partition batch, and on flush it writes to
// ClickHouse AND WAITS, then commits the exact offsets it just persisted.
// If the write fails, it does not commit, and the messages are replayed.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::{Offset, TopicPartitionList};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::detector::registry::DetectorRegistry;
use crate::dlq::{DlqProducer, DlqReason};
use crate::model::{Alert, AlertRow, MetricRow, ProcessedMetric, RawMetric};
use crate::producer::AlertProducer;
use crate::storage::clickhouse::ClickHouseWriter;

/// Rows accumulated for one partition, plus the highest offset they cover.
struct PartitionBatch {
    rows: Vec<MetricRow>,
    alert_rows: Vec<AlertRow>,
    last_offset: i64,
}

impl PartitionBatch {
    fn new() -> Self {
        PartitionBatch {
            rows: Vec::new(),
            alert_rows: Vec::new(),
            last_offset: -1,
        }
    }
}

pub struct ConsumerLoop {
    consumer: StreamConsumer,
    writer: Arc<ClickHouseWriter>,
    alerts: Arc<AlertProducer>,
    dlq: Arc<DlqProducer>,
    config: Arc<Config>,
    batches: HashMap<i32, PartitionBatch>,
    detectors: DetectorRegistry,
    buffered: usize,
}

impl ConsumerLoop {
    pub fn new(
        config: Arc<Config>,
        writer: Arc<ClickHouseWriter>,
        alerts: Arc<AlertProducer>,
        dlq: Arc<DlqProducer>,
    ) -> Result<Self> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka_brokers)
            .set("group.id", &config.kafka_consumer_group)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", &config.kafka_auto_offset_reset)
            .set("session.timeout.ms", "30000")
            // Must exceed worst-case flush time or the broker evicts us
            // mid-write and hands our partitions to someone else.
            .set("max.poll.interval.ms", "300000")
            .set("partition.assignment.strategy", "cooperative-sticky")
            .create()?;

        consumer.subscribe(&[&config.kafka_topic_raw])?;

        Ok(ConsumerLoop {
            consumer,
            writer,
            alerts,
            dlq,
            detectors: DetectorRegistry::new(
                config.detector_capacity,
                config.ewma_alpha,
                config.zscore_window,
                config.anomaly_threshold,
            ),
            config,
            batches: HashMap::new(),
            buffered: 0,
        })
    }

    pub async fn run(&mut self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let flush_interval = Duration::from_millis(self.config.flush_interval_ms);
        let mut ticker = tokio::time::interval(flush_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Once a flush fails, stop consuming until the same batch is durable.
        // This keeps memory bounded and makes Kafka the only growing queue.
        let mut flush_blocked = false;

        info!(
            topic = %self.config.kafka_topic_raw,
            group = %self.config.kafka_consumer_group,
            "consumer started"
        );

        loop {
            tokio::select! {
                biased;

                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!(buffered = self.buffered, "shutdown: draining");
                        // Best effort. If this fails we simply do not commit,
                        // and the next owner replays. No data is lost either way.
                        if let Err(e) = self.flush_all().await {
                            warn!(error = %e, "drain flush failed; offsets not committed");
                        }
                        return Ok(());
                    }
                }

                _ = ticker.tick() => {
                    if self.buffered > 0 {
                        match self.flush_all().await {
                            Ok(()) => flush_blocked = false,
                            Err(e) => {
                                flush_blocked = true;
                                error!(error = %e, "timed flush failed; pausing consumption until retry succeeds");
                            }
                        }
                    }
                }

                msg = self.consumer.recv(), if !flush_blocked => {
                    match msg {
                        Ok(m) => {
                            let owned = m.detach();
                            self.handle(&owned).await?;
                            if self.buffered >= self.config.batch_size {
                                match self.flush_all().await {
                                    Ok(()) => flush_blocked = false,
                                    Err(e) => {
                                        flush_blocked = true;
                                        error!(error = %e, "size flush failed; pausing consumption until retry succeeds");
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!(error = %e, "kafka recv error");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        }
    }

    async fn handle(&mut self, msg: &rdkafka::message::OwnedMessage) -> Result<()> {
        let start = Instant::now();
        let partition = msg.partition();
        let offset = msg.offset();

        let payload = match msg.payload() {
            Some(p) => p,
            None => {
                // Route to DLQ and only then account for the offset.
                self.dlq
                    .send(&[], DlqReason::EmptyPayload, partition, offset)
                    .await?;
                self.account(partition, offset, None, None);
                return Ok(());
            }
        };

        let raw: RawMetric = match serde_json::from_slice(payload) {
            Ok(m) => m,
            Err(e) => {
                self.dlq
                    .send(
                        payload,
                        DlqReason::Deserialize(e.to_string()),
                        partition,
                        offset,
                    )
                    .await?;
                self.account(partition, offset, None, None);
                return Ok(());
            }
        };

        crate::metrics::MESSAGES_CONSUMED
            .with_label_values(&[&partition.to_string()])
            .inc();

        // Guard against non-finite values
        if !raw.value.is_finite() {
            self.dlq
                .send(payload, DlqReason::ValueNotFinite, partition, offset)
                .await?;
            self.account(partition, offset, None, None);
            return Ok(());
        }

        let verdict = self.detectors.evaluate(&raw);

        let processed = ProcessedMetric {
            name: raw.name.clone(),
            value: raw.value,
            unit: raw.unit.clone(),
            tags: raw.tags.clone(),
            timestamp: raw.timestamp,
            host: raw.host.clone(),
            tenant_id: raw.tenant_id.clone(),
            anomaly_score: verdict.score,
            is_anomaly: verdict.is_anomaly,
            detector_type: verdict.detector_type.clone(),
        };

        // Fan-out topics are advisory. A failure here must not block the
        // durable path, but it must be visible.
        if let Err(e) = self.alerts.publish_processed(&processed).await {
            warn!(error = %e, "processed fan-out failed");
            crate::metrics::FANOUT_ERRORS.inc();
        }

        let mut alert_row = None;
        if verdict.is_anomaly {
            crate::metrics::ANOMALIES_DETECTED
                .with_label_values(&[&raw.name, verdict.severity.as_str()])
                .inc();
            let alert = Alert {
                timestamp: raw.timestamp,
                metric_name: raw.name.clone(),
                host: raw.host.clone(),
                tenant_id: raw.tenant_id.clone(),
                value: raw.value,
                anomaly_score: verdict.score,
                detector_type: verdict.detector_type,
                severity: verdict.severity,
                tags: raw.tags.clone(),
            };
            if let Err(e) = self.alerts.publish_alert(&alert).await {
                warn!(error = %e, "alert fan-out failed");
                crate::metrics::FANOUT_ERRORS.inc();
            }
            alert_row = Some(AlertRow::from_alert(&alert));

            info!(
                metric = %raw.name,
                host = %raw.host,
                value = raw.value,
                score = verdict.score,
                "Anomaly detected"
            );
        }

        let row = MetricRow::from_processed(&processed, partition, offset);
        self.account(partition, offset, Some(row), alert_row);

        crate::metrics::PROCESSING_LATENCY.observe(start.elapsed().as_secs_f64());
        debug!(
            metric = %raw.name,
            latency_us = start.elapsed().as_micros(),
            "processed"
        );
        Ok(())
    }

    /// Record that `offset` on `partition` has been accounted for, optionally
    /// with a row that must be persisted before the offset may be committed.
    fn account(
        &mut self,
        partition: i32,
        offset: i64,
        row: Option<MetricRow>,
        alert_row: Option<AlertRow>,
    ) {
        let b = self
            .batches
            .entry(partition)
            .or_insert_with(PartitionBatch::new);
        b.last_offset = offset;
        if let Some(r) = row {
            b.rows.push(r);
            self.buffered += 1;
        }
        if let Some(a) = alert_row {
            b.alert_rows.push(a);
        }
    }

    /// Persist every buffered row, then commit the covered offsets.
    /// Returns Err WITHOUT committing if the write does not succeed.
    async fn flush_all(&mut self) -> Result<()> {
        if self.batches.is_empty() {
            return Ok(());
        }

        let rows: Vec<MetricRow> = self
            .batches
            .values()
            .flat_map(|b| b.rows.iter().cloned())
            .collect();

        let alert_rows: Vec<AlertRow> = self
            .batches
            .values()
            .flat_map(|b| b.alert_rows.iter().cloned())
            .collect();

        if !rows.is_empty() {
            // Blocks until durable or exhausted. Errors propagate: we do not commit.
            self.writer.write_metrics(&rows).await?;
        }

        if !alert_rows.is_empty() {
            // Alert audit rows share the source offset durability boundary.
            self.writer.write_alerts(&alert_rows).await?;
        }

        let mut tpl = TopicPartitionList::new();
        for (partition, b) in self.batches.iter() {
            if b.last_offset >= 0 {
                tpl.add_partition_offset(
                    &self.config.kafka_topic_raw,
                    *partition,
                    Offset::Offset(b.last_offset + 1), // next offset to read
                )?;
            }
        }

        // Sync commit: we want to know it landed.
        self.consumer.commit(&tpl, CommitMode::Sync)?;

        crate::metrics::ROWS_COMMITTED.inc_by(rows.len() as f64);
        self.batches.clear();
        self.buffered = 0;
        Ok(())
    }
}
