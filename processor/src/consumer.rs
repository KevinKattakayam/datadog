// Kafka consumer loop. Owns batching, ClickHouse persistence and offset
// commits so that an offset is NEVER committed before its row is durable.
//
// The restructure: delete the mpsc channel for metrics. The consumer
// accumulates rows in a per-partition batch, and on flush it writes to
// ClickHouse AND WAITS, then commits the exact offsets it just persisted.
// If the write fails, it does not commit, and the messages are replayed.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Result;
use futures_util::StreamExt;
use rdkafka::client::ClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::message::Message;
use rdkafka::{Offset, TopicPartitionList};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::detector::registry::DetectorRegistry;
use crate::dlq::{DlqProducer, DlqReason};
use crate::model::{Alert, AlertRow, MetricRow, ProcessedMetric, RawMetric};
use crate::producer::{alert_id, AlertProducer};
use crate::storage::clickhouse::{is_permanent_clickhouse_error, ClickHouseWriter};

/// Rows accumulated for one partition, plus the highest offset they cover.
struct PartitionBatch {
    rows: Vec<MetricRow>,
    alert_rows: Vec<AlertRow>,
    last_offset: i64,
    /// Number of Kafka records (not rows) this batch accounts for.
    records: usize,
    /// Kafka CreateTime of each accounted record, for freshness measurement.
    created_ms: Vec<i64>,
}

impl PartitionBatch {
    fn new() -> Self {
        PartitionBatch {
            rows: Vec::new(),
            alert_rows: Vec::new(),
            last_offset: -1,
            records: 0,
            created_ms: Vec::new(),
        }
    }
}

/// Consumer context that records rebalance events for the consumer loop.
///
/// librdkafka invokes these callbacks from inside `poll`, on the task that is
/// driving the stream, so they must be quick and must not touch the loop's
/// state directly. They only record which partitions were revoked; the loop
/// drains that list before it next writes or commits.
///
/// Why it matters: rows buffered for a partition we no longer own must not
/// have their offsets committed by us. The broker does not check partition
/// ownership on OffsetCommit, so a late commit from the previous owner can
/// move the new owner's committed position backwards (duplicate replay) or
/// interleave with its commits. Releasing the batch is always safe: nothing
/// in it was committed, so the new owner replays it from the last commit.
#[derive(Default)]
pub struct RebalanceTracker {
    revoked: Mutex<Vec<i32>>,
    assigned: Mutex<HashSet<i32>>,
}

impl RebalanceTracker {
    fn take_revoked(&self) -> Vec<i32> {
        std::mem::take(&mut *self.revoked.lock().unwrap_or_else(|e| e.into_inner()))
    }
}

impl ClientContext for RebalanceTracker {}

impl ConsumerContext for RebalanceTracker {
    fn pre_rebalance(&self, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Revoke(tpl) => {
                crate::metrics::REBALANCE_EVENTS
                    .with_label_values(&["revoke"])
                    .inc();
                let parts: Vec<i32> = tpl.elements().iter().map(|e| e.partition()).collect();
                info!(partitions = ?parts, "partitions revoked");
                let mut assigned = self.assigned.lock().unwrap_or_else(|e| e.into_inner());
                for p in &parts {
                    assigned.remove(p);
                }
                crate::metrics::ASSIGNED_PARTITIONS.set(assigned.len() as f64);
                self.revoked
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .extend(parts);
            }
            Rebalance::Assign(tpl) => {
                crate::metrics::REBALANCE_EVENTS
                    .with_label_values(&["assign"])
                    .inc();
                let parts: Vec<i32> = tpl.elements().iter().map(|e| e.partition()).collect();
                info!(partitions = ?parts, "partitions assigned");
                let mut assigned = self.assigned.lock().unwrap_or_else(|e| e.into_inner());
                assigned.extend(parts);
                crate::metrics::ASSIGNED_PARTITIONS.set(assigned.len() as f64);
            }
            Rebalance::Error(e) => {
                crate::metrics::REBALANCE_EVENTS
                    .with_label_values(&["error"])
                    .inc();
                warn!(error = %e, "rebalance error");
            }
        }
    }
}

type PipelineConsumer = StreamConsumer<RebalanceTracker>;

pub struct ConsumerLoop {
    consumer: Arc<PipelineConsumer>,
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
        let consumer: PipelineConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka_brokers)
            .set("group.id", &config.kafka_consumer_group)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", &config.kafka_auto_offset_reset)
            .set("session.timeout.ms", "30000")
            // Must exceed worst-case flush time or the broker evicts us
            // mid-write and hands our partitions to someone else. Worst case
            // is max_write_attempts x clickhouse_timeout plus backoff; the
            // defaults (5 x 30s) stay well under this.
            .set("max.poll.interval.ms", "300000")
            .set("partition.assignment.strategy", "cooperative-sticky")
            .create_with_context(RebalanceTracker::default())?;

        consumer.subscribe(&[&config.kafka_topic_raw])?;

        Ok(ConsumerLoop {
            consumer: Arc::new(consumer),
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
        // Once a flush fails, pause assigned partitions. We must still call
        // recv while paused so librdkafka continues polling and heartbeating;
        // otherwise max.poll.interval.ms evicts this member during an outage.
        let mut flush_blocked = false;
        // Keep one stream alive for the lifetime of the loop. Recreating a
        // recv future whenever the flush timer fires can cancel it before
        // librdkafka's scheduled wake-up polls the consumer.
        let polling_consumer = Arc::clone(&self.consumer);
        let mut messages = polling_consumer.stream();

        info!(
            topic = %self.config.kafka_topic_raw,
            group = %self.config.kafka_consumer_group,
            "consumer started"
        );

        loop {
            // Do not bias the periodic flush over recv. During an outage the
            // flush branch can be ready repeatedly; fairness is required so
            // the stream consumer gets a chance to poll and heartbeat.
            tokio::select! {

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
                            Ok(()) => {
                                if flush_blocked {
                                    self.resume_assigned();
                                }
                                flush_blocked = false;
                            }
                            Err(e) => {
                                self.pause_assigned();
                                flush_blocked = true;
                                error!(error = %e, "timed flush failed; partitions paused while polling continues");
                            }
                        }
                    }
                }

                // Continue receiving while partitions are paused. This keeps
                // group heartbeats and rebalance handling alive. librdkafka
                // can return records it prefetched before pause; retaining
                // those in the same uncommitted batch is safe and bounded.
                msg = messages.next() => {
                    match msg {
                        Some(Ok(m)) => {
                            let owned = m.detach();
                            self.handle(&owned).await?;
                            if !flush_blocked && self.buffered >= self.config.batch_size {
                                match self.flush_all().await {
                                    Ok(()) => {
                                        if flush_blocked {
                                            self.resume_assigned();
                                        }
                                        flush_blocked = false;
                                    }
                                    Err(e) => {
                                        self.pause_assigned();
                                        flush_blocked = true;
                                        error!(error = %e, "size flush failed; partitions paused while polling continues");
                                    }
                                }
                            }
                        }
                        Some(Err(e)) => {
                            error!(error = %e, "kafka recv error");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                        None => return Err(anyhow::anyhow!("kafka message stream ended")),
                    }
                }
            }
        }
    }

    fn pause_assigned(&self) {
        match self.consumer.assignment() {
            Ok(partitions) => {
                if let Err(e) = self.consumer.pause(&partitions) {
                    warn!(error = %e, "failed to pause assigned partitions");
                }
            }
            Err(e) => warn!(error = %e, "failed to obtain assigned partitions for pause"),
        }
    }

    fn resume_assigned(&self) {
        match self.consumer.assignment() {
            Ok(partitions) => {
                if let Err(e) = self.consumer.resume(&partitions) {
                    warn!(error = %e, "failed to resume assigned partitions");
                }
            }
            Err(e) => warn!(error = %e, "failed to obtain assigned partitions for resume"),
        }
    }

    async fn handle(&mut self, msg: &rdkafka::message::OwnedMessage) -> Result<()> {
        let start = Instant::now();
        let partition = msg.partition();
        let offset = msg.offset();
        let created_ms = msg.timestamp().to_millis();

        let payload = match msg.payload() {
            Some(p) => p,
            None => {
                // Route to DLQ and only then account for the offset.
                self.dlq
                    .send(&[], DlqReason::EmptyPayload, partition, offset)
                    .await?;
                self.account(partition, offset, created_ms, None, None);
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
                self.account(partition, offset, created_ms, None, None);
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
            self.account(partition, offset, created_ms, None, None);
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

        let mut alert_row = None;
        if verdict.is_anomaly {
            crate::metrics::ANOMALIES_DETECTED
                .with_label_values(&[verdict.detector_type.as_str(), verdict.severity.as_str()])
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
            let id = alert_id(&self.config.kafka_topic_raw, partition, offset);
            if let Err(e) = self.alerts.publish_alert(&alert, &id).await {
                warn!(error = %e, "alert fan-out failed");
                crate::metrics::FANOUT_ERRORS.inc();
            }
            alert_row = Some(AlertRow::from_alert(&alert, partition, offset));

            info!(
                metric = %raw.name,
                host = %raw.host,
                value = raw.value,
                score = verdict.score,
                "Anomaly detected"
            );
        }

        let row = MetricRow::from_processed(&processed, partition, offset);
        self.account(partition, offset, created_ms, Some(row), alert_row);

        crate::metrics::PROCESSING_LATENCY.observe(start.elapsed().as_secs_f64());
        debug!(
            metric = %raw.name,
            latency_us = start.elapsed().as_micros(),
            "processed"
        );
        Ok(())
    }

    /// Record that `offset` on `partition` has been accounted for, optionally
    /// with rows that must be persisted before the offset may be committed.
    ///
    /// `buffered` counts Kafka records, not ClickHouse rows: DLQ-only records
    /// still require an offset commit after their DLQ publish succeeds.
    fn account(
        &mut self,
        partition: i32,
        offset: i64,
        created_ms: Option<i64>,
        row: Option<MetricRow>,
        alert_row: Option<AlertRow>,
    ) {
        let b = self
            .batches
            .entry(partition)
            .or_insert_with(PartitionBatch::new);
        b.last_offset = offset;
        b.records += 1;
        if let Some(ts) = created_ms {
            b.created_ms.push(ts);
        }
        self.buffered += 1;
        if let Some(r) = row {
            b.rows.push(r);
        }
        if let Some(a) = alert_row {
            b.alert_rows.push(a);
        }
    }

    /// Persist every buffered row, then commit the covered offsets.
    /// Returns Err WITHOUT committing if the write does not succeed.
    async fn flush_all(&mut self) -> Result<()> {
        self.release_unowned()?;
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
            // Blocks until durable or exhausted. A deterministic row rejection
            // must not freeze its partition forever: isolate it, publish the
            // original row to the DLQ, and durably retain every good sibling.
            // Transport and circuit-breaker errors still propagate unchanged,
            // so their offsets remain uncommitted for retry.
            if let Err(error) = self.writer.write_metrics(&rows).await {
                if is_permanent_clickhouse_error(&error) {
                    warn!(error = %error, rows = rows.len(), "isolating permanent ClickHouse row rejection");
                    self.bisect_metrics_or_send_to_dlq().await?;
                } else {
                    return Err(error);
                }
            }
        }

        if !alert_rows.is_empty() {
            // Alert audit rows share the source offset durability boundary.
            if let Err(error) = self.writer.write_alerts(&alert_rows).await {
                if is_permanent_clickhouse_error(&error) {
                    warn!(error = %error, rows = alert_rows.len(), "isolating permanent ClickHouse alert rejection");
                    self.bisect_alerts_or_send_to_dlq().await?;
                } else {
                    return Err(error);
                }
            }
        }

        if std::env::var("PROCESSOR_FAILPOINT").as_deref() == Ok("after_write_before_commit") {
            error!("failpoint after_write_before_commit triggered; aborting before offset commit");
            std::process::abort();
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
        self.observe_freshness();
        self.batches.clear();
        self.buffered = 0;
        Ok(())
    }

    /// Drop buffered records for partitions this member no longer owns, so
    /// their offsets are never committed by us. See `RebalanceTracker`.
    ///
    /// Two sources: partitions the rebalance callback reported as revoked,
    /// and — as a belt-and-braces check right before a write — anything not
    /// in the consumer's current assignment.
    fn release_unowned(&mut self) -> Result<()> {
        let mut drop: HashSet<i32> = self.consumer.context().take_revoked().into_iter().collect();

        if !self.batches.is_empty() {
            let owned: HashSet<i32> = self
                .consumer
                .assignment()?
                .elements()
                .iter()
                .filter(|e| e.topic() == self.config.kafka_topic_raw)
                .map(|e| e.partition())
                .collect();
            drop.extend(self.batches.keys().filter(|p| !owned.contains(p)));
        }

        for partition in drop {
            if let Some(batch) = self.batches.remove(&partition) {
                self.buffered = self.buffered.saturating_sub(batch.records);
                crate::metrics::REVOKED_RECORDS_DROPPED.inc_by(batch.records as f64);
                warn!(
                    partition,
                    records = batch.records,
                    "released uncommitted batch for a partition we no longer own; new owner replays it"
                );
            }
        }
        Ok(())
    }

    /// Record ingest-to-durable latency for every record just committed.
    fn observe_freshness(&self) {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut oldest: Option<i64> = None;
        for ts in self.batches.values().flat_map(|b| b.created_ms.iter()) {
            let lag_s = (now_ms - ts).max(0) as f64 / 1000.0;
            crate::metrics::END_TO_END_LAG.observe(lag_s);
            oldest = Some(oldest.map_or(*ts, |o: i64| o.min(*ts)));
        }
        if let Some(ts) = oldest {
            crate::metrics::OLDEST_COMMITTED_AGE.set((now_ms - ts).max(0) as f64 / 1000.0);
        }
        crate::metrics::LAST_COMMIT_TIMESTAMP.set(now_ms as f64 / 1000.0);
    }

    /// Re-insert subsets until a ClickHouse rejection can be attributed to a
    /// single source row. Good subsets are already durable on return. A poison
    /// row is written to the DLQ before its source offset is eligible to be
    /// committed. This is iterative to avoid recursive async futures.
    async fn bisect_metrics_or_send_to_dlq(&self) -> Result<()> {
        let records: Vec<(i32, MetricRow)> = self
            .batches
            .iter()
            .flat_map(|(partition, batch)| batch.rows.iter().cloned().map(|row| (*partition, row)))
            .collect();
        let mut pending = vec![records];

        while let Some(records) = pending.pop() {
            if records.is_empty() {
                continue;
            }
            let rows: Vec<MetricRow> = records.iter().map(|(_, row)| row.clone()).collect();
            match self.writer.write_metrics(&rows).await {
                Ok(()) => {}
                Err(error) if is_permanent_clickhouse_error(&error) && records.len() == 1 => {
                    let (partition, row) = &records[0];
                    let payload = serde_json::to_vec(row)?;
                    self.dlq
                        .send(
                            &payload,
                            DlqReason::ClickHousePermanent(error.to_string()),
                            *partition,
                            row.kafka_offset as i64,
                        )
                        .await?;
                }
                Err(error) if is_permanent_clickhouse_error(&error) => {
                    let midpoint = records.len() / 2;
                    let (left, right) = records.split_at(midpoint);
                    pending.push(right.to_vec());
                    pending.push(left.to_vec());
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    /// Apply the same source-offset safety rule to alert audit rows. Alert
    /// storage must not make an otherwise valid raw metric unrecoverable.
    async fn bisect_alerts_or_send_to_dlq(&self) -> Result<()> {
        let records: Vec<(i32, AlertRow)> = self
            .batches
            .iter()
            .flat_map(|(partition, batch)| {
                batch
                    .alert_rows
                    .iter()
                    .cloned()
                    .map(|row| (*partition, row))
            })
            .collect();
        let mut pending = vec![records];

        while let Some(records) = pending.pop() {
            if records.is_empty() {
                continue;
            }
            let rows: Vec<AlertRow> = records.iter().map(|(_, row)| row.clone()).collect();
            match self.writer.write_alerts(&rows).await {
                Ok(()) => {}
                Err(error) if is_permanent_clickhouse_error(&error) && records.len() == 1 => {
                    let (partition, row) = &records[0];
                    let payload = serde_json::to_vec(row)?;
                    self.dlq
                        .send(
                            &payload,
                            DlqReason::ClickHousePermanent(error.to_string()),
                            *partition,
                            row.kafka_offset as i64,
                        )
                        .await?;
                }
                Err(error) if is_permanent_clickhouse_error(&error) => {
                    let midpoint = records.len() / 2;
                    let (left, right) = records.split_at(midpoint);
                    pending.push(right.to_vec());
                    pending.push(left.to_vec());
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}
