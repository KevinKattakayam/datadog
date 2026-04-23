// ClickHouse batch writer for processed metrics and alerts.
//
// Uses the clickhouse-rs crate for native protocol communication.
// Implements batched inserts with configurable batch size and flush interval.

use anyhow::Result;
use clickhouse::Client;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{error, info, warn};

use crate::model::{AlertRow, MetricRow};

/// ClickHouse writer that batches inserts for efficiency.
pub struct ClickHouseWriter {
    client: Client,
    batch_size: usize,
    flush_interval: Duration,
}

impl ClickHouseWriter {
    /// Create a new ClickHouse writer.
    pub fn new(url: &str, batch_size: usize, flush_interval_ms: u64) -> Result<Self> {
        let client = Client::default()
            .with_url(url)
            .with_database("observability");

        info!(url = url, batch_size = batch_size, "ClickHouse writer initialized");

        Ok(ClickHouseWriter {
            client,
            batch_size,
            flush_interval: Duration::from_millis(flush_interval_ms),
        })
    }

    /// Start the metric writer loop. Receives metrics via channel and batch-inserts them.
    pub async fn run_metric_writer(
        &self,
        mut rx: mpsc::Receiver<MetricRow>,
    ) {
        let mut batch: Vec<MetricRow> = Vec::with_capacity(self.batch_size);
        let mut interval = time::interval(self.flush_interval);

        loop {
            tokio::select! {
                Some(row) = rx.recv() => {
                    batch.push(row);
                    if batch.len() >= self.batch_size {
                        self.flush_metrics(&mut batch).await;
                    }
                }
                _ = interval.tick() => {
                    if !batch.is_empty() {
                        self.flush_metrics(&mut batch).await;
                    }
                }
            }
        }
    }

    /// Start the alert writer loop.
    pub async fn run_alert_writer(
        &self,
        mut rx: mpsc::Receiver<AlertRow>,
    ) {
        let mut batch: Vec<AlertRow> = Vec::with_capacity(100);
        let mut interval = time::interval(self.flush_interval);

        loop {
            tokio::select! {
                Some(row) = rx.recv() => {
                    batch.push(row);
                    if batch.len() >= 100 {
                        self.flush_alerts(&mut batch).await;
                    }
                }
                _ = interval.tick() => {
                    if !batch.is_empty() {
                        self.flush_alerts(&mut batch).await;
                    }
                }
            }
        }
    }

    /// Flush a batch of metric rows to ClickHouse.
    async fn flush_metrics(&self, batch: &mut Vec<MetricRow>) {
        let count = batch.len();
        let start = std::time::Instant::now();

        match self.insert_metrics(batch).await {
            Ok(_) => {
                let elapsed = start.elapsed();
                info!(
                    count = count,
                    duration_ms = elapsed.as_millis(),
                    "Flushed metrics to ClickHouse"
                );
                crate::metrics::CLICKHOUSE_WRITE_DURATION
                    .observe(elapsed.as_secs_f64());
                crate::metrics::CLICKHOUSE_BATCH_SIZE.set(count as f64);
            }
            Err(e) => {
                error!(error = %e, count = count, "Failed to flush metrics to ClickHouse");
                crate::metrics::CLICKHOUSE_WRITE_ERRORS.inc();
            }
        }

        batch.clear();
    }

    /// Flush a batch of alert rows to ClickHouse.
    async fn flush_alerts(&self, batch: &mut Vec<AlertRow>) {
        let count = batch.len();
        match self.insert_alerts(batch).await {
            Ok(_) => info!(count = count, "Flushed alerts to ClickHouse"),
            Err(e) => error!(error = %e, count = count, "Failed to flush alerts to ClickHouse"),
        }
        batch.clear();
    }

    /// Insert metric rows using ClickHouse inserter.
    async fn insert_metrics(&self, rows: &[MetricRow]) -> Result<()> {
        let mut insert = self.client.insert("metrics")?;
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }

    /// Insert alert rows using ClickHouse inserter.
    async fn insert_alerts(&self, rows: &[AlertRow]) -> Result<()> {
        let mut insert = self.client.insert("alerts")?;
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }

    /// Check if ClickHouse is reachable.
    pub async fn health_check(&self) -> bool {
        self.client
            .query("SELECT 1")
            .execute()
            .await
            .is_ok()
    }
}
