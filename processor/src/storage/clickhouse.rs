// ClickHouse batch writer for processed metrics and alerts.
//
// Uses the clickhouse-rs crate for native protocol communication.
// Circuit breaker pattern prevents cascading failures when ClickHouse is degraded.
// Writer now returns Result and never clears data on failure — ownership stays
// with the caller, which is the only thing that can decide not to commit.

use anyhow::{anyhow, Result};
use clickhouse::Client;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

use crate::model::{AlertRow, MetricRow};

// ── Circuit Breaker ──────────────────────────────────────────

/// Circuit breaker states.
const CB_CLOSED: u8 = 0;
const CB_OPEN: u8 = 1;
const CB_HALF_OPEN: u8 = 2;

/// Circuit breaker for ClickHouse write operations.
/// Prevents cascading failures by short-circuiting writes when the
/// backend is degraded, with automatic recovery via half-open probes.
pub struct CircuitBreaker {
    state: AtomicU8,
    failure_count: AtomicU64,
    success_count: AtomicU64,
    failure_threshold: u64,
    success_threshold: u64,
    open_duration_ms: AtomicU64,
    last_failure_epoch_ms: AtomicU64,
    base_backoff_ms: u64,
    max_backoff_ms: u64,
}

impl CircuitBreaker {
    /// Create a new circuit breaker.
    ///
    /// - `failure_threshold`: consecutive failures before opening the circuit.
    /// - `success_threshold`: successes in half-open before closing.
    /// - `base_backoff_ms`: initial backoff when circuit opens.
    /// - `max_backoff_ms`: maximum backoff cap.
    pub fn new(
        failure_threshold: u64,
        success_threshold: u64,
        base_backoff_ms: u64,
        max_backoff_ms: u64,
    ) -> Self {
        CircuitBreaker {
            state: AtomicU8::new(CB_CLOSED),
            failure_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            failure_threshold,
            success_threshold,
            open_duration_ms: AtomicU64::new(base_backoff_ms),
            last_failure_epoch_ms: AtomicU64::new(0),
            base_backoff_ms,
            max_backoff_ms,
        }
    }

    /// Check if a request is allowed to proceed.
    pub fn allow(&self) -> bool {
        match self.state.load(Ordering::SeqCst) {
            CB_CLOSED => true,
            CB_OPEN => {
                // Check if backoff period has elapsed
                let now_ms = now_epoch_ms();
                let last = self.last_failure_epoch_ms.load(Ordering::SeqCst);
                let duration = self.open_duration_ms.load(Ordering::SeqCst);

                if now_ms.saturating_sub(last) >= duration {
                    // Transition to half-open: allow one probe request
                    self.state.store(CB_HALF_OPEN, Ordering::SeqCst);
                    self.success_count.store(0, Ordering::SeqCst);
                    info!("Circuit breaker: OPEN → HALF_OPEN (probing)");
                    true
                } else {
                    false
                }
            }
            CB_HALF_OPEN => true,
            _ => false,
        }
    }

    /// Record a successful operation.
    pub fn record_success(&self) {
        match self.state.load(Ordering::SeqCst) {
            CB_HALF_OPEN => {
                let count = self.success_count.fetch_add(1, Ordering::SeqCst) + 1;
                if count >= self.success_threshold {
                    // Recovered — close the circuit
                    self.state.store(CB_CLOSED, Ordering::SeqCst);
                    self.failure_count.store(0, Ordering::SeqCst);
                    self.open_duration_ms
                        .store(self.base_backoff_ms, Ordering::SeqCst);
                    info!("Circuit breaker: HALF_OPEN → CLOSED (recovered)");
                }
            }
            CB_CLOSED => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::SeqCst);
            }
            _ => {}
        }
    }

    /// Record a failed operation.
    pub fn record_failure(&self) {
        let count = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
        self.last_failure_epoch_ms
            .store(now_epoch_ms(), Ordering::SeqCst);

        match self.state.load(Ordering::SeqCst) {
            CB_CLOSED if count >= self.failure_threshold => {
                // Trip the circuit
                self.state.store(CB_OPEN, Ordering::SeqCst);
                let backoff = self.open_duration_ms.load(Ordering::SeqCst);
                warn!(
                    failures = count,
                    backoff_ms = backoff,
                    "Circuit breaker: CLOSED → OPEN"
                );
            }
            CB_CLOSED => {}
            CB_HALF_OPEN => {
                // Probe failed — go back to open with exponential backoff
                self.state.store(CB_OPEN, Ordering::SeqCst);
                let current = self.open_duration_ms.load(Ordering::SeqCst);
                let new_backoff = (current * 2).min(self.max_backoff_ms);
                self.open_duration_ms.store(new_backoff, Ordering::SeqCst);
                warn!(
                    backoff_ms = new_backoff,
                    "Circuit breaker: HALF_OPEN → OPEN (exponential backoff)"
                );
            }
            _ => {}
        }
    }

    /// Get the current state as a string (for metrics/logging).
    pub fn state_name(&self) -> &'static str {
        match self.state.load(Ordering::SeqCst) {
            CB_CLOSED => "closed",
            CB_OPEN => "open",
            CB_HALF_OPEN => "half_open",
            _ => "unknown",
        }
    }
}

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ── ClickHouse Writer ────────────────────────────────────────

/// ClickHouse writer with circuit breaker protection and retry with backoff.
/// Never drops data. Returns Err only when every attempt failed, in
/// which case the caller MUST NOT commit the offsets.
pub struct ClickHouseWriter {
    client: Client,
    circuit_breaker: CircuitBreaker,
    max_attempts: u32,
    base_retry_ms: u64,
}

impl ClickHouseWriter {
    /// Create a new ClickHouse writer with circuit breaker.
    pub fn new(
        url: &str,
        database: &str,
        username: &str,
        password: &str,
        max_attempts: u32,
    ) -> Result<Self> {
        let client = Client::default()
            .with_url(url)
            .with_database(database)
            .with_user(username)
            .with_password(password);

        let circuit_breaker = CircuitBreaker::new(
            5,      // 5 consecutive failures → open
            3,      // 3 successes in half-open → close
            1_000,  // 1s initial backoff
            60_000, // 60s max backoff
        );

        info!(
            url = url,
            database = database,
            "ClickHouse writer initialized with circuit breaker"
        );

        Ok(ClickHouseWriter {
            client,
            circuit_breaker,
            max_attempts,
            base_retry_ms: 200,
        })
    }

    /// Persist rows, retrying with exponential backoff and jitter.
    /// Never drops data. Returns Err only when every attempt failed, in
    /// which case the caller MUST NOT commit the offsets.
    pub async fn write_metrics(&self, rows: &[MetricRow]) -> Result<()> {
        let mut attempt = 0u32;

        loop {
            attempt += 1;

            if !self.circuit_breaker.allow() {
                // Open circuit: do not delete, do not hammer. Sleep and let
                // the consumer stall. Kafka is the buffer; lag is the signal.
                crate::metrics::CIRCUIT_STATE.set(1.0);
                warn!(rows = rows.len(), "circuit open — pausing consumer");
                tokio::time::sleep(Duration::from_millis(500)).await;
                if attempt >= self.max_attempts {
                    return Err(anyhow!("circuit open after {attempt} attempts"));
                }
                continue;
            }

            let started = Instant::now();
            match self.insert_metrics(rows).await {
                Ok(()) => {
                    self.circuit_breaker.record_success();
                    crate::metrics::CIRCUIT_STATE.set(0.0);
                    crate::metrics::CLICKHOUSE_WRITE_DURATION
                        .observe(started.elapsed().as_secs_f64());
                    crate::metrics::CLICKHOUSE_BATCH_SIZE.set(rows.len() as f64);
                    info!(rows = rows.len(), attempt, "flushed to clickhouse");
                    return Ok(());
                }
                Err(e) => {
                    self.circuit_breaker.record_failure();
                    crate::metrics::CLICKHOUSE_WRITE_ERRORS.inc();

                    if attempt >= self.max_attempts {
                        error!(
                            error = %e,
                            rows = rows.len(),
                            attempt,
                            "write failed permanently — offsets NOT committed"
                        );
                        return Err(e);
                    }

                    // Exponential backoff with full jitter.
                    let ceiling = self.base_retry_ms.saturating_mul(1u64 << attempt.min(6));
                    let delay = fastrand::u64(0..=ceiling.max(1));
                    warn!(error = %e, attempt, delay_ms = delay, "write failed — retrying");
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
            }
        }
    }

    /// Write alert rows with the same retry semantics.
    pub async fn write_alerts(&self, rows: &[AlertRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut attempt = 0u32;

        loop {
            attempt += 1;
            if !self.circuit_breaker.allow() {
                crate::metrics::CIRCUIT_STATE.set(1.0);
                tokio::time::sleep(Duration::from_millis(500)).await;
                if attempt >= self.max_attempts {
                    return Err(anyhow!("circuit open after {attempt} attempts for alerts"));
                }
                continue;
            }

            match self.insert_alerts(rows).await {
                Ok(()) => {
                    self.circuit_breaker.record_success();
                    crate::metrics::CIRCUIT_STATE.set(0.0);
                    info!(count = rows.len(), "flushed alerts to ClickHouse");
                    return Ok(());
                }
                Err(e) => {
                    self.circuit_breaker.record_failure();
                    if attempt >= self.max_attempts {
                        error!(error = %e, "alert write failed permanently");
                        return Err(e);
                    }
                    let ceiling = self.base_retry_ms.saturating_mul(1u64 << attempt.min(6));
                    let delay = fastrand::u64(0..=ceiling.max(1));
                    warn!(error = %e, attempt, delay_ms = delay, "alert write failed — retrying");
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
            }
        }
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

    /// True readiness: can we reach the table we actually write to?
    pub async fn health_check(&self) -> bool {
        self.client
            .query("SELECT 1 FROM metrics LIMIT 0")
            .execute()
            .await
            .is_ok()
    }

    /// Get the current circuit breaker state name.
    pub fn circuit_state(&self) -> &'static str {
        self.circuit_breaker.state_name()
    }
}

// ── Circuit Breaker Tests ────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_starts_closed() {
        let cb = CircuitBreaker::new(3, 2, 1000, 60000);
        assert_eq!(cb.state_name(), "closed");
        assert!(cb.allow());
    }

    #[test]
    fn test_circuit_breaker_opens_after_threshold() {
        let cb = CircuitBreaker::new(3, 2, 1000, 60000);

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state_name(), "closed");

        cb.record_failure(); // 3rd failure — trips the circuit
        assert_eq!(cb.state_name(), "open");
        assert!(!cb.allow()); // Should deny requests
    }

    #[test]
    fn test_circuit_breaker_success_resets_failures() {
        let cb = CircuitBreaker::new(3, 2, 1000, 60000);

        cb.record_failure();
        cb.record_failure();
        cb.record_success(); // Reset failure count

        // Should still be closed (failures reset)
        cb.record_failure();
        assert_eq!(cb.state_name(), "closed");
    }

    #[test]
    fn test_circuit_breaker_half_open_recovers() {
        let cb = CircuitBreaker::new(3, 2, 0, 60000); // 0ms backoff for test

        // Trip the circuit
        cb.record_failure();
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state_name(), "open");

        // With 0ms backoff, allow() should transition to half-open
        assert!(cb.allow());
        assert_eq!(cb.state_name(), "half_open");

        // Successes in half-open → closed
        cb.record_success();
        cb.record_success(); // 2nd success meets threshold
        assert_eq!(cb.state_name(), "closed");
    }

    #[test]
    fn test_circuit_breaker_half_open_failure_reopens() {
        let cb = CircuitBreaker::new(3, 2, 0, 60000);

        // Trip and transition to half-open
        for _ in 0..3 {
            cb.record_failure();
        }
        cb.allow(); // → half_open

        // Failure in half-open → back to open with increased backoff
        cb.record_failure();
        assert_eq!(cb.state_name(), "open");
    }
}
