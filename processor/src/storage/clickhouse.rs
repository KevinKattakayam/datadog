// ClickHouse batch writer for processed metrics and alerts.
//
// Uses the clickhouse-rs crate for native protocol communication.
// Circuit breaker pattern prevents cascading failures when ClickHouse is degraded.
// Writer now returns Result and never clears data on failure — ownership stays
// with the caller, which is the only thing that can decide not to commit.

use anyhow::{anyhow, Result};
use clickhouse::error::Error as ChError;
use clickhouse::Client;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

use crate::model::{AlertRow, MetricRow};

/// ClickHouse exception codes that mean "this exact input can never be
/// inserted": parse/type/range failures on a row's data. Everything else —
/// TOO_MANY_PARTS (252), MEMORY_LIMIT_EXCEEDED (241), TABLE_IS_READ_ONLY
/// (242), TIMEOUT_EXCEEDED (159), UNKNOWN_TABLE (60), missing columns (16/47),
/// AUTHENTICATION_FAILED (516), ACCESS_DENIED (497) — is an operational state
/// that heals or is fixed by an operator. Treating those as permanent would
/// bisect the batch and route every healthy row to the DLQ.
///
/// The list is deliberately an allowlist: an unknown code retries, because a
/// wrong "retry" costs latency while a wrong "permanent" costs customer data.
pub const PERMANENT_EXCEPTION_CODES: &[u32] = &[
    6,   // CANNOT_PARSE_TEXT
    26,  // CANNOT_PARSE_QUOTED_STRING
    27,  // CANNOT_PARSE_INPUT_ASSERTION_FAILED
    38,  // CANNOT_PARSE_DATE
    41,  // CANNOT_PARSE_DATETIME
    43,  // ILLEGAL_TYPE_OF_ARGUMENT
    53,  // TYPE_MISMATCH
    69,  // ARGUMENT_OUT_OF_BOUND
    70,  // CANNOT_CONVERT_TYPE
    72,  // CANNOT_PARSE_NUMBER
    117, // INCORRECT_DATA
    131, // TOO_LARGE_STRING_SIZE
    321, // VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE
    349, // CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN
    469, // VIOLATED_CONSTRAINT
];

/// Classify a writer error as permanent (isolate the row, DLQ it) or
/// retryable (leave offsets uncommitted, retry, let Kafka buffer).
pub fn is_permanent_clickhouse_error(error: &anyhow::Error) -> bool {
    match error.downcast_ref::<ChError>() {
        // Client-side serialisation of a single row is deterministic.
        Some(ChError::SequenceMustHaveLength)
        | Some(ChError::InvalidUtf8Encoding(_))
        | Some(ChError::Custom(_)) => true,
        Some(ChError::BadResponse(message)) => exception_code(message)
            .map(|code| PERMANENT_EXCEPTION_CODES.contains(&code))
            .unwrap_or(false),
        // Network, timeout, compression, circuit-open, or anything unknown.
        _ => false,
    }
}

/// Extract `N` from ClickHouse's `Code: N. DB::Exception: ...` body.
pub fn exception_code(message: &str) -> Option<u32> {
    let start = message.find("Code: ")? + "Code: ".len();
    let rest = &message[start..];
    let end = rest.find(|c: char| !c.is_ascii_digit())?;
    rest[..end].parse().ok()
}

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
    /// Upper bound on one INSERT round-trip. Without it a half-open TCP
    /// connection parks the consumer loop forever while lag grows silently.
    request_timeout: Duration,
}

impl ClickHouseWriter {
    /// Create a new ClickHouse writer with circuit breaker.
    pub fn new(
        url: &str,
        database: &str,
        username: &str,
        password: &str,
        max_attempts: u32,
        request_timeout: Duration,
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
            request_timeout,
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

    /// Insert metric rows, bounded by the request timeout.
    async fn insert_metrics(&self, rows: &[MetricRow]) -> Result<()> {
        self.bounded("metrics", async {
            let mut insert = self.client.insert("metrics")?;
            for row in rows {
                insert.write(row).await?;
            }
            insert.end().await?;
            Ok(())
        })
        .await
    }

    /// Insert alert rows, bounded by the request timeout.
    async fn insert_alerts(&self, rows: &[AlertRow]) -> Result<()> {
        self.bounded("alerts", async {
            let mut insert = self.client.insert("alerts")?;
            for row in rows {
                insert.write(row).await?;
            }
            insert.end().await?;
            Ok(())
        })
        .await
    }

    /// Run one ClickHouse round-trip with a deadline. A timeout is surfaced as
    /// `ChError::TimedOut`, which the classifier treats as retryable. Dropping
    /// the future aborts the HTTP request, so ClickHouse never commits a
    /// half-sent block; if it did commit, the replay deduplicates on the
    /// Kafka coordinates in the sort key.
    async fn bounded<F>(&self, table: &str, op: F) -> Result<()>
    where
        F: std::future::Future<Output = Result<()>>,
    {
        match tokio::time::timeout(self.request_timeout, op).await {
            Ok(result) => result,
            Err(_) => {
                crate::metrics::CLICKHOUSE_TIMEOUTS.inc();
                warn!(
                    table,
                    timeout_ms = self.request_timeout.as_millis() as u64,
                    "clickhouse insert timed out"
                );
                Err(anyhow::Error::new(ChError::TimedOut))
            }
        }
    }

    /// True readiness: can we reach the table we actually write to? Bounded
    /// so a stuck ClickHouse makes the probe fail instead of hang.
    pub async fn health_check(&self) -> bool {
        let probe = self.client.query("SELECT 1 FROM metrics LIMIT 0").execute();
        matches!(
            tokio::time::timeout(Duration::from_secs(2), probe).await,
            Ok(Ok(()))
        )
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
    use crate::model::MetricRow;

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

    fn bad_response(body: &str) -> anyhow::Error {
        anyhow::Error::new(ChError::BadResponse(body.to_string()))
    }

    #[test]
    fn parses_exception_code() {
        assert_eq!(
            exception_code("Code: 252. DB::Exception: Too many parts"),
            Some(252)
        );
        assert_eq!(
            exception_code("bad response: Code: 6. DB::Exception: x"),
            Some(6)
        );
        assert_eq!(exception_code("no code here"), None);
    }

    #[test]
    fn row_data_rejection_is_permanent() {
        let e = bad_response("Code: 27. DB::Exception: Cannot parse input: expected '\"'");
        assert!(is_permanent_clickhouse_error(&e));
    }

    #[test]
    fn overload_is_retryable_not_dlq() {
        // The bug this guards: every DB::Exception used to count as permanent,
        // so a merge backlog sent the whole batch to the DLQ row by row.
        for body in [
            "Code: 252. DB::Exception: Too many parts (300). Merges are processing significantly slower than inserts. (TOO_MANY_PARTS)",
            "Code: 241. DB::Exception: Memory limit (total) exceeded. (MEMORY_LIMIT_EXCEEDED)",
            "Code: 242. DB::Exception: Table is in readonly mode. (TABLE_IS_READ_ONLY)",
            "Code: 159. DB::Exception: Timeout exceeded. (TIMEOUT_EXCEEDED)",
        ] {
            assert!(!is_permanent_clickhouse_error(&bad_response(body)), "{body}");
        }
    }

    #[test]
    fn schema_and_auth_problems_are_retryable() {
        // An operator fixes these with a migration or a Secret; the rows are fine.
        for body in [
            "Code: 47. DB::Exception: Unknown column tenant_id",
            "Code: 16. DB::Exception: No such column tenant_id in table",
            "Code: 60. DB::Exception: Table observability.metrics does not exist. (UNKNOWN_TABLE)",
            "Code: 516. DB::Exception: processor: Authentication failed. (AUTHENTICATION_FAILED)",
            "Code: 497. DB::Exception: processor: Not enough privileges. (ACCESS_DENIED)",
        ] {
            assert!(
                !is_permanent_clickhouse_error(&bad_response(body)),
                "{body}"
            );
        }
    }

    #[test]
    fn transport_and_unknown_errors_are_retryable() {
        assert!(!is_permanent_clickhouse_error(&anyhow::Error::new(
            ChError::TimedOut
        )));
        assert!(!is_permanent_clickhouse_error(&anyhow!(
            "circuit open after 5 attempts"
        )));
        assert!(!is_permanent_clickhouse_error(&anyhow!(
            "invalid something, but not a ClickHouse error"
        )));
        assert!(!is_permanent_clickhouse_error(&bad_response(
            "HTTP 502 Bad Gateway"
        )));
    }

    #[test]
    fn client_side_serialisation_failure_is_permanent() {
        let e = anyhow::Error::new(ChError::Custom("bad row".into()));
        assert!(is_permanent_clickhouse_error(&e));
    }

    fn sample_row() -> MetricRow {
        MetricRow {
            ts: 1_700_000_000_000,
            tenant_id: "tenant-a".to_string(),
            name: "test.metric".to_string(),
            host: "host-a".to_string(),
            value: 1.0,
            unit: "count".to_string(),
            tags: vec![],
            anomaly_score: 0.0,
            is_anomaly: 0,
            kafka_partition: 1,
            kafka_offset: 42,
        }
    }

    #[tokio::test]
    async fn silent_server_times_out_instead_of_hanging() {
        // Accept TCP connections and never answer: the half-open case.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                held.push(sock);
            }
        });
        let writer = ClickHouseWriter::new(
            &format!("http://{addr}"),
            "observability",
            "default",
            "",
            1,
            Duration::from_millis(200),
        )
        .unwrap();
        let started = Instant::now();
        let err = writer.write_metrics(&[sample_row()]).await.unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(5), "writer hung");
        assert!(
            !is_permanent_clickhouse_error(&err),
            "timeout must be retryable"
        );
    }

    #[tokio::test]
    async fn write_metrics_returns_error_after_configured_attempts_and_keeps_rows() {
        // Port 1 has no listener in the test environment. The caller owns the
        // slice, so a failed writer must return an error without mutating it.
        let writer = ClickHouseWriter::new(
            "http://127.0.0.1:1",
            "observability",
            "default",
            "",
            2,
            Duration::from_secs(2),
        )
        .unwrap();
        let rows = vec![sample_row()];
        let before = rows.clone();

        assert!(writer.write_metrics(&rows).await.is_err());
        assert_eq!(rows.len(), before.len());
        assert_eq!(rows[0].kafka_offset, before[0].kafka_offset);
    }
}
