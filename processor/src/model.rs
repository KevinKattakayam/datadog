// Enterprise Observability Pipeline — Data Models
// Core data structures shared across the Rust processor.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Raw metric as received from Kafka (published by Go ingestor).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawMetric {
    pub name: String,
    pub value: f64,
    #[serde(default)]
    pub unit: String,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    #[serde(alias = "ts")]
    pub timestamp: i64,
    pub host: String,
    #[serde(default)]
    pub tenant_id: Option<String>,
}

/// Processed metric with anomaly detection results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedMetric {
    pub name: String,
    pub value: f64,
    pub unit: String,
    pub tags: HashMap<String, String>,
    pub timestamp: i64,
    pub host: String,
    #[serde(default)]
    pub tenant_id: Option<String>,
    pub anomaly_score: f64,
    pub is_anomaly: bool,
    pub detector_type: String,
}

/// Alert fired when an anomaly is detected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alert {
    pub timestamp: i64,
    pub metric_name: String,
    pub host: String,
    #[serde(default)]
    pub tenant_id: Option<String>,
    pub value: f64,
    pub anomaly_score: f64,
    pub detector_type: String,
    pub severity: AlertSeverity,
    pub tags: HashMap<String, String>,
}

/// Alert severity levels.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AlertSeverity {
    Warning,
    Critical,
}

impl AlertSeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            AlertSeverity::Warning => "warning",
            AlertSeverity::Critical => "critical",
        }
    }
}

impl std::fmt::Display for AlertSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// ClickHouse row for batch insertion.
#[derive(Debug, Clone, Serialize, clickhouse::Row)]
pub struct MetricRow {
    pub ts: i64,
    pub tenant_id: String,
    pub name: String,
    pub host: String,
    pub value: f64,
    pub unit: String,
    pub tags: Vec<(String, String)>,
    pub anomaly_score: f64,
    pub is_anomaly: u8,
    pub kafka_partition: u16,
    pub kafka_offset: u64,
}

impl MetricRow {
    pub fn from_processed(m: &ProcessedMetric, partition: i32, offset: i64) -> Self {
        let tags: Vec<(String, String)> =
            m.tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

        MetricRow {
            ts: m.timestamp * 1000, // Convert to milliseconds for DateTime64(3)
            tenant_id: m.tenant_id.clone().unwrap_or_else(|| "default".to_string()),
            name: m.name.clone(),
            host: m.host.clone(),
            value: m.value,
            unit: m.unit.clone(),
            tags,
            anomaly_score: m.anomaly_score,
            is_anomaly: if m.is_anomaly { 1 } else { 0 },
            kafka_partition: partition as u16,
            kafka_offset: offset as u64,
        }
    }
}

/// ClickHouse row for alert audit log.
#[derive(Debug, Clone, Serialize, clickhouse::Row)]
pub struct AlertRow {
    pub ts: i64,
    pub tenant_id: String,
    pub metric_name: String,
    pub host: String,
    pub value: f64,
    pub anomaly_score: f64,
    pub detector_type: String,
    pub severity: String,
    pub tags: Vec<(String, String)>,
    pub kafka_partition: u16,
    pub kafka_offset: u64,
}

impl AlertRow {
    pub fn from_alert(a: &Alert, partition: i32, offset: i64) -> Self {
        let tags: Vec<(String, String)> =
            a.tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

        AlertRow {
            ts: a.timestamp * 1000,
            tenant_id: a.tenant_id.clone().unwrap_or_else(|| "default".to_string()),
            metric_name: a.metric_name.clone(),
            host: a.host.clone(),
            value: a.value,
            anomaly_score: a.anomaly_score,
            detector_type: a.detector_type.clone(),
            severity: a.severity.to_string(),
            tags,
            kafka_partition: partition as u16,
            kafka_offset: offset as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alert_row_keeps_source_kafka_coordinates() {
        let alert = Alert {
            timestamp: 1_700_000_000,
            metric_name: "cpu.usage".to_string(),
            host: "host-a".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            value: 99.0,
            anomaly_score: 4.0,
            detector_type: "ewma".to_string(),
            severity: AlertSeverity::Critical,
            tags: HashMap::new(),
        };

        let row = AlertRow::from_alert(&alert, 3, 42);
        assert_eq!(row.kafka_partition, 3);
        assert_eq!(row.kafka_offset, 42);
    }
}
