#[cfg(test)]
mod eval;
pub mod ewma;
pub mod registry;
pub mod zscore;

use crate::model::{AlertSeverity, RawMetric};

/// Result of anomaly detection on a metric value.
#[derive(Debug, Clone)]
pub struct DetectionResult {
    pub is_anomaly: bool,
    pub score: f64,
    pub detector_type: String,
    pub severity: AlertSeverity,
}

/// Trait for anomaly detectors.
pub trait AnomalyDetector: Send + Sync {
    /// Update the detector with a new value and return whether it's anomalous.
    fn detect(&mut self, metric: &RawMetric) -> DetectionResult;

    /// Get the detector type name.
    fn name(&self) -> &str;
}
