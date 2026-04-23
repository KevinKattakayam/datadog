// EWMA (Exponentially Weighted Moving Average) Anomaly Detector
//
// Tracks the exponentially weighted mean and variance of a metric stream.
// Flags values as anomalous when they deviate beyond a configurable number
// of standard deviations from the EWMA.

use super::{AnomalyDetector, DetectionResult};
use crate::model::{AlertSeverity, RawMetric};

/// EWMA-based anomaly detector with configurable sensitivity.
#[derive(Debug, Clone)]
pub struct EwmaDetector {
    /// Smoothing factor (0 < alpha < 1). Higher = more reactive.
    alpha: f64,
    /// Current exponentially weighted moving average.
    ewma: f64,
    /// Current exponentially weighted variance.
    variance: f64,
    /// Number of standard deviations to trigger anomaly.
    threshold_sigmas: f64,
    /// Number of samples seen (used for warm-up).
    count: u64,
    /// Minimum samples before detection activates.
    min_samples: u64,
}

impl EwmaDetector {
    /// Create a new EWMA detector.
    ///
    /// # Arguments
    /// * `alpha` - Smoothing factor (0.0 to 1.0). 0.3 is a good default.
    /// * `threshold_sigmas` - Standard deviations for anomaly threshold. 3.0 is common.
    pub fn new(alpha: f64, threshold_sigmas: f64) -> Self {
        EwmaDetector {
            alpha: alpha.clamp(0.01, 0.99),
            ewma: 0.0,
            variance: 0.0,
            threshold_sigmas,
            count: 0,
            min_samples: 10,
        }
    }

    /// Update the EWMA with a new value and return whether it's anomalous.
    pub fn update(&mut self, value: f64) -> (bool, f64) {
        self.count += 1;

        if self.count == 1 {
            // Initialize with first value
            self.ewma = value;
            self.variance = 0.0;
            return (false, 0.0);
        }

        // Compute deviation BEFORE updating — this is the standard approach
        let diff = value - self.ewma;

        // Compute z-score before updating (measures surprise)
        let std_dev = self.variance.sqrt();
        let z_score = if std_dev > 1e-10 {
            diff.abs() / std_dev
        } else {
            0.0
        };

        // Now update EWMA and variance
        self.ewma += self.alpha * diff;
        self.variance = (1.0 - self.alpha) * (self.variance + self.alpha * diff * diff);

        // Don't flag anomalies during warm-up period
        if self.count < self.min_samples {
            return (false, 0.0);
        }

        let is_anomaly = z_score > self.threshold_sigmas;

        (is_anomaly, z_score)
    }
}

impl AnomalyDetector for EwmaDetector {
    fn detect(&mut self, metric: &RawMetric) -> DetectionResult {
        let (is_anomaly, score) = self.update(metric.value);

        let severity = if score > self.threshold_sigmas * 1.5 {
            AlertSeverity::Critical
        } else {
            AlertSeverity::Warning
        };

        DetectionResult {
            is_anomaly,
            score,
            detector_type: self.name().to_string(),
            severity,
        }
    }

    fn name(&self) -> &str {
        "ewma"
    }

    fn reset(&mut self) {
        self.ewma = 0.0;
        self.variance = 0.0;
        self.count = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ewma_normal_values() {
        let mut detector = EwmaDetector::new(0.3, 3.0);

        // Feed normal values
        for i in 0..20 {
            let value = 100.0 + (i as f64 % 5.0);
            let (is_anomaly, _) = detector.update(value);
            if detector.count > 10 {
                assert!(!is_anomaly, "normal value flagged as anomaly");
            }
        }
    }

    #[test]
    fn test_ewma_detects_spike() {
        let mut detector = EwmaDetector::new(0.3, 3.0);

        // Establish baseline with slight variance (realistic data)
        for i in 0..50 {
            let value = 100.0 + (i as f64 % 3.0) - 1.0; // values between 99 and 101
            detector.update(value);
        }

        // Inject a massive spike — 10x the normal range
        let (is_anomaly, score) = detector.update(1000.0);
        assert!(is_anomaly, "spike should be detected as anomaly, score={}", score);
        assert!(score > 3.0, "score should exceed threshold, got {}", score);
    }

    #[test]
    fn test_ewma_warmup_period() {
        let mut detector = EwmaDetector::new(0.3, 3.0);

        // During warm-up, nothing should be flagged
        for _ in 0..9 {
            let (is_anomaly, _) = detector.update(1000.0);
            assert!(!is_anomaly, "should not flag during warm-up");
        }
    }
}
