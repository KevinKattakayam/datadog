// Rolling Z-Score Anomaly Detector
//
// Maintains a sliding window of recent values and computes the Z-score
// of each new value against the window's mean and standard deviation.
// More robust than EWMA for non-stationary signals with regime changes.

use std::collections::VecDeque;

use super::{AnomalyDetector, DetectionResult};
use crate::model::{AlertSeverity, RawMetric};

/// Rolling window Z-score anomaly detector.
#[derive(Debug, Clone)]
pub struct ZScoreDetector {
    /// Rolling window of recent values.
    window: VecDeque<f64>,
    /// Maximum window size (number of samples).
    window_size: usize,
    /// Z-score threshold for anomaly detection.
    threshold: f64,
    /// Running sum for efficient mean calculation.
    sum: f64,
    /// Running sum of squares for efficient variance calculation.
    sum_sq: f64,
}

impl ZScoreDetector {
    /// Create a new Z-score detector.
    ///
    /// # Arguments
    /// * `window_size` - Number of samples in the rolling window. 300 (5 min at 1/sec) is typical.
    /// * `threshold` - Z-score threshold for anomaly detection. 3.0 is standard.
    pub fn new(window_size: usize, threshold: f64) -> Self {
        ZScoreDetector {
            window: VecDeque::with_capacity(window_size),
            window_size,
            threshold,
            sum: 0.0,
            sum_sq: 0.0,
        }
    }

    /// Compute the Z-score of a new value against the current window.
    pub fn update(&mut self, value: f64) -> (bool, f64) {
        // Add new value
        self.sum += value;
        self.sum_sq += value * value;
        self.window.push_back(value);

        // Remove oldest if window is full
        if self.window.len() > self.window_size {
            if let Some(old) = self.window.pop_front() {
                self.sum -= old;
                self.sum_sq -= old * old;
            }
        }

        let n = self.window.len() as f64;

        // Need at least 10 samples for meaningful statistics
        if self.window.len() < 10 {
            return (false, 0.0);
        }

        let mean = self.sum / n;
        let variance = (self.sum_sq / n) - (mean * mean);

        // Guard against negative variance from floating point errors
        if variance < 1e-10 {
            return (false, 0.0);
        }

        let std_dev = variance.sqrt();
        let z_score = (value - mean).abs() / std_dev;

        (z_score > self.threshold, z_score)
    }

    /// Get the current window size.
    pub fn current_window_size(&self) -> usize {
        self.window.len()
    }
}

impl AnomalyDetector for ZScoreDetector {
    fn detect(&mut self, metric: &RawMetric) -> DetectionResult {
        let (is_anomaly, score) = self.update(metric.value);

        let severity = if score > self.threshold * 1.5 {
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
        "zscore"
    }

    fn reset(&mut self) {
        self.window.clear();
        self.sum = 0.0;
        self.sum_sq = 0.0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zscore_normal_values() {
        let mut detector = ZScoreDetector::new(100, 3.0);

        // Feed stable values
        for _ in 0..50 {
            let (is_anomaly, _) = detector.update(100.0);
            assert!(!is_anomaly);
        }
    }

    #[test]
    fn test_zscore_detects_outlier() {
        let mut detector = ZScoreDetector::new(100, 3.0);

        // Establish baseline
        for _ in 0..100 {
            detector.update(100.0);
        }

        // Inject outlier
        let (is_anomaly, score) = detector.update(500.0);
        assert!(is_anomaly, "outlier should be detected");
        assert!(score > 3.0, "score should exceed threshold, got {}", score);
    }

    #[test]
    fn test_zscore_window_slides() {
        let mut detector = ZScoreDetector::new(20, 3.0);

        // Fill with 100s
        for _ in 0..20 {
            detector.update(100.0);
        }

        // Gradually shift to 200s — should adapt
        for _ in 0..40 {
            detector.update(200.0);
        }

        // After adaptation, 200 should be normal
        let (is_anomaly, _) = detector.update(200.0);
        assert!(!is_anomaly, "after adaptation, 200 should be normal");
    }

    #[test]
    fn test_zscore_warmup() {
        let mut detector = ZScoreDetector::new(100, 3.0);

        // First 9 values should never flag
        for _ in 0..9 {
            let (is_anomaly, _) = detector.update(999999.0);
            assert!(!is_anomaly, "should not flag during warm-up");
        }
    }
}
