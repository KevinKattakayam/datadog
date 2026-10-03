// Rolling Z-Score Anomaly Detector
//
// Maintains a sliding window of recent values and scores each new value
// against the window's mean and standard deviation.
//
// Three properties the previous version lacked, each covered by a test:
//
// 1. Score BEFORE insert. Including the candidate in its own baseline caps the
//    reachable score at (n-1)/sqrt(n): 2.85 for the 10-sample minimum, below
//    the default 3.0 threshold, so a small window could never fire.
// 2. Welford's algorithm, not sum/sum-of-squares. `sum_sq/n - mean^2`
//    subtracts two nearly equal numbers; for a byte counter near 1e12 the
//    result is rounding noise and can go negative.
// 3. Flat-baseline fallback. A perfectly constant series has zero variance,
//    and a move away from it is the strongest possible signal, not "score 0".

use std::collections::VecDeque;

use super::{AnomalyDetector, DetectionResult};
use crate::model::{AlertSeverity, RawMetric};

/// Minimum samples before the window statistics are trusted.
const MIN_SAMPLES: usize = 10;
/// Relative move off a flat baseline that counts as anomalous.
const FLAT_REL_TOLERANCE: f64 = 0.10;

/// Rolling window Z-score anomaly detector.
#[derive(Debug, Clone)]
pub struct ZScoreDetector {
    window: VecDeque<f64>,
    window_size: usize,
    threshold: f64,
    /// Running mean of `window`.
    mean: f64,
    /// Running sum of squared deviations from `mean` (Welford's M2).
    m2: f64,
    /// Inserts since the last exact recomputation; bounds float drift.
    since_resync: usize,
}

impl ZScoreDetector {
    /// Create a new Z-score detector.
    ///
    /// * `window_size` - samples in the rolling window (min 10). 300 is 5 min at 1/s.
    /// * `threshold` - Z-score threshold for anomaly detection. 3.0 is standard.
    pub fn new(window_size: usize, threshold: f64) -> Self {
        let window_size = window_size.max(MIN_SAMPLES);
        ZScoreDetector {
            window: VecDeque::with_capacity(window_size + 1),
            window_size,
            threshold,
            mean: 0.0,
            m2: 0.0,
            since_resync: 0,
        }
    }

    /// Score `value` against the current window, then add it to the window.
    pub fn update(&mut self, value: f64) -> (bool, f64) {
        if !value.is_finite() {
            crate::metrics::NON_FINITE_VALUES.inc();
            return (false, 0.0);
        }

        let score = self.score(value);
        self.insert(value);

        match score {
            Some(z) => (z > self.threshold, z),
            None => (false, 0.0),
        }
    }

    /// Z-score of `value` against the window as it stands, or None in warm-up.
    fn score(&self, value: f64) -> Option<f64> {
        let n = self.window.len();
        if n < MIN_SAMPLES {
            return None;
        }
        let variance = (self.m2 / n as f64).max(0.0);
        let std_dev = variance.sqrt();
        let diff = (value - self.mean).abs();
        let scale = self.mean.abs().max(1.0);

        // "Zero" relative to the series' own magnitude. f64 resolves about
        // 2e-16 of a value, so 1e-12 of the level sits far above rounding
        // noise yet far below any real jitter (at 1e12 that is sigma = 1.0).
        if std_dev > 1e-12 * scale {
            Some(diff / std_dev)
        } else if diff / scale > FLAT_REL_TOLERANCE {
            // Saturate rather than return infinity, so severity stays defined.
            Some(self.threshold * 2.0)
        } else {
            Some(0.0)
        }
    }

    fn insert(&mut self, value: f64) {
        if self.window.len() < self.window_size {
            // Growing phase: standard Welford update.
            self.window.push_back(value);
            let n = self.window.len() as f64;
            let delta = value - self.mean;
            self.mean += delta / n;
            self.m2 += delta * (value - self.mean);
        } else {
            // Full window: replace the oldest value in O(1).
            let old = self.window.pop_front().unwrap_or(value);
            self.window.push_back(value);
            let n = self.window.len() as f64;
            let old_mean = self.mean;
            self.mean = old_mean + (value - old) / n;
            self.m2 += (value - old) * (value - self.mean + old - old_mean);
            if self.m2 < 0.0 {
                self.m2 = 0.0;
            }
        }

        // Every `window_size` inserts, recompute exactly. Amortised O(1) and it
        // stops sliding-window rounding error from accumulating for days.
        self.since_resync += 1;
        if self.since_resync >= self.window_size {
            self.resync();
        }
    }

    fn resync(&mut self) {
        let n = self.window.len() as f64;
        if n == 0.0 {
            self.mean = 0.0;
            self.m2 = 0.0;
        } else {
            self.mean = self.window.iter().sum::<f64>() / n;
            self.m2 = self.window.iter().map(|v| (v - self.mean).powi(2)).sum();
        }
        self.since_resync = 0;
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exact_std(values: &VecDeque<f64>) -> f64 {
        let n = values.len() as f64;
        let mean = values.iter().sum::<f64>() / n;
        (values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n).sqrt()
    }

    #[test]
    fn test_zscore_normal_values() {
        let mut detector = ZScoreDetector::new(100, 3.0);
        for i in 0..200 {
            let (is_anomaly, _) = detector.update(100.0 + (i % 5) as f64);
            assert!(!is_anomaly, "normal jitter flagged at i={i}");
        }
    }

    #[test]
    fn test_zscore_detects_outlier() {
        let mut detector = ZScoreDetector::new(100, 3.0);
        for i in 0..100 {
            detector.update(100.0 + (i % 3) as f64);
        }
        let (is_anomaly, score) = detector.update(500.0);
        assert!(is_anomaly, "outlier should be detected, score={score}");
    }

    #[test]
    fn small_window_can_fire() {
        // With the candidate included in its own baseline, a 10-sample window
        // could never exceed (n-1)/sqrt(n) = 2.85 sigma. It must fire now.
        let mut d = ZScoreDetector::new(10, 3.0);
        for i in 0..10 {
            d.update(if i % 2 == 0 { 99.0 } else { 101.0 });
        }
        let (is_anomaly, score) = d.update(1000.0);
        assert!(
            is_anomaly,
            "10-sample window must detect a 450-sigma jump, got {score}"
        );
        assert!(score > 100.0);
    }

    #[test]
    fn flat_baseline_then_step_is_detected() {
        let mut d = ZScoreDetector::new(300, 3.0);
        for _ in 0..300 {
            let (a, _) = d.update(1.0);
            assert!(!a, "a constant series must not alarm");
        }
        let (is_anomaly, score) = d.update(0.0);
        assert!(
            is_anomaly,
            "1.0 -> 0.0 on a flat gauge must alarm, got {score}"
        );
    }

    #[test]
    fn flat_baseline_tolerates_small_jitter() {
        let mut d = ZScoreDetector::new(300, 3.0);
        for _ in 0..300 {
            d.update(1000.0);
        }
        let (is_anomaly, _) = d.update(1002.0);
        assert!(!is_anomaly, "0.2% off a flat baseline is noise");
    }

    #[test]
    fn large_magnitudes_keep_correct_variance() {
        // A byte counter near 1e12 with +/-1000 jitter. The naive formula
        // returns rounding noise here; Welford must track the true std.
        let mut d = ZScoreDetector::new(50, 3.0);
        for i in 0..500 {
            d.update(1.0e12 + ((i % 3) as f64 - 1.0) * 1000.0);
        }
        let n = d.window.len() as f64;
        let welford_std = (d.m2 / n).sqrt();
        let truth = exact_std(&d.window);
        assert!(
            (welford_std - truth).abs() / truth < 1e-6,
            "welford={welford_std} exact={truth}"
        );
        // And a real jump on that scale is still detected.
        let (is_anomaly, _) = d.update(1.0e12 + 50_000.0);
        assert!(is_anomaly);
    }

    #[test]
    fn running_stats_match_exact_after_many_slides() {
        let mut d = ZScoreDetector::new(37, 3.0);
        let mut x = 0.123_f64;
        for _ in 0..10_000 {
            x = (x * 9301.0 + 49297.0) % 233280.0; // deterministic pseudo-noise
            d.update(x / 1000.0);
        }
        let n = d.window.len() as f64;
        let mean = d.window.iter().sum::<f64>() / n;
        assert!((d.mean - mean).abs() < 1e-9 * mean.abs().max(1.0));
        assert!(((d.m2 / n).sqrt() - exact_std(&d.window)).abs() < 1e-6);
    }

    #[test]
    fn test_zscore_window_slides() {
        let mut detector = ZScoreDetector::new(20, 3.0);
        for _ in 0..20 {
            detector.update(100.0);
        }
        for _ in 0..40 {
            detector.update(200.0);
        }
        let (is_anomaly, _) = detector.update(200.0);
        assert!(!is_anomaly, "after adaptation, 200 should be normal");
    }

    #[test]
    fn test_zscore_warmup() {
        let mut detector = ZScoreDetector::new(100, 3.0);
        for _ in 0..MIN_SAMPLES {
            let (is_anomaly, _) = detector.update(999_999.0);
            assert!(!is_anomaly, "should not flag during warm-up");
        }
    }

    #[test]
    fn non_finite_values_are_ignored() {
        let mut d = ZScoreDetector::new(20, 3.0);
        for _ in 0..20 {
            d.update(5.0);
        }
        assert_eq!(d.update(f64::NAN), (false, 0.0));
        assert_eq!(d.update(f64::INFINITY), (false, 0.0));
        assert!(d.mean.is_finite() && d.m2.is_finite());
        assert_eq!(d.window.len(), 20);
    }
}
