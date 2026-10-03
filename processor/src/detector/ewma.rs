// EWMA (Exponentially Weighted Moving Average) Anomaly Detector
//
// Tracks the exponentially weighted mean and variance of a metric stream.
// Flags values as anomalous when they deviate beyond a configurable number
// of standard deviations from the EWMA.
//
// Fix: a near-zero variance now triggers relative-deviation fallback instead
// of returning z_score=0.0. A perfectly stable metric that suddenly moves is
// the *strongest* signal, not the weakest.
//
// Fix: the variance is smoothed ~6x more slowly than the mean. Sharing one
// alpha made the variance a ~6-sample estimate, which false-alarmed 37 times
// per 1,000 points on pure noise at 3 sigma. See docs/DETECTOR_EVALUATION.md.

use super::{AnomalyDetector, DetectionResult};
use crate::model::{AlertSeverity, RawMetric};

/// EWMA-based anomaly detector with configurable sensitivity.
#[derive(Debug, Clone)]
pub struct EwmaDetector {
    /// Smoothing factor (0 < alpha < 1). Higher = more reactive.
    alpha: f64,
    /// Smoothing factor for the variance estimate. Deliberately much slower
    /// than `alpha`: the mean needs to track the metric, but a variance built
    /// from the same ~6-sample window is so noisy that a 3-sigma threshold
    /// fires about 37 times per 1,000 points on pure Gaussian noise.
    variance_alpha: f64,
    /// Current exponentially weighted moving average.
    ewma: f64,
    /// Current exponentially weighted variance.
    variance: f64,
    /// Number of standard deviations to trigger anomaly.
    threshold_sigmas: f64,
    /// Number of samples seen (used for warm-up).
    count: u64,
    /// Minimum samples before detection activates.
    /// Set to 10/alpha (clamped 30..500) so the variance estimate is meaningful.
    min_samples: u64,
    /// Relative tolerance used when the observed variance collapses to zero.
    /// A flat metric that moves by more than this fraction of its own level
    /// is anomalous regardless of what the variance estimate says.
    flat_rel_tolerance: f64,
}

impl EwmaDetector {
    /// Create a new EWMA detector.
    ///
    /// # Arguments
    /// * `alpha` - Smoothing factor (0.0 to 1.0). 0.3 is a good default.
    /// * `threshold_sigmas` - Standard deviations for anomaly threshold. 3.0 is common.
    pub fn new(alpha: f64, threshold_sigmas: f64) -> Self {
        let alpha = alpha.clamp(0.01, 0.99);
        EwmaDetector {
            alpha,
            variance_alpha: (alpha / 6.0).clamp(0.01, alpha),
            ewma: 0.0,
            variance: 0.0,
            threshold_sigmas,
            count: 0,
            // 1/alpha is the effective memory. Ten times that is the smallest
            // window in which a variance estimate is worth anything. The old
            // value of 10 was ~3 effective samples.
            min_samples: ((10.0 / alpha) as u64).clamp(30, 500),
            flat_rel_tolerance: 0.10,
        }
    }

    /// Update the EWMA with a new value and return whether it's anomalous.
    pub fn update(&mut self, value: f64) -> (bool, f64) {
        // Guard the arithmetic before it poisons the state permanently: one
        // NaN in `ewma` makes every later comparison false, forever, silently.
        if !value.is_finite() {
            crate::metrics::NON_FINITE_VALUES.inc();
            return (false, 0.0);
        }

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

        let score = if std_dev > 1e-10 {
            diff.abs() / std_dev
        } else {
            // Degenerate baseline: the metric has been effectively constant.
            // Fall back to relative deviation, scaled against the metric's own
            // level so this works for a gauge at 1.0 and a counter at 1e9.
            let scale = self.ewma.abs().max(1.0);
            if diff.abs() / scale > self.flat_rel_tolerance {
                // Saturate above the threshold rather than returning inf, so
                // downstream severity arithmetic stays well defined.
                self.threshold_sigmas * 2.0
            } else {
                0.0
            }
        };

        // Now update EWMA and variance. While fewer than 1/variance_alpha
        // samples have been seen, weight each one by 1/n (a plain running
        // estimate) so the variance is not biased toward its zero start.
        let var_alpha = self.variance_alpha.max(1.0 / self.count as f64);
        self.ewma += self.alpha * diff;
        self.variance = (1.0 - var_alpha) * (self.variance + var_alpha * diff * diff);

        // Don't flag anomalies during warm-up period
        if self.count < self.min_samples {
            return (false, 0.0);
        }

        (score > self.threshold_sigmas, score)
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ewma_normal_values() {
        let mut detector = EwmaDetector::new(0.3, 3.0);

        // Feed normal values — need enough to pass the raised min_samples
        for i in 0..100 {
            let value = 100.0 + (i as f64 % 5.0);
            let (is_anomaly, _) = detector.update(value);
            if detector.count > detector.min_samples {
                assert!(!is_anomaly, "normal value flagged as anomaly");
            }
        }
    }

    #[test]
    fn test_ewma_detects_spike() {
        let mut detector = EwmaDetector::new(0.3, 3.0);

        // Establish baseline with slight variance (realistic data)
        for i in 0..100 {
            let value = 100.0 + (i as f64 % 3.0) - 1.0; // values between 99 and 101
            detector.update(value);
        }

        // Inject a massive spike — 10x the normal range
        let (is_anomaly, score) = detector.update(1000.0);
        assert!(
            is_anomaly,
            "spike should be detected as anomaly, score={}",
            score
        );
        assert!(score > 3.0, "score should exceed threshold, got {}", score);
    }

    #[test]
    fn test_ewma_warmup_period() {
        let mut detector = EwmaDetector::new(0.3, 3.0);

        // During warm-up, nothing should be flagged even for extreme values
        for _ in 0..detector.min_samples.saturating_sub(1) {
            let (is_anomaly, _) = detector.update(1000.0);
            assert!(!is_anomaly, "should not flag during warm-up");
        }
    }

    #[test]
    fn flat_baseline_then_step_is_detected() {
        // The exact case the previous implementation could never flag.
        let mut d = EwmaDetector::new(0.3, 3.0);
        for _ in 0..200 {
            let (a, _) = d.update(1.0);
            assert!(!a, "a constant series must not alarm");
        }
        let (is_anomaly, score) = d.update(0.0);
        assert!(
            is_anomaly,
            "1.0 -> 0.0 on a flat gauge must alarm, got score={score}"
        );
    }

    #[test]
    fn flat_baseline_tolerates_small_jitter() {
        let mut d = EwmaDetector::new(0.3, 3.0);
        for _ in 0..200 {
            d.update(1000.0);
        }
        let (is_anomaly, _) = d.update(1002.0); // 0.2% — noise, not an incident
        assert!(!is_anomaly);
    }

    #[test]
    fn nan_does_not_poison_state() {
        let mut d = EwmaDetector::new(0.3, 3.0);
        for _ in 0..100 {
            d.update(50.0);
        }
        d.update(f64::NAN);
        assert!(d.ewma.is_finite(), "NaN must not corrupt the baseline");
        let (is_anomaly, _) = d.update(50.0);
        assert!(!is_anomaly);
    }
}
