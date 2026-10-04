// Seasonal baseline detector.
//
// EWMA and the rolling Z-score compare a value with the *recent past*. When a
// metric has a daily or weekly rhythm (a nightly batch job, a lunchtime peak),
// the recent past is the wrong baseline: every recurring burst looks like an
// anomaly, and a genuine anomaly during a naturally busy hour hides inside
// that hour's larger swings.
//
// This detector keeps one baseline per position in the cycle. The cycle
// (`period_secs`, normally one day) is split into `buckets` equal slots. A
// value is scored against the history of its own slot: 14:00 is compared with
// previous 14:00s, not with 13:55.
//
// It is keyed by the sample's own timestamp, not by arrival order, so late or
// replayed records land in the right slot.
//
// Memory is `buckets` x 24 bytes per series: 24 buckets is about 600 bytes.
//
// Not yet wired into the registry; see docs/DETECTOR_EVALUATION.md for the
// measurements that decide whether and how it is.

#![allow(dead_code)]

/// Relative move off a flat baseline that counts as anomalous.
const FLAT_REL_TOLERANCE: f64 = 0.10;

#[derive(Debug, Clone, Copy, Default)]
struct Bucket {
    mean: f64,
    /// Exponentially weighted variance.
    var: f64,
    n: u64,
}

#[derive(Debug, Clone)]
pub struct SeasonalDetector {
    period_secs: i64,
    bucket_secs: i64,
    buckets: Vec<Bucket>,
    /// Weight of a new sample once a slot has enough history.
    alpha: f64,
    threshold: f64,
    /// Samples a slot needs before it is trusted. A slot that has seen only
    /// one or two values knows nothing about its own variability.
    min_samples: u64,
}

impl SeasonalDetector {
    /// * `period_secs` - length of the cycle (86_400 for daily).
    /// * `buckets` - slots per cycle (24 gives hourly slots for a daily cycle).
    /// * `threshold` - deviation, in standard deviations of the slot, to alarm.
    pub fn new(period_secs: i64, buckets: usize, threshold: f64) -> Self {
        let buckets = buckets.max(1);
        let period_secs = period_secs.max(buckets as i64);
        SeasonalDetector {
            period_secs,
            bucket_secs: (period_secs / buckets as i64).max(1),
            buckets: vec![Bucket::default(); buckets],
            alpha: 0.1,
            threshold,
            min_samples: 6,
        }
    }

    /// Share of the series' variance explained by time of day, from 0.0 (no
    /// daily pattern) to 1.0 (value is fully determined by the slot).
    /// Zero until every slot has enough history. Use it to decide whether this
    /// series is worth scoring seasonally at all: on a series with no pattern
    /// the per-slot baselines only add noise.
    pub fn strength(&self) -> f64 {
        if self.buckets.iter().any(|b| b.n < self.min_samples) {
            return 0.0;
        }
        let k = self.buckets.len() as f64;
        let mean_of_means = self.buckets.iter().map(|b| b.mean).sum::<f64>() / k;
        let between = self
            .buckets
            .iter()
            .map(|b| (b.mean - mean_of_means).powi(2))
            .sum::<f64>()
            / k;
        let within = self.buckets.iter().map(|b| b.var.max(0.0)).sum::<f64>() / k;
        let total = between + within;
        if total <= 0.0 {
            0.0
        } else {
            between / total
        }
    }

    /// True once the slot for `timestamp` has enough history to be scored.
    pub fn is_ready(&self, timestamp: i64) -> bool {
        self.buckets[self.slot(timestamp)].n >= self.min_samples
    }

    fn slot(&self, timestamp: i64) -> usize {
        let offset = timestamp.rem_euclid(self.period_secs);
        ((offset / self.bucket_secs) as usize).min(self.buckets.len() - 1)
    }

    /// Score `value` against its slot, then fold it into the slot.
    /// Returns (is_anomaly, score). Slots still warming up never alarm.
    pub fn update(&mut self, value: f64, timestamp: i64) -> (bool, f64) {
        if !value.is_finite() {
            return (false, 0.0);
        }
        let idx = self.slot(timestamp);
        let b = self.buckets[idx];

        let score = if b.n >= self.min_samples {
            let std_dev = b.var.max(0.0).sqrt();
            let diff = (value - b.mean).abs();
            let scale = b.mean.abs().max(1.0);
            if std_dev > 1e-9 * scale {
                Some(diff / std_dev)
            } else if diff / scale > FLAT_REL_TOLERANCE {
                Some(self.threshold * 2.0)
            } else {
                Some(0.0)
            }
        } else {
            None
        };

        // Fold the sample in. A value that alarms is pulled back to the edge of
        // the band first, so a multi-day incident cannot teach the slot that
        // the incident is normal in a single step.
        let fold = match score {
            Some(z) if z > self.threshold && b.var > 0.0 => {
                let limit = self.threshold * b.var.sqrt();
                b.mean + (value - b.mean).clamp(-limit, limit)
            }
            _ => value,
        };
        let slot = &mut self.buckets[idx];
        slot.n += 1;
        if slot.n == 1 {
            slot.mean = fold;
            slot.var = 0.0;
        } else {
            // Plain running estimate until the slot has seen 1/alpha samples,
            // then exponential weighting.
            let w = self.alpha.max(1.0 / slot.n as f64);
            let diff = fold - slot.mean;
            let incr = w * diff;
            slot.mean += incr;
            slot.var = (1.0 - w) * (slot.var + diff * incr);
        }

        match score {
            Some(z) => (z > self.threshold, z),
            None => (false, 0.0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 86_400;
    const STEP: i64 = 300; // 5-minute samples

    /// A flat series with a +100 burst in one hour every day.
    fn value_at(ts: i64) -> f64 {
        let hour = (ts.rem_euclid(DAY)) / 3600;
        100.0 + if hour == 3 { 100.0 } else { 0.0 } + ((ts / STEP) % 3) as f64 - 1.0
    }

    #[test]
    fn warming_slots_never_alarm() {
        let mut d = SeasonalDetector::new(DAY, 24, 3.0);
        for i in 0..200 {
            assert!(!d.update(1000.0 * i as f64, i * STEP).0);
        }
    }

    #[test]
    fn a_recurring_burst_is_learned_and_not_flagged() {
        let mut d = SeasonalDetector::new(DAY, 24, 3.0);
        let mut burst_alarms_after_warmup = 0;
        for i in 0..(14 * 288) {
            let ts = i * STEP;
            let (alarm, _) = d.update(value_at(ts), ts);
            let day = ts / DAY;
            if alarm && day >= 5 {
                burst_alarms_after_warmup += 1;
            }
        }
        assert_eq!(
            burst_alarms_after_warmup, 0,
            "the daily burst must stop alarming once learned"
        );
    }

    #[test]
    fn an_off_pattern_spike_is_still_flagged() {
        let mut d = SeasonalDetector::new(DAY, 24, 3.0);
        for i in 0..(10 * 288) {
            let ts = i * STEP;
            d.update(value_at(ts), ts);
        }
        // Noon on day 11 is an ordinary hour; 300 is far outside it.
        let ts = 10 * DAY + 12 * 3600;
        let (alarm, score) = d.update(300.0, ts);
        assert!(alarm, "off-pattern spike should alarm, score={score}");
    }

    #[test]
    fn non_finite_values_are_ignored() {
        let mut d = SeasonalDetector::new(DAY, 24, 3.0);
        assert_eq!(d.update(f64::NAN, 0), (false, 0.0));
        assert_eq!(d.update(f64::INFINITY, 0), (false, 0.0));
    }

    #[test]
    fn negative_and_huge_timestamps_land_in_a_valid_slot() {
        let mut d = SeasonalDetector::new(DAY, 24, 3.0);
        d.update(1.0, -1);
        d.update(1.0, i64::MAX / 2);
        d.update(1.0, i64::MIN / 2);
    }
}
