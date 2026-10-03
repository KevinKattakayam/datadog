// Bounded, LRU-evicted detector state. The previous HashMaps in consumer.rs
// grew without limit: one entry per (metric, host) ever seen, never removed.
// With rolling pod names that is an unbounded leak.

use lru::LruCache;
use std::num::NonZeroUsize;

use crate::detector::{
    ewma::EwmaDetector, zscore::ZScoreDetector, AnomalyDetector, DetectionResult,
};
use crate::model::RawMetric;

struct Pair {
    ewma: EwmaDetector,
    zscore: ZScoreDetector,
}

pub struct DetectorRegistry {
    cache: LruCache<String, Pair>,
    alpha: f64,
    window: usize,
    threshold: f64,
}

impl DetectorRegistry {
    pub fn new(capacity: usize, alpha: f64, window: usize, threshold: f64) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1_000)).unwrap();
        DetectorRegistry {
            cache: LruCache::new(cap),
            alpha,
            window,
            threshold,
        }
    }

    pub fn evaluate(&mut self, m: &RawMetric) -> DetectionResult {
        // Include tenant: two tenants reporting "cpu.usage" on a host called
        // "web-01" must not share a baseline.
        let tenant = m.tenant_id.as_deref().unwrap_or("default");
        let key = format!("{}|{}|{}", tenant, m.name, m.host);

        let was_full = self.cache.len() == self.cache.cap().get();
        let existed = self.cache.contains(&key);
        if was_full && !existed {
            crate::metrics::DETECTOR_EVICTIONS.inc();
        }

        let (alpha, window, threshold) = (self.alpha, self.window, self.threshold);
        let pair = self.cache.get_or_insert_mut(key, || Pair {
            ewma: EwmaDetector::new(alpha, threshold),
            zscore: ZScoreDetector::new(window, threshold),
        });

        let e = pair.ewma.detect(m);
        let z = pair.zscore.detect(m);

        crate::metrics::DETECTOR_SERIES_TRACKED.set(self.cache.len() as f64);
        combine(e, z)
    }
}

/// Either detector firing is an anomaly. Attribute it to a detector that
/// actually fired (the higher-scoring one if both did); when neither fired,
/// report the higher score for observability.
fn combine(e: DetectionResult, z: DetectionResult) -> DetectionResult {
    match (e.is_anomaly, z.is_anomaly) {
        (true, false) => e,
        (false, true) => z,
        _ if e.score >= z.score => e,
        _ => z,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::AlertSeverity;

    fn r(name: &str, fired: bool, score: f64) -> DetectionResult {
        DetectionResult {
            is_anomaly: fired,
            score,
            detector_type: name.to_string(),
            severity: AlertSeverity::Warning,
        }
    }

    #[test]
    fn attributes_to_the_detector_that_fired() {
        // zscore scored higher but did not fire; ewma fired.
        let out = combine(r("ewma", true, 3.5), r("zscore", false, 9.0));
        assert!(out.is_anomaly);
        assert_eq!(out.detector_type, "ewma");
    }

    #[test]
    fn both_fired_takes_higher_score() {
        let out = combine(r("ewma", true, 4.0), r("zscore", true, 7.0));
        assert_eq!(out.detector_type, "zscore");
    }

    #[test]
    fn neither_fired_is_not_an_anomaly() {
        let out = combine(r("ewma", false, 1.0), r("zscore", false, 2.0));
        assert!(!out.is_anomaly);
    }
}
