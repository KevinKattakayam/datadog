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

        // Either detector firing is an anomaly; report whichever scored higher.
        if e.score >= z.score {
            DetectionResult {
                is_anomaly: e.is_anomaly || z.is_anomaly,
                score: e.score,
                detector_type: e.detector_type,
                severity: e.severity,
            }
        } else {
            DetectionResult {
                is_anomaly: e.is_anomaly || z.is_anomaly,
                score: z.score,
                detector_type: z.detector_type,
                severity: z.severity,
            }
        }
    }
}
