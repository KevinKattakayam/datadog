//! Detector quality evaluation on labelled synthetic series.
//!
//! Every series is generated from a fixed seed, so the numbers below are
//! reproducible byte for byte:
//!
//!   cargo test --locked detector::eval -- --nocapture
//!
//! Scoring is event based. An *event* is a labelled span of anomalous points.
//! An alert is a true positive if it lands inside an event or within
//! `RECOVERY_TOLERANCE` points after it (the return to baseline is a real
//! change, not a false alarm). Every other alert is a false positive.
//!
//! * recall    = events with at least one true-positive alert / events
//! * precision = true-positive alerts / all alerts
//! * FP/1k     = false-positive alerts per 1,000 normal points
//!
//! These are synthetic series. They show how the detectors behave on four
//! well-understood shapes, including one (`recurring_batch`) that the current
//! detectors are expected to handle badly. They are not a claim about
//! production data.

use super::{ewma::EwmaDetector, zscore::ZScoreDetector};

const N: usize = 3000;
const WARMUP: usize = 300;
const RECOVERY_TOLERANCE: usize = 2;
/// Same values `docker-compose.yml` runs the processor with.
const ALPHA: f64 = 0.3;
const WINDOW: usize = 300;

struct Series {
    name: &'static str,
    values: Vec<f64>,
    /// Inclusive (start, end) index spans of labelled anomalies.
    events: Vec<(usize, usize)>,
}

/// Standard normal sample (Box-Muller).
fn gauss(rng: &mut fastrand::Rng) -> f64 {
    let u1 = rng.f64().max(1e-12);
    let u2 = rng.f64();
    (-2.0 * u1.ln()).sqrt() * (2.0 * std::f64::consts::PI * u2).cos()
}

/// Noisy stable metric (mean 100, sd 2) with 20 single-point spikes of
/// 6 to 12 standard deviations, either direction.
fn noisy_spikes() -> Series {
    let mut rng = fastrand::Rng::with_seed(1);
    let mut values: Vec<f64> = (0..N).map(|_| 100.0 + 2.0 * gauss(&mut rng)).collect();
    let mut events = Vec::new();
    for k in 0..20 {
        let at = WARMUP + 100 + k * 130;
        let sigmas = 6.0 + rng.f64() * 6.0;
        let sign = if rng.bool() { 1.0 } else { -1.0 };
        values[at] += sign * sigmas * 2.0;
        events.push((at, at));
    }
    Series {
        name: "noisy_spikes",
        values,
        events,
    }
}

/// A constant health gauge (1.0) that drops to 0.0 for five points, ten
/// times. This is the case the original EWMA could never flag.
fn flat_gauge() -> Series {
    let mut values = vec![1.0; N];
    let mut events = Vec::new();
    for k in 0..10 {
        let at = WARMUP + 100 + k * 250;
        for v in &mut values[at..at + 5] {
            *v = 0.0;
        }
        events.push((at, at + 4));
    }
    Series {
        name: "flat_gauge",
        values,
        events,
    }
}

/// Noisy metric (mean 100, sd 2) that jumps to 130 for 40 points, 15 times.
fn level_shift() -> Series {
    let mut rng = fastrand::Rng::with_seed(2);
    let mut values: Vec<f64> = (0..N).map(|_| 100.0 + 2.0 * gauss(&mut rng)).collect();
    let mut events = Vec::new();
    for k in 0..15 {
        let at = WARMUP + 100 + k * 180;
        for v in &mut values[at..at + 40] {
            *v += 30.0;
        }
        events.push((at, at + 39));
    }
    Series {
        name: "level_shift",
        values,
        events,
    }
}

/// A metric with a *normal* recurring burst: +100 for 12 points every 288
/// points (a daily batch job at 5-minute resolution). Only 10 injected
/// spikes are labelled anomalies. The bursts are expected behaviour, so any
/// alert on them is a false positive. A detector without seasonality cannot
/// tell the two apart; this series measures how much that costs.
fn recurring_batch() -> Series {
    let mut rng = fastrand::Rng::with_seed(3);
    let mut values: Vec<f64> = (0..N).map(|_| 100.0 + 2.0 * gauss(&mut rng)).collect();
    let mut burst_starts = Vec::new();
    let mut s = 288;
    while s + 12 < N {
        for v in &mut values[s..s + 12] {
            *v += 100.0;
        }
        burst_starts.push(s);
        s += 288;
    }
    let mut events = Vec::new();
    for k in 0..10 {
        // Offset by 53 so no injected spike lands on or right after a burst.
        let at = WARMUP + 53 + k * 270;
        let near_burst = burst_starts
            .iter()
            .any(|b| at + RECOVERY_TOLERANCE >= *b && at <= b + 12 + RECOVERY_TOLERANCE);
        if near_burst {
            continue;
        }
        values[at] += 40.0;
        events.push((at, at));
    }
    Series {
        name: "recurring_batch",
        values,
        events,
    }
}

#[derive(Clone, Copy, Debug)]
enum Which {
    Ewma,
    ZScore,
    Either,
}

impl Which {
    fn label(self) -> &'static str {
        match self {
            Which::Ewma => "ewma",
            Which::ZScore => "zscore",
            Which::Either => "either",
        }
    }
}

#[derive(Debug)]
struct Score {
    events: usize,
    detected: usize,
    alerts: usize,
    true_alerts: usize,
    false_alerts: usize,
    normal_points: usize,
}

impl Score {
    fn recall(&self) -> f64 {
        if self.events == 0 {
            1.0
        } else {
            self.detected as f64 / self.events as f64
        }
    }
    fn precision(&self) -> Option<f64> {
        (self.alerts > 0).then(|| self.true_alerts as f64 / self.alerts as f64)
    }
    fn fp_per_1k(&self) -> f64 {
        self.false_alerts as f64 * 1000.0 / self.normal_points.max(1) as f64
    }
}

fn run(series: &Series, which: Which, threshold: f64) -> Score {
    let mut ewma = EwmaDetector::new(ALPHA, threshold);
    let mut zscore = ZScoreDetector::new(WINDOW, threshold);

    let mut alert_at = Vec::new();
    for (i, v) in series.values.iter().enumerate() {
        // Both detectors must see every point so their state matches
        // production, where the registry updates both on every metric.
        let e = ewma.update(*v).0;
        let z = zscore.update(*v).0;
        let fired = match which {
            Which::Ewma => e,
            Which::ZScore => z,
            Which::Either => e || z,
        };
        if fired {
            alert_at.push(i);
        }
    }

    let in_event = |i: usize| {
        series
            .events
            .iter()
            .position(|(s, e)| i >= *s && i <= e + RECOVERY_TOLERANCE)
    };

    let mut hit = vec![false; series.events.len()];
    let mut true_alerts = 0;
    for &i in &alert_at {
        if let Some(ev) = in_event(i) {
            hit[ev] = true;
            true_alerts += 1;
        }
    }
    let event_points: usize = series.events.iter().map(|(s, e)| e - s + 1).sum();

    Score {
        events: series.events.len(),
        detected: hit.iter().filter(|h| **h).count(),
        alerts: alert_at.len(),
        true_alerts,
        false_alerts: alert_at.len() - true_alerts,
        normal_points: N - event_points,
    }
}

fn all_series() -> Vec<Series> {
    vec![
        noisy_spikes(),
        flat_gauge(),
        level_shift(),
        recurring_batch(),
    ]
}

/// Prints the full table. Run with `--nocapture` to see it.
#[test]
fn report() {
    println!();
    println!(
        "{:<16} {:<7} {:<5} {:>6} {:>7} {:>7} {:>7} {:>6}",
        "series", "det", "thr", "events", "recall", "prec", "alerts", "FP/1k"
    );
    for series in all_series() {
        for thr in [3.0, 4.0, 5.0] {
            for which in [Which::Ewma, Which::ZScore, Which::Either] {
                let s = run(&series, which, thr);
                let prec = s
                    .precision()
                    .map(|p| format!("{:.2}", p))
                    .unwrap_or_else(|| "n/a".into());
                println!(
                    "{:<16} {:<7} {:<5.1} {:>3}/{:<2} {:>7.2} {:>7} {:>7} {:>6.1}",
                    series.name,
                    which.label(),
                    thr,
                    s.detected,
                    s.events,
                    s.recall(),
                    prec,
                    s.alerts,
                    s.fp_per_1k()
                );
            }
        }
    }
}

#[test]
fn series_are_deterministic() {
    let a = noisy_spikes();
    let b = noisy_spikes();
    assert_eq!(a.values, b.values);
    assert_eq!(a.events, b.events);
}

#[test]
fn labels_are_sane() {
    for s in all_series() {
        assert!(!s.events.is_empty(), "{} has no events", s.name);
        for (start, end) in &s.events {
            assert!(*start >= WARMUP, "{}: event inside warm-up", s.name);
            assert!(start <= end && *end < N);
        }
    }
}

fn find(name: &str) -> Series {
    all_series()
        .into_iter()
        .find(|s| s.name == name)
        .expect("series exists")
}

// ---- Regression floors -------------------------------------------------
// Set just inside the measured values (see docs/DETECTOR_EVALUATION.md) so a
// change that makes the detectors measurably worse fails CI.

#[test]
fn ewma_false_alarm_rate_on_pure_noise_stays_near_theory() {
    // A 3-sigma threshold on Gaussian noise should alarm roughly 3 times per
    // 1,000 points. With the variance sharing the mean's fast smoothing it was
    // 37.2. The floor catches a regression to that behaviour.
    let s = run(&find("noisy_spikes"), Which::Ewma, 3.0);
    assert_eq!(s.detected, s.events, "every spike must be found");
    assert!(
        s.fp_per_1k() <= 6.0,
        "EWMA false alarms per 1k points: {:.1}",
        s.fp_per_1k()
    );
}

#[test]
fn combined_detector_is_clean_on_noisy_spikes_at_4_sigma() {
    let s = run(&find("noisy_spikes"), Which::Either, 4.0);
    assert_eq!(s.detected, s.events);
    assert!(s.precision().unwrap_or(0.0) >= 0.95);
}

#[test]
fn flat_gauge_every_outage_is_found_with_no_false_alarms() {
    for which in [Which::Ewma, Which::ZScore, Which::Either] {
        let s = run(&find("flat_gauge"), which, 3.0);
        assert_eq!(s.detected, s.events, "{}", which.label());
        assert_eq!(s.false_alerts, 0, "{}", which.label());
    }
}

#[test]
fn combined_detector_finds_every_level_shift_with_high_precision() {
    let s = run(&find("level_shift"), Which::Either, 3.0);
    assert_eq!(s.detected, s.events);
    assert!(s.precision().unwrap_or(0.0) >= 0.85);
}

/// A KNOWN LIMITATION, asserted so it cannot be forgotten. Neither detector
/// models seasonality, so a normal recurring burst is flagged every time.
/// When seasonality-aware detection lands, this test should be inverted.
#[test]
fn known_limitation_recurring_batch_is_mostly_false_alarms() {
    let s = run(&find("recurring_batch"), Which::Either, 3.0);
    assert!(
        s.precision().unwrap_or(1.0) < 0.5,
        "if this now passes precision, update docs/DETECTOR_EVALUATION.md"
    );
}
