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

use super::{ewma::EwmaDetector, seasonal::SeasonalDetector, zscore::ZScoreDetector};

const N: usize = 3000;
const WARMUP: usize = 300;
const RECOVERY_TOLERANCE: usize = 2;
/// Same values `docker-compose.yml` runs the processor with.
const ALPHA: f64 = 0.3;
const WINDOW: usize = 300;
/// Synthetic series are sampled every 5 minutes; 288 points make a day.
const STEP_SECS: i64 = 300;
const SEASON_PERIOD_SECS: i64 = 86_400;
const SEASON_BUCKETS: usize = 24;
/// Strength of the daily pattern above which `Auto` trusts the seasonal model.
const AUTO_STRENGTH_MIN: f64 = 0.5;
const ALL_DETECTORS: [Which; 6] = [
    Which::Ewma,
    Which::ZScore,
    Which::Either,
    Which::Seasonal,
    Which::Gated,
    Which::Auto,
];

struct Series {
    name: &'static str,
    values: Vec<f64>,
    /// Unix timestamp of each point (the seasonal detector keys on it).
    times: Vec<i64>,
    /// Inclusive (start, end) index spans of labelled anomalies.
    events: Vec<(usize, usize)>,
}

fn step_times(n: usize) -> Vec<i64> {
    (0..n).map(|i| i as i64 * STEP_SECS).collect()
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
        times: step_times(N),
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
        times: step_times(N),
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
        times: step_times(N),
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
        times: step_times(N),
        values,
        events,
    }
}

#[derive(Clone, Copy, Debug)]
enum Which {
    Ewma,
    ZScore,
    Either,
    /// The seasonal baseline alone.
    Seasonal,
    /// Seasonal baseline where the series shows a strong daily pattern,
    /// otherwise the plain either-detector rule.
    Auto,
    /// EWMA-or-Z-score alerts, kept only where the seasonal baseline also
    /// calls the value abnormal for its time of day. Where the slot has not
    /// warmed up yet it falls back to the plain either-detector rule.
    Gated,
}

impl Which {
    fn label(self) -> &'static str {
        match self {
            Which::Ewma => "ewma",
            Which::ZScore => "zscore",
            Which::Either => "either",
            Which::Seasonal => "season",
            Which::Gated => "gated",
            Which::Auto => "auto",
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
    let mut seasonal = SeasonalDetector::new(SEASON_PERIOD_SECS, SEASON_BUCKETS, threshold);

    let mut alert_at = Vec::new();
    for (i, v) in series.values.iter().enumerate() {
        // Both detectors must see every point so their state matches
        // production, where the registry updates both on every metric.
        let e = ewma.update(*v).0;
        let z = zscore.update(*v).0;
        let t = series.times[i];
        let ready = seasonal.is_ready(t);
        let strong = seasonal.strength() >= AUTO_STRENGTH_MIN;
        let sea = seasonal.update(*v, t).0;
        let fired = match which {
            Which::Auto => {
                if strong {
                    sea
                } else {
                    e || z
                }
            }
            Which::Ewma => e,
            Which::ZScore => z,
            Which::Either => e || z,
            Which::Seasonal => sea,
            Which::Gated => {
                if ready {
                    (e || z) && sea
                } else {
                    e || z
                }
            }
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
        normal_points: series.values.len() - event_points,
    }
}

/// Daily-pattern strength after the whole series has been seen.
fn final_strength(series: &Series) -> f64 {
    let mut d = SeasonalDetector::new(SEASON_PERIOD_SECS, SEASON_BUCKETS, 3.0);
    for (v, t) in series.values.iter().zip(&series.times) {
        d.update(*v, *t);
    }
    d.strength()
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
        println!(
            "# {} daily-pattern strength {:.2}",
            series.name,
            final_strength(&series)
        );
        for thr in [3.0, 4.0, 5.0] {
            for which in ALL_DETECTORS {
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

// ---- Real data: Numenta Anomaly Benchmark ------------------------------
//
// Seven real-world series (NYC taxi demand, AWS CPU and latency, machine
// temperature) with human-labelled anomaly windows. The data is AGPL-3.0, so
// it is NOT stored in this repository. Download it and point NAB_DIR at it:
//
//   NAB_DIR=/path/to/NAB cargo test --locked detector::eval::nab_report \
//       -- --ignored --nocapture
//
// Each labelled window is an event; an alert inside a window is a true
// positive. These are real series but not this pipeline's own traffic.

const NAB_SERIES: [&str; 7] = [
    "nyc_taxi",
    "ec2_request_latency_system_failure",
    "cpu_utilization_asg_misconfiguration",
    "machine_temperature_system_failure",
    "ambient_temperature_system_failure",
    "rogue_agent_key_hold",
    "rogue_agent_key_updown",
];

/// Unix seconds for "YYYY-MM-DD HH:MM:SS" (UTC; only differences matter here).
fn epoch_secs(ts: &str) -> i64 {
    if ts.len() < 19 || !ts.is_char_boundary(19) {
        return 0;
    }
    let n = |a: usize, b: usize| ts[a..b].parse::<i64>().unwrap_or(0);
    let (y, m, d) = (n(0, 4), n(5, 7), n(8, 10));
    let (hh, mm, ss) = (n(11, 13), n(14, 16), n(17, 19));
    // Days from civil date (Howard Hinnant's algorithm).
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146_097 + doe - 719_468) * 86_400 + hh * 3600 + mm * 60 + ss
}

fn load_nab(dir: &str, name: &str, labels: &serde_json::Value) -> Series {
    let csv = std::fs::read_to_string(format!("{dir}/data/realKnownCause/{name}.csv"))
        .unwrap_or_else(|e| panic!("cannot read {name}.csv: {e}"));
    let mut stamps: Vec<String> = Vec::new();
    let mut values: Vec<f64> = Vec::new();
    let mut times: Vec<i64> = Vec::new();
    for line in csv.lines().skip(1) {
        let Some((ts, v)) = line.split_once(',') else {
            continue;
        };
        if let Ok(v) = v.trim().parse::<f64>() {
            // "YYYY-MM-DD HH:MM:SS" sorts correctly as text.
            stamps.push(ts.chars().take(19).collect());
            times.push(epoch_secs(ts));
            values.push(v);
        }
    }
    let key = format!("realKnownCause/{name}.csv");
    let mut events = Vec::new();
    for w in labels[&key].as_array().expect("labels for series") {
        let start: String = w[0].as_str().unwrap().chars().take(19).collect();
        let end: String = w[1].as_str().unwrap().chars().take(19).collect();
        let first = stamps.iter().position(|t| *t >= start);
        let last = stamps.iter().rposition(|t| *t <= end);
        if let (Some(a), Some(b)) = (first, last) {
            if a <= b {
                events.push((a, b));
            }
        }
    }
    Series {
        name: "nab",
        times,
        values,
        events,
    }
}

#[test]
#[ignore = "needs the NAB data; see the comment above"]
fn nab_report() {
    let Ok(dir) = std::env::var("NAB_DIR") else {
        println!("NAB_DIR is not set; nothing to evaluate");
        return;
    };
    let labels: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(format!("{dir}/labels/combined_windows.json"))
            .expect("labels file"),
    )
    .expect("labels json");

    println!();
    println!(
        "{:<38} {:<7} {:<5} {:>6} {:>7} {:>7} {:>7} {:>6}",
        "series", "det", "thr", "events", "recall", "prec", "alerts", "FP/1k"
    );
    for name in NAB_SERIES {
        let series = load_nab(&dir, name, &labels);
        println!(
            "# {} daily-pattern strength {:.2}",
            name,
            final_strength(&series)
        );
        for thr in [3.0, 4.0, 5.0] {
            for which in ALL_DETECTORS {
                let s = run(&series, which, thr);
                let prec = s
                    .precision()
                    .map(|p| format!("{:.2}", p))
                    .unwrap_or_else(|| "n/a".into());
                println!(
                    "{:<38} {:<7} {:<5.1} {:>3}/{:<2} {:>7.2} {:>7} {:>7} {:>6.1}",
                    name,
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
fn epoch_conversion_matches_known_values() {
    assert_eq!(epoch_secs("1970-01-01 00:00:00"), 0);
    assert_eq!(epoch_secs("2014-07-01 00:30:00"), 1_404_174_600);
    assert_eq!(epoch_secs("2000-03-01 00:00:00"), 951_868_800);
}

/// Where the daily pattern is real, trusting the seasonal baseline finds the
/// injected spikes the plain rule misses and stops flagging the normal burst.
#[test]
fn auto_policy_fixes_recurring_batch_where_either_cannot() {
    let series = find("recurring_batch");
    let either = run(&series, Which::Either, 4.0);
    let auto = run(&series, Which::Auto, 4.0);
    assert_eq!(auto.detected, auto.events, "auto must find every spike");
    assert!(auto.recall() > either.recall());
    assert!(
        auto.precision().unwrap_or(0.0) >= 0.35,
        "auto precision {:?} (either was {:?})",
        auto.precision(),
        either.precision()
    );
}

/// Where there is no daily pattern, `Auto` must not behave worse than the
/// plain rule.
#[test]
fn auto_policy_does_not_regress_series_without_a_daily_pattern() {
    for name in ["noisy_spikes", "flat_gauge"] {
        let series = find(name);
        let either = run(&series, Which::Either, 3.0);
        let auto = run(&series, Which::Auto, 3.0);
        assert!(auto.detected >= either.detected, "{name}: recall regressed");
        assert!(
            auto.false_alerts <= either.false_alerts + 2,
            "{name}: false alarms {} vs {}",
            auto.false_alerts,
            either.false_alerts
        );
    }
}
