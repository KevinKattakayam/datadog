# Detector evaluation

Reproduce every number below with:

```bash
make detector-eval        # cargo test --locked detector::eval -- --nocapture
```

The first four series below are synthetic and generated from fixed seeds, so
the output is identical on every run. They show how the detectors behave on
four well-understood shapes. A second section evaluates real, human-labelled
series from the Numenta Anomaly Benchmark. **Neither is this pipeline's own
production traffic.**

Settings match `docker-compose.yml`: EWMA alpha 0.3, Z-score window 300.
Production fires an alert when **either** detector fires (`registry.rs`).

## Method

3,000 points per series. An *event* is a labelled span of anomalous points.
An alert is a true positive if it lands inside an event or within two points
after it (the return to baseline is a real change). Every other alert is a
false positive.

| Series | Shape | Labelled events |
|---|---|---|
| `noisy_spikes` | mean 100, sd 2 | 20 single-point spikes of 6 to 12 sd |
| `flat_gauge` | constant 1.0 health gauge | 10 drops to 0.0 for 5 points |
| `level_shift` | mean 100, sd 2 | 15 jumps of +30 lasting 40 points |
| `recurring_batch` | mean 100, sd 2, plus a normal +100 burst every 288 points | 9 injected spikes; the bursts are normal |

## Results at the shipped threshold (3.0), combined detector

| Series | Recall | Precision | False alarms per 1,000 normal points |
|---|---|---|---|
| `noisy_spikes` | 20 / 20 | 0.65 | 3.7 |
| `flat_gauge` | 10 / 10 | 1.00 | 0.0 |
| `level_shift` | 15 / 15 | 0.91 | 2.9 |
| `recurring_batch` | 7 / 9 | 0.05 | 45.8 |

At threshold 4.0 the combined detector scores recall 1.00 and precision 1.00
on `noisy_spikes`, and recall 1.00 with precision 1.00 on `level_shift`, with
0.0 false alarms per 1,000 points on both. Raising
`PROCESSOR_ANOMALY_THRESHOLD` from 3.0 to 4.0 is worth considering if alert
noise matters more than catching 3-to-4-sigma deviations. Run the harness to
see all three thresholds for every series and detector.

## What the harness found, and what changed

**EWMA false-alarmed 37 times per 1,000 points on pure noise at 3 sigma.**
Theory says a 3-sigma threshold on Gaussian noise should alarm about 3 times.
The cause: the variance estimate shared the mean's fast smoothing (alpha 0.3),
so it averaged only about 6 samples and was very noisy. The variance now uses
its own smoothing, about 6 times slower, and a plain running estimate while
the detector warms up.

| EWMA alone, threshold 3.0 | Before | After |
|---|---|---|
| `noisy_spikes` precision | 0.15 | 0.65 |
| `noisy_spikes` false alarms per 1,000 | 37.2 | 3.7 |
| `level_shift` precision | 0.35 | 0.90 |
| `level_shift` false alarms per 1,000 | 34.6 | 2.1 |
| Recall on both | 1.00 | 1.00 |

Regression tests in `processor/src/detector/eval.rs` fail if these numbers
get materially worse.

## Known limitations (measured, not fixed)

- **No seasonality.** On `recurring_batch`, every normal burst raises an alert:
  precision 0.05, 45.8 false alarms per 1,000 points. A test asserts this so it
  is not forgotten; invert it when seasonality-aware detection lands.
- **That fix cost some recall on the same series** (EWMA recall 9/9 before,
  7/9 after). Bursts inflate the slower variance, so a later injected spike can
  hide inside it. On the three series without recurring bursts recall is
  unchanged.
- **The Z-score detector misses most level shifts** (2 of 15 at 3.0). The first
  shift inflates its 300-point window, which masks later ones. EWMA finds all
  15, and the combined detector fires on either, so the registry's output is
  not affected. Z-score alone should not be relied on for level shifts.
- Detector state lives in process memory and resets on restart or rebalance.

## Real data: Numenta Anomaly Benchmark

```bash
make detector-eval-real     # downloads NAB on first use, then runs the harness
```

Seven real series (NYC taxi demand, AWS CPU and request latency, machine and
ambient temperature, and two server metrics) with human-labelled anomaly
windows. NAB is AGPL-3.0, so the data is downloaded into a git-ignored
`.nab/` directory and never committed. Each labelled window is an event; an
alert inside a window is a true positive; every other alert is a false
positive. The detectors, settings and scoring are the same as above.

**The synthetic results were optimistic.** At the shipped threshold (3.0) the
combined detector finds nearly every labelled event but is very noisy:

| Series | Recall | Precision | False alarms per 1,000 points |
|---|---|---|---|
| `nyc_taxi` | 3 / 5 | 0.83 | 0.1 |
| `ec2_request_latency` | 3 / 3 | 0.26 | 11.4 |
| `cpu_utilization_asg` | 1 / 1 | 0.03 | 63.4 |
| `machine_temperature` | 4 / 4 | 0.11 | 48.7 |
| `ambient_temperature` | 2 / 2 | 0.25 | 11.3 |
| `rogue_agent_key_hold` | 2 / 2 | 0.43 | 17.7 |
| `rogue_agent_key_updown` | 2 / 2 | 0.15 | 19.0 |

Across all seven series (19 events), combined detector:

| Threshold | Events found | Alerts | Precision | False alarms per 1,000 points |
|---|---|---|---|---|
| 3.0 (shipped) | 17 / 19 | 2,526 | 0.10 | 36.3 |
| 4.0 | 14 / 19 | 672 | 0.20 | 8.5 |
| 5.0 | 11 / 19 | 230 | 0.32 | 2.5 |

Raising the threshold from 3.0 to 4.0 cuts false alarms by roughly four times
and costs 3 of 19 events. Whether that is the right trade depends on whether
a missed event or alert fatigue costs more; the shipped default has not
changed.

### Ideas tried in a prototype (not built into the processor)

`bench/detector_prototype.py` re-implements the two detectors in Python (its
alert counts match the Rust harness exactly) so ideas can be tested in
minutes. Run `python3 bench/detector_prototype.py knobs|grouping|seasonal`.

- **Threshold, requiring both detectors, requiring consecutive points.** These
  only slide along a single trade-off curve between missed events and false
  alarms. None is a free improvement.
- **Merging alerts into incidents.** Merging alerts within 48 points cuts 1,527
  alerts to 201 incidents at threshold 3, but about 170 incidents are still
  false. It reduces volume, not the number of distinct false alarms.
- **Removing a seasonal cycle first** is where the measurements are
  interesting. On `nyc_taxi`, which has a strong weekly cycle:

| Baseline | Threshold | Events found | Incidents | False incidents |
|---|---|---|---|---|
| none (shipped) | 4 | 1 / 5 | 1 | 0 |
| none (shipped) | 5 | 0 / 5 | 0 | 0 |
| daily | 4 | 2 / 5 | 49 | 46 |
| weekly | 4 | 5 / 5 | 47 | 32 |
| weekly | 5 | 5 / 5 | 21 | 12 |

A weekly baseline finds all five events at 4 and 5 sigma, where the shipped
detector finds one or none, at the cost of roughly one false incident per
week (about 30 weeks of data). A **daily** baseline on the same series is
much worse, because weekends differ from weekdays. Applied blindly to every
series, a daily baseline adds false incidents (685 against 452 at threshold
3, for one extra event), because most series have no daily cycle.

So seasonality is worth building, but a generic on-by-default version would
make most series noisier. The next section is a Rust implementation of that
idea, measured in the same harness; it is not wired into the processor.

### A seasonal baseline, built in Rust and measured (not wired in)

`processor/src/detector/seasonal.rs` keeps one baseline per time-of-day slot
(24 hourly slots over a 24-hour cycle, about 600 bytes per series) and scores a
value against the history of its own slot, using the sample's own timestamp.
The harness (`make detector-eval`, `make detector-eval-real`) scores it three
ways: alone (`season`), as a filter on the existing alerts (`gated`), and as
`auto`: use the seasonal baseline only where the series' daily pattern explains
at least half of its variance (measured online), otherwise the plain
either-detector rule.

`auto` at threshold 4.0, against the shipped rule:

| Series | Daily-pattern strength | Shipped rule | `auto` |
|---|---|---|---|
| `nyc_taxi` (real) | 0.54 | 1 / 5 events, 2 alerts, precision 1.00 | **5 / 5 events**, 74 alerts, precision 0.41 |
| `recurring_batch` (synthetic) | 0.99 | 6 / 9 events, 79 alerts, precision 0.08 | **9 / 9 events**, 20 alerts, precision 0.45 |

At the shipped threshold 3.0, `nyc_taxi` goes from 3 / 5 events and 6 alerts to
5 / 5 events and 198 alerts. Of the nine other series, seven score identically
and two change slightly (`machine_temperature` at 4.0: 333 to 508 alerts;
`level_shift` at 4.0: 46 to 62 alerts).

This is a **trade, not a free win**. On `nyc_taxi`, `auto` finds the four real
events the shipped rule misses, and pays for it with dozens of extra alerts.
Grouping them into incidents would shrink that cost; the prototype above shows
how far.

Tried and dropped: `gated` (keep an alert only if the seasonal baseline agrees)
loses real events on real data (`machine_temperature` 4 / 4 to 1 / 4 and
`nyc_taxi` 3 / 5 to 1 / 5 at threshold 3.0). `season` alone catches every event
on every NAB series at thresholds 3.0 and 4.0 (and on every synthetic series at
3.0) but is noisier than the shipped rule on series with no daily
pattern (`rogue_agent_key_hold` precision 0.11 against 0.43).

The Python prototype's weekly baseline also reaches 5 / 5 on `nyc_taxi`. The two
implementations differ (the prototype subtracts a cycle, re-runs the detectors
and counts merged incidents; the Rust detector scores per slot and counts
alerts), so alert and incident counts are not comparable. The Rust detector
accepts any period, but only the daily setting has been measured.

### Limits

- Seven series from one benchmark; windows are long and several anomalies are
  subtle, so absolute scores on NAB are low for detectors of every kind.
- The detectors were run with fixed settings; nothing was tuned per series.
- This does not replace evaluating on the pipeline's own traffic.
- The `auto` strength cut-off (0.5) was set by looking at these eleven series,
  and only one real series (`nyc_taxi`, 0.54) is above it while another
  (`rogue_agent_key_hold`, 0.47) is just below. That is one real positive
  example; the cut-off is fragile and should be revisited on more data before
  `auto` is enabled anywhere.
- The seasonal detector is not registered in `registry.rs`, has no
  configuration, and does not persist its baselines across restarts.
