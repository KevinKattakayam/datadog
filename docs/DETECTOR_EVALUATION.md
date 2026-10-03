# Detector evaluation

Reproduce every number below with:

```bash
make detector-eval        # cargo test --locked detector::eval -- --nocapture
```

The series are synthetic and generated from fixed seeds, so the output is
identical on every run. They show how the detectors behave on four
well-understood shapes. **They are not a claim about production data.** The
labelled-dataset item on the roadmap is only partly closed by this: real,
production-shaped traffic has not been evaluated.

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
