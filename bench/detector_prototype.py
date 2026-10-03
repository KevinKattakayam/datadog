#!/usr/bin/env python3
"""Exploratory comparison of detector ideas on the real NAB series.

This is a PROTOTYPE, not the shipped detector. It re-implements the EWMA and
rolling Z-score detectors from processor/src/detector (the alert counts match
the Rust harness exactly) so that ideas can be tried in minutes before any are
built in Rust.

    bash bench/fetch_nab.sh
    python3 bench/detector_prototype.py knobs      # threshold / AND / debounce
    python3 bench/detector_prototype.py grouping   # merge alerts into incidents
    python3 bench/detector_prototype.py seasonal   # remove a daily/weekly cycle
"""
import collections, json, math, os, statistics, sys

D = os.environ.get("NAB_DIR", ".nab")
NAMES = ["nyc_taxi", "ec2_request_latency_system_failure",
         "cpu_utilization_asg_misconfiguration", "machine_temperature_system_failure",
         "ambient_temperature_system_failure", "rogue_agent_key_hold",
         "rogue_agent_key_updown"]
PER_DAY = {"nyc_taxi": 48}  # 30-minute samples; the others are 5-minute


def per_day(n):
    return PER_DAY.get(n, 288)


def load(n, labels):
    ts, v = [], []
    for line in open(f"{D}/data/realKnownCause/{n}.csv").read().splitlines()[1:]:
        t, x = line.split(",")
        ts.append(t[:19]); v.append(float(x))
    ev = []
    for s, e in labels[f"realKnownCause/{n}.csv"]:
        s, e = s[:19], e[:19]
        a = next((i for i, t in enumerate(ts) if t >= s), None)
        b = max((i for i, t in enumerate(ts) if t <= e), default=None)
        if a is not None and b is not None and a <= b:
            ev.append((a, b))
    return v, ev


def ewma_scores(v, thr, alpha=0.3):
    valpha = min(max(alpha / 6, 0.01), alpha)
    min_s = max(30, min(500, int(10 / alpha)))
    ew = var = 0.0; cnt = 0; out = []
    for x in v:
        cnt += 1
        if cnt == 1:
            ew = x; out.append(0); continue
        diff = x - ew; sd = math.sqrt(var)
        if sd > 1e-10: sc = abs(diff) / sd
        else: sc = thr * 2 if abs(diff) / max(abs(ew), 1) > 0.10 else 0
        va = max(valpha, 1 / cnt)
        ew += alpha * diff; var = (1 - va) * (var + va * diff * diff)
        out.append(0 if cnt < min_s else sc)
    return out


def z_scores(v, thr, window=300):
    win = collections.deque(); out = []
    for x in v:
        n = len(win)
        if n >= 10:
            m = sum(win) / n; sd = math.sqrt(sum((y - m) ** 2 for y in win) / n)
            sc = max(abs(m), 1)
            if sd > 1e-12 * sc: s = abs(x - m) / sd
            else: s = thr * 2 if abs(x - m) / sc > 0.10 else 0
        else:
            s = 0
        out.append(s); win.append(x)
        if len(win) > window: win.popleft()
    return out


def either(v, thr):
    return [a > thr or b > thr for a, b in zip(ewma_scores(v, thr), z_scores(v, thr))]


def episodes(fired, gap):
    eps, cur = [], None
    for i, f in enumerate(fired):
        if f:
            if cur and i - cur[1] <= gap: cur[1] = i
            else: cur = [i, i]; eps.append(cur)
    return eps


def score_alerts(v, ev, fired):
    alerts = [i for i, f in enumerate(fired) if f]
    hit, tp = set(), 0
    for i in alerts:
        for k, (a, b) in enumerate(ev):
            if a <= i <= b: hit.add(k); tp += 1; break
    pts = len(v) - sum(b - a + 1 for a, b in ev)
    return len(hit), len(ev), len(alerts), tp, (len(alerts) - tp) * 1000 / max(1, pts)


def score_episodes(ev, eps):
    hit, tp = set(), 0
    for a, b in eps:
        m = [k for k, (x, y) in enumerate(ev) if not (b < x or a > y)]
        if m: tp += 1; hit.update(m)
    return len(hit), len(ev), len(eps), tp


def seasonal_flags(n, v, thr, period, cycles):
    res, valid = [0.0] * len(v), [False] * len(v)
    for i in range(len(v)):
        past = [v[i - k * period] for k in range(1, cycles + 1) if i - k * period >= 0]
        if len(past) >= 2:
            res[i] = v[i] - statistics.median(past); valid[i] = True
    start = next((i for i, f in enumerate(valid) if f), len(v))
    return [False] * start + either(res[start:], thr)


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "knobs"
    labels = json.load(open(f"{D}/labels/combined_windows.json"))
    data = {n: load(n, labels) for n in NAMES}

    if mode == "knobs":
        print(f"{'variant':26s} {'recall':>10s} {'alerts':>7s} {'prec':>6s} {'FP/1k':>6s}")
        for thr in (3, 4, 5):
            for name in ("either", "both", "ewma", "zscore"):
                for k in (1, 2, 3):
                    Dt = Et = At = Tt = fp = pts = 0
                    for n in NAMES:
                        v, ev = data[n]
                        e = [s > thr for s in ewma_scores(v, thr)]
                        z = [s > thr for s in z_scores(v, thr)]
                        fl = {"either": [a or b for a, b in zip(e, z)],
                              "both": [a and b for a, b in zip(e, z)],
                              "ewma": e, "zscore": z}[name]
                        if k > 1:  # require k consecutive flagged points
                            run, out = 0, []
                            for f in fl:
                                run = run + 1 if f else 0
                                out.append(run >= k)
                            fl = out
                        d, e_, a, t, _ = score_alerts(v, ev, fl)
                        Dt += d; Et += e_; At += a; Tt += t
                        fp += a - t; pts += len(v) - sum(y - x + 1 for x, y in ev)
                    print(f"{name + ' thr' + str(thr) + ' k' + str(k):26s} "
                          f"{Dt}/{Et}={Dt / Et:4.2f} {At:7d} {Tt / max(At, 1):6.2f} {fp * 1000 / pts:6.1f}")

    elif mode == "grouping":
        print(f"{'threshold / merge gap':24s} {'recall':>10s} {'incidents':>10s} {'false':>6s}")
        for thr in (3, 4, 5):
            for gap in (1, 12, 48):
                Dt = Et = Nt = Tt = 0
                for n in NAMES:
                    v, ev = data[n]
                    d, e, a, t = score_episodes(ev, episodes(either(v, thr), gap))
                    Dt += d; Et += e; Nt += a; Tt += t
                print(f"thr{thr} gap{gap:<3d}".ljust(24) + f" {Dt}/{Et}={Dt / Et:4.2f} {Nt:10d} {Nt - Tt:6d}")

    elif mode == "seasonal":
        v, ev = data["nyc_taxi"]
        print("nyc_taxi (strong daily and weekly cycle), incidents merged within 12 points")
        print(f"{'baseline':22s} {'thr':>4s} {'events':>7s} {'incidents':>10s} {'false':>6s}")
        for label, fl_fn in (("none (shipped)", lambda t: either(v, t)),
                             ("daily, 7 cycles", lambda t: seasonal_flags("nyc_taxi", v, t, 48, 7)),
                             ("weekly, 3 cycles", lambda t: seasonal_flags("nyc_taxi", v, t, 336, 3))):
            for thr in (3, 4, 5):
                d, e, a, t = score_episodes(ev, episodes(fl_fn(thr), 12))
                print(f"{label:22s} {thr:4d} {d:>4d}/{e:<2d} {a:10d} {a - t:6d}")
        print("\nSame idea applied blindly to every series (daily baseline):")
        for thr in (3, 4, 5):
            for label, fn in (("plain", lambda n, v, t: either(v, t)),
                              ("daily seasonal", lambda n, v, t: seasonal_flags(n, v, t, per_day(n), 7))):
                Dt = Et = Nt = Tt = 0
                for n in NAMES:
                    vv, ee = data[n]
                    d, e, a, t = score_episodes(ee, episodes(fn(n, vv, thr), 12))
                    Dt += d; Et += e; Nt += a; Tt += t
                print(f"  {label:15s} thr{thr}: events {Dt}/{Et}  incidents {Nt:4d}  false {Nt - Tt}")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main()
