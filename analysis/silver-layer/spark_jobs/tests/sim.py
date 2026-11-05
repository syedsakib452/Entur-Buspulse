"""Simulated buses for the tests: a straight 6 km line, a stop every 400 m."""
import numpy as np
import pandas as pd

STOP_EVERY_M, N_STOPS, SPEED = 400.0, 15, 10.0     # 10 m/s = 36 km/h


def drive(t0, dwell_at, rng, ping_every=30.0, noise=3.0):
    """Exact path of one bus (time -> x) with dwells at some stops, then 30 s pings with GPS noise.
    Returns pings (t, x, y) and the departure time at every stop."""
    t, x = [t0], [0.0]
    dep = {}
    for i in range(N_STOPS):
        s = i * STOP_EVERY_M
        if i:
            t.append(t[-1] + STOP_EVERY_M / SPEED); x.append(s)
        if i in dwell_at:
            t.append(t[-1] + dwell_at[i]); x.append(s)
        dep[i] = t[-1]
    t, x = np.array(t), np.array(x)
    # real GPS: about every 30 s, but not exactly (otherwise pings lock onto the stop rhythm)
    pt = t0 + rng.uniform(0, ping_every) + np.cumsum(rng.uniform(0.6, 1.4, int(4 * (t[-1] - t0) / ping_every)) * ping_every)
    pt = pt[pt < t[-1]]
    px = np.interp(pt, t, x) + rng.normal(0, noise, len(pt))
    py = rng.normal(0, noise, len(pt))
    return pd.DataFrame({"t": pt, "x": px, "y": py}), dep


def line_day(seed=1, t0=1_761_300_000.0):
    """Two trips on line L1, 10 minutes apart, by vehicles A and B. Trip T1 (bus A) stops 25 s at
    stops 2, 5, 8 and drives past the rest; trip T2 (bus B) stops at every stop for 20 s."""
    rng = np.random.default_rng(seed)
    pa, depa = drive(t0, {2: 25, 5: 25, 8: 25}, rng)
    pb, depb = drive(t0 + 600, {i: 20 for i in range(N_STOPS)}, rng)
    pings = pd.concat([pa.assign(vehicle_id="A"), pb.assign(vehicle_id="B")], ignore_index=True)
    pings["line_ref"] = "L1"
    calls = []
    for trip, dep in [("T1", depa), ("T2", depb)]:
        for i, T in dep.items():
            calls.append({"kind": "call", "service_date": "2026-10-30", "trip_id": trip, "route_id": "L1",
                          "route_short_name": "1", "stop_id": f"S{i}", "stop_name": f"Stop {i}",
                          "stop_sequence": i + 1, "occ": 0, "sched_no_stop": False,
                          "visit_time_s": T, "x": i * STOP_EVERY_M, "y": 0.0, "shape_id": "SH1", "seq": None})
    shape = [{"kind": "shape", "shape_id": "SH1", "seq": k, "x": float(k * 50), "y": 0.0}
             for k in range(int((N_STOPS - 1) * STOP_EVERY_M / 50) + 1)]
    left = pd.DataFrame(calls + shape)
    left.attrs["dep"] = {"T1": depa, "T2": depb}
    return left, pings
