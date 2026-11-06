"""The GPS maths of analyzing_parquet_files_v6.ipynb, for ONE bus line (or ONE vehicle) at a time.

Spark (stop_analysis.py) splits the day into groups and calls these functions once per group:
- vehicle_speeds():  one vehicle's pings  -> derived speeds (v6 section 4)
- process_line():    one line's observed calls, route geometry and GPS tracks
                     -> which bus ran which trip (section 5), stopped or not (section 6),
                        evidence along the route (section 7b)
So only one vehicle or one line is ever in memory in a worker, instead of the whole day.

The code below is the notebook's, moved into functions; thresholds are unchanged.
Plain numpy/pandas on purpose: per-trip geometry (projection onto the route, isotonic
regression, interpolation) does not map onto SQL, but each group is small.
"""
import numpy as np
import pandas as pd

# ---- the stop test (v6 setup cell) ----
NEAR_M = 30        # a ping this close to the stop (metres) counts as "at the stop"
SLOW_MS = 2.0      # speed at or below this (m/s, about 7 km/h) = standing still
SLOW_RATIO = 0.75  # ...or at or below this share of the bus's usual speed nearby = slowed down for the stop
FAST_RATIO = 0.90  # at or above this share of its usual speed = driving past
MIN_CRUISE_MS = 3  # a usual speed below this (m/s) is too low to judge "relative to usual"
CRUISE_WIN = 11    # usual speed = looked up among the 11 neighbouring step speeds (about 5 minutes of driving)
CRUISE_Q = 0.75    # ...as their 75th percentile: the speed the bus reaches when it is not standing
WIN_S = 60         # look at pings within +-60 s of the recorded stop time
MAX_GAP_S = 90     # pings further apart than this are not joined into a track
MIN_DT_S = 5       # pings closer than this give a noisy speed, so no speed is derived

# ---- matching a vehicle to a trip ----
ASSIGN_MAX_M = 100    # median distance (m) between the bus and the trip's stops at the recorded times
ASSIGN_MIN_STOPS = 3  # ...judged on at least this many stops

# ---- v6 section 7b ----
R_STOP_M = 25              # "at the stop" = within this many metres ALONG the route
ANCHOR_WIN_M = 600         # a ping is snapped only to the route part where the ET times say the bus is (handles loops)
MAX_OFFROUTE_SHAPE = 60    # drop pings further than this (m) from the GTFS shape
MAX_OFFROUTE_STOPS = 150   # same when the route is only the stop-to-stop polyline (coarser)
MAX_STOP_OFFROUTE_M = 100  # a stop further than this from its route is not used
PAD_S = 120                # use pings this many seconds before the first / after the last recorded call
MIN_PINGS = 5              # shortest unbroken ping chunk that is used

M_LAT = 111_320.0


def make_to_xy(lat0, lon0):
    """Metres on a flat local grid around (lat0, lon0), accurate to about a metre at these distances."""
    m_lon = M_LAT * np.cos(np.radians(lat0))

    def to_xy(lat, lon):
        return (np.asarray(lon, dtype=float) - lon0) * m_lon, (np.asarray(lat, dtype=float) - lat0) * M_LAT
    return to_xy


# ------------------------------------------------------------------ section 4: speeds, one vehicle
SPEED_COLS = ["vehicle_id", "line_ref", "t", "x", "y", "speed_min", "ratio_min"]


def vehicle_speeds(pdf):
    """One vehicle's de-duplicated pings (t, x, y, line_ref) -> derived speed per ping.
    speed_min = slower of the step into and out of the ping; ratio_min = speed_min / the bus's usual
    speed around that moment (75th percentile of the neighbouring step speeds)."""
    vp = pdf.sort_values("t").reset_index(drop=True)
    dt = vp["t"].diff()
    step = np.hypot(vp["x"].diff(), vp["y"].diff())
    speed_in = (step / dt).where((dt >= MIN_DT_S) & (dt <= MAX_GAP_S))
    vp["speed_in"] = speed_in
    vp["speed_out"] = speed_in.shift(-1)
    vp["speed_min"] = np.fmin(vp["speed_in"], vp["speed_out"])
    cruise = vp["speed_in"].rolling(CRUISE_WIN, center=True, min_periods=5).quantile(CRUISE_Q)
    vp["ratio_min"] = (vp["speed_min"] / cruise).where(cruise >= MIN_CRUISE_MS)
    return vp[SPEED_COLS]


# ------------------------------------------------------------------ section 5 helpers
def dist_at_time(track, T, X, Y):
    # distance from (X, Y) to where the vehicle is at time T, by linear interpolation between the two pings
    # around T. NaN if the vehicle has no pair of pings (at most MAX_GAP_S apart) around T.
    t, x, y = track[:3]
    out = np.full(len(T), np.nan)
    if len(t) < 2:
        return out
    i1 = np.clip(np.searchsorted(t, T), 1, len(t) - 1)
    i0 = i1 - 1
    gap = t[i1] - t[i0]
    ok = (T >= t[i0]) & (T <= t[i1]) & (gap > 0) & (gap <= MAX_GAP_S)
    w = np.where(ok, (T - t[i0]) / np.where(gap > 0, gap, 1), 0.0)
    xi = x[i0] + w * (x[i1] - x[i0])
    yi = y[i0] + w * (y[i1] - y[i0])
    return np.where(ok, np.hypot(xi - X, yi - Y), np.nan)


# ------------------------------------------------------------------ section 7b helpers
def clean_poly(x, y):
    x, y = np.asarray(x, float), np.asarray(y, float)
    keep = np.r_[True, np.hypot(np.diff(x), np.diff(y)) > 0.5]
    x, y = x[keep], y[keep]
    if len(x) < 2:
        return None
    return x, y, np.r_[0.0, np.cumsum(np.hypot(np.diff(x), np.diff(y)))]


def project(px, py, poly, lo=None, hi=None):
    """Snap points to a polyline; lo/hi = allowed along-route window per point (m).
    Returns distance along the route and distance from the route."""
    x, y, cum = poly
    ax, ay, dx, dy = x[:-1], y[:-1], np.diff(x), np.diff(y)
    L2 = dx * dx + dy * dy
    u = np.clip(((px[:, None] - ax) * dx + (py[:, None] - ay) * dy) / L2, 0.0, 1.0)
    d2 = (px[:, None] - (ax + u * dx)) ** 2 + (py[:, None] - (ay + u * dy)) ** 2
    if lo is not None:
        d2 = np.where((cum[1:][None, :] >= lo[:, None]) & (cum[:-1][None, :] <= hi[:, None]), d2, np.inf)
    k = np.argmin(d2, axis=1)
    r = np.arange(len(px))
    return cum[k] + u[r, k] * np.sqrt(L2[k]), np.sqrt(d2[r, k])


def project_stops(sx, sy, poly):
    """Stops in trip order, each snapped at or after the previous one."""
    s, off = np.full(len(sx), np.nan), np.full(len(sx), np.nan)
    prev = 0.0
    for i in range(len(sx)):
        si, di = project(sx[i:i + 1], sy[i:i + 1], poly, lo=np.array([prev - 1.0]), hi=np.array([np.inf]))
        off[i] = di[0]
        if di[0] <= MAX_STOP_OFFROUTE_M:
            s[i], prev = si[0], si[0]
    return s, off


def pav(y):
    """Isotonic (non-decreasing) regression, pool-adjacent-violators."""
    vals, wts = [], []
    for v in y:
        vals.append(float(v)); wts.append(1)
        while len(vals) > 1 and vals[-2] > vals[-1]:
            v2, w2 = vals.pop(), wts.pop()
            vals[-1] = (vals[-1] * wts[-1] + v2 * w2) / (wts[-1] + w2)
            wts[-1] += w2
    return np.repeat(vals, wts)


def thin(t):
    keep, last = [], -np.inf
    for i, v in enumerate(t):
        if v - last >= MIN_DT_S:
            keep.append(i); last = v
    return np.array(keep, int)


ROUTE_COLS = ["s_stop_m", "stop_offroute_m", "gps_enter_t", "gps_leave_t", "local_gap_s", "time_lost_s"]


def trip_evidence(g, trk, poly_src):
    """One trip: along-route position of the bus, time lost around each stop, and the stop-free intervals.
    Returns (per-call columns as dict of arrays + route_status + geom_src, list of stop-free time losses)."""
    n = len(g)
    out = {c: np.full(n, np.nan) for c in ROUTE_COLS}
    status = np.full(n, "not_covered", dtype=object)
    null = []

    def done():
        out["route_status"] = status
        out["geom_src"] = np.full(n, poly_src[1] if poly_src else None, dtype=object)
        return out, null

    if poly_src is None:
        status[:] = "no_route_geometry"; return done()
    poly, src = poly_src
    if trk is None:
        status[:] = "no_vehicle_track"; return done()

    s_stop, off_stop = project_stops(g["x"].to_numpy(), g["y"].to_numpy(), poly)
    out["s_stop_m"], out["stop_offroute_m"] = s_stop, off_stop
    T = g["T"].to_numpy(float)
    ok = ~np.isnan(s_stop) & ~np.isnan(T)
    status[np.isnan(s_stop)] = "stop_offroute"
    if ok.sum() < 3:
        return done()
    aT, aS = np.maximum.accumulate(T[ok]), s_stop[ok]
    T_first, T_last = T[ok].min(), T[ok].max()

    t_all, x_all, y_all = trk[:3]
    i0, i1 = np.searchsorted(t_all, [T_first - PAD_S, T_last + PAD_S])
    t, x, y = t_all[i0:i1], x_all[i0:i1], y_all[i0:i1]
    if len(t) < MIN_PINGS:
        status[ok] = "few_pings"; return done()
    exp_s = np.interp(t, aT, aS)       # rough position from the ET times, only used to pick the right part of the route
    s_raw, off = project(x, y, poly, lo=exp_s - ANCHOR_WIN_M, hi=exp_s + ANCHOR_WIN_M)
    good = off <= (MAX_OFFROUTE_SHAPE if src == "shape" else MAX_OFFROUTE_STOPS)
    t, s_raw = t[good], s_raw[good]
    k = thin(t) if len(t) else np.array([], int)
    t, s_raw = t[k], s_raw[k]
    if len(t) < MIN_PINGS:
        status[ok] = "few_pings"; return done()

    s_iso = pav(s_raw)
    bounds = np.r_[0, np.where(np.diff(t) > MAX_GAP_S)[0] + 1, len(t)]       # never bridge a GPS hole
    stop_s_sorted = np.sort(s_stop[~np.isnan(s_stop)])
    assigned = np.zeros(n, bool)
    any_chunk = False
    for a, b in zip(bounds[:-1], bounds[1:]):
        if b - a < MIN_PINGS:
            continue
        any_chunk = True
        tc, sc = t[a:b], s_iso[a:b]
        dt_p, ds_p = np.diff(tc), np.diff(sc)
        cr = pd.Series(ds_p / dt_p).rolling(CRUISE_WIN, center=True, min_periods=5).quantile(CRUISE_Q).to_numpy()
        cr = np.where(cr >= MIN_CRUISE_MS, cr, np.nan)
        loss = dt_p - ds_p / cr                                               # seconds lost vs the bus's usual speed
        has_stop = (np.searchsorted(stop_s_sorted, sc[1:] + R_STOP_M, "right")
                    - np.searchsorted(stop_s_sorted, sc[:-1] - R_STOP_M, "left")) > 0
        in_trip = (tc[:-1] >= T_first) & (tc[1:] <= T_last)
        null.append(loss[~has_stop & in_trip & np.isfinite(loss)])            # intervals with no stop inside

        for i in np.where(ok & ~assigned)[0]:
            s0 = s_stop[i]
            if not (sc[0] - R_STOP_M <= s0 <= sc[-1] + R_STOP_M):
                continue
            touch = (sc[1:] >= s0 - R_STOP_M) & (sc[:-1] <= s0 + R_STOP_M)
            if touch.any() and np.isfinite(loss[touch]).any():
                out["time_lost_s"][i] = float(np.nanmax(loss[touch]))
            ie = int(np.searchsorted(sc, s0 - R_STOP_M, "left"))               # linear interpolation of enter / leave time
            if 0 < ie < len(sc):
                out["gps_enter_t"][i] = tc[ie - 1] + (s0 - R_STOP_M - sc[ie - 1]) / (sc[ie] - sc[ie - 1]) * (tc[ie] - tc[ie - 1])
                out["local_gap_s"][i] = tc[ie] - tc[ie - 1]
            il = int(np.searchsorted(sc, s0 + R_STOP_M, "right"))
            if 0 < il < len(sc):
                out["gps_leave_t"][i] = tc[il - 1] + (s0 + R_STOP_M - sc[il - 1]) / (sc[il] - sc[il - 1]) * (tc[il] - tc[il - 1])
            status[i] = "ok"
            assigned[i] = True
    if not any_chunk:
        status[ok] = "gps_gap"
    return done()


# ------------------------------------------------------------------ one line
CALL_COLS = ["service_date", "trip_id", "route_id", "route_short_name", "stop_id", "stop_name",
             "stop_sequence", "occ", "sched_no_stop", "visit_time_s", "x", "y"]
OUT_SCHEMA = (
    "row_type string, service_date string, trip_id string, route_id string, route_short_name string, "
    "stop_id string, stop_name string, stop_sequence int, occ int, sched_no_stop boolean, visit_time_s double, "
    "vehicle_id string, assign_median_m double, d_at_T_m double, min_dist_m double, n_pings_window int, "
    "n_near int, n_near_slow int, slowest_near_ms double, slowest_near_ratio double, gps_class string, "
    "route_status string, geom_src string, s_stop_m double, stop_offroute_m double, gps_enter_t double, "
    "gps_leave_t double, local_gap_s double, time_lost_s double, n_stops int, median_m double, runner_up_m double")
OUT_COLS = [f.strip().split(" ")[0] for f in OUT_SCHEMA.split(",")]
GPS_CLASSES = ["stopped_confirmed", "likely_stopped", "likely_passed", "near_unclear",
               "no_ping_at_stop", "no_gps", "no_vehicle"]


def _polylines(geom, calls):
    """trip_id -> (polyline, "shape" | "stops"): the GTFS shape if the trip has one, else its stop-to-stop line."""
    polys = {}
    shapes = geom[geom["kind"] == "shape"]
    by_shape = {}
    for sid, d in shapes.sort_values(["shape_id", "seq"]).groupby("shape_id", sort=False):
        p = clean_poly(d["x"], d["y"])
        if p is not None:
            by_shape[sid] = p
    shape_of = calls.drop_duplicates("trip_id").set_index("trip_id")["shape_id"]
    for trip, sid in shape_of.items():
        if sid in by_shape:
            polys[trip] = (by_shape[sid], "shape")
    tripstops = geom[(geom["kind"] == "tripstop") & ~geom["trip_id"].isin(set(polys))]
    for trip, d in tripstops.sort_values(["trip_id", "seq"]).groupby("trip_id", sort=False):
        p = clean_poly(d["x"], d["y"])
        if p is not None:
            polys[trip] = (p, "stops")
    return polys


def process_line(left, pings):
    """One line. left = observed calls (kind 'call') + route geometry (kind 'shape' / 'tripstop'),
    pings = that line's GPS (vehicle_id, t, x, y, speed_min, ratio_min).
    Returns rows of three kinds (row_type): 'call' (stop_gps_status), 'trip' (trip_vehicle) and
    'null' (one stop-free ping interval: its time_lost_s, for the p_excess comparison)."""
    calls = left[left["kind"] == "call"].copy()
    geom = left[left["kind"] != "call"]
    if calls.empty:
        return pd.DataFrame(columns=OUT_COLS)
    ev = calls.dropna(subset=["x", "y", "visit_time_s"]).reset_index(drop=True)
    ev["T"] = ev["visit_time_s"].astype(float)

    tracks = {}
    for veh, d in pings.sort_values(["vehicle_id", "t"]).groupby("vehicle_id", sort=False):
        tracks[veh] = tuple(d[c].to_numpy(dtype=float) for c in ["t", "x", "y", "speed_min", "ratio_min"])

    # ---- section 5: which bus ran which trip
    rows = []
    T, X, Y = ev["T"].to_numpy(), ev["x"].to_numpy(float), ev["y"].to_numpy(float)
    for veh, trk in tracks.items():
        d = dist_at_time(trk, T, X, Y)
        ok = ~np.isnan(d)
        if ok.any():
            rows.append(pd.DataFrame({"ev_id": ev.index.to_numpy()[ok], "vehicle_id": veh, "d": d[ok]}))
    if rows:
        m = pd.concat(rows, ignore_index=True)
        m["service_date"] = ev["service_date"].to_numpy()[m["ev_id"]]
        m["trip_id"] = ev["trip_id"].to_numpy()[m["ev_id"]]
        score = m.groupby(["service_date", "trip_id", "vehicle_id"])["d"].agg(n_stops="size", median_m="median").reset_index()
        score = score[score["n_stops"] >= ASSIGN_MIN_STOPS].sort_values(["service_date", "trip_id", "median_m"])
        score["rank"] = score.groupby(["service_date", "trip_id"]).cumcount()
        best = score[score["rank"] == 0].drop(columns="rank")
        runner = score[score["rank"] == 1][["service_date", "trip_id", "median_m"]].rename(columns={"median_m": "runner_up_m"})
        best = best.merge(runner, on=["service_date", "trip_id"], how="left")
        assigned = best[best["median_m"] <= ASSIGN_MAX_M].copy()
    else:
        assigned = pd.DataFrame(columns=["service_date", "trip_id", "vehicle_id", "n_stops", "median_m", "runner_up_m"])

    # ---- section 6: stopped or not
    ev = ev.merge(assigned[["service_date", "trip_id", "vehicle_id", "median_m"]]
                  .rename(columns={"median_m": "assign_median_m"}), on=["service_date", "trip_id"], how="left")
    res = {c: np.full(len(ev), np.nan) for c in ["d_at_T_m", "min_dist_m", "slowest_near_ms", "slowest_near_ratio"]}
    res["n_pings_window"] = np.zeros(len(ev), dtype=int)
    res["n_near"] = np.zeros(len(ev), dtype=int)
    res["n_near_slow"] = np.zeros(len(ev), dtype=int)
    for veh, e in ev[ev["vehicle_id"].notna()].groupby("vehicle_id", sort=False):
        trk = tracks[veh]
        t, x, y, v, r = trk
        idx = e.index.to_numpy()
        Te, Xe, Ye = e["T"].to_numpy(), e["x"].to_numpy(float), e["y"].to_numpy(float)
        res["d_at_T_m"][idx] = dist_at_time(trk, Te, Xe, Ye)
        lo = np.searchsorted(t, Te - WIN_S, side="left")
        hi = np.searchsorted(t, Te + WIN_S, side="right")
        min_d = np.full(len(e), np.nan)
        n_near = np.zeros(len(e), dtype=int)
        n_slow = np.zeros(len(e), dtype=int)
        slowest = np.full(len(e), np.nan)
        slowest_r = np.full(len(e), np.nan)
        for k in range(int((hi - lo).max()) if len(e) else 0):
            j = lo + k
            in_win = j < hi
            jj = np.where(in_win, j, 0)
            d = np.where(in_win, np.hypot(x[jj] - Xe, y[jj] - Ye), np.nan)
            near = in_win & (d <= NEAR_M)
            sp = np.where(near, v[jj], np.nan)
            rt = np.where(near, r[jj], np.nan)
            min_d = np.fmin(min_d, d)
            n_near += near
            n_slow += near & ((sp <= SLOW_MS) | (rt <= SLOW_RATIO))
            slowest = np.fmin(slowest, sp)
            slowest_r = np.fmin(slowest_r, rt)
        res["n_pings_window"][idx] = hi - lo
        res["min_dist_m"][idx] = min_d
        res["n_near"][idx] = n_near
        res["n_near_slow"][idx] = n_slow
        res["slowest_near_ms"][idx] = slowest
        res["slowest_near_ratio"][idx] = slowest_r
    stop_gps = pd.concat([ev, pd.DataFrame(res, index=ev.index)], axis=1)
    stop_gps["gps_class"] = np.select(
        [stop_gps["vehicle_id"].isna(),
         stop_gps["d_at_T_m"].isna(),
         stop_gps["n_near_slow"] >= 2,
         stop_gps["n_near_slow"] == 1,
         (stop_gps["n_near"] > 0) & (stop_gps["slowest_near_ratio"] >= FAST_RATIO),
         stop_gps["n_near"] > 0],
        ["no_vehicle", "no_gps", "stopped_confirmed", "likely_stopped", "likely_passed", "near_unclear"],
        default="no_ping_at_stop")

    # ---- section 7b: evidence along the route
    polys = _polylines(geom, calls)
    for c in ROUTE_COLS:
        stop_gps[c] = np.nan
    stop_gps["route_status"] = None
    stop_gps["geom_src"] = None
    nulls = []
    with_veh = stop_gps[stop_gps["vehicle_id"].notna()]
    for _, g in with_veh.groupby(["service_date", "trip_id"], sort=False):
        g = g.sort_values("stop_sequence")
        out, nl = trip_evidence(g, tracks.get(g["vehicle_id"].iloc[0]), polys.get(g["trip_id"].iloc[0]))
        for c, vals in out.items():
            stop_gps.loc[g.index, c] = vals
        nulls.extend(nl)

    stop_gps["row_type"] = "call"
    trips = assigned.assign(row_type="trip")
    null = np.concatenate(nulls) if nulls else np.array([])
    null_rows = pd.DataFrame({"row_type": "null", "time_lost_s": null})
    out = pd.concat([stop_gps, trips, null_rows], ignore_index=True)
    for c in OUT_COLS:
        if c not in out.columns:
            out[c] = None
    out = out[OUT_COLS]
    for c in ["stop_sequence", "occ", "n_pings_window", "n_near", "n_near_slow", "n_stops"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int32")
    for c in ["visit_time_s", "assign_median_m", "d_at_T_m", "min_dist_m", "slowest_near_ms", "slowest_near_ratio",
              "s_stop_m", "stop_offroute_m", "gps_enter_t", "gps_leave_t", "local_gap_s", "time_lost_s",
              "median_m", "runner_up_m"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")
    out["sched_no_stop"] = out["sched_no_stop"].astype("boolean")
    for c in ["row_type", "service_date", "trip_id", "route_id", "route_short_name", "stop_id", "stop_name",
              "vehicle_id", "gps_class", "route_status", "geom_src"]:
        out[c] = out[c].astype("object").where(out[c].notna(), None)
    return out
