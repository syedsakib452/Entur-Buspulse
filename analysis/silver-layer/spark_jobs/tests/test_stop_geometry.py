"""Tests for the per-line GPS maths (no Spark needed)."""
import numpy as np
import pandas as pd

from spark_jobs import stop_geometry as g
from spark_jobs.tests.sim import line_day


def _run(seed=1):
    left, pings = line_day(seed)
    sp = pd.concat([g.vehicle_speeds(d) for _, d in pings.groupby("vehicle_id")], ignore_index=True)
    return g.process_line(left, sp), sp


def test_speeds_are_derived_and_plausible():
    _, sp = _run()
    assert set(sp.columns) == set(g.SPEED_COLS)
    assert sp["speed_min"].notna().mean() > 0.8
    a = sp[sp["vehicle_id"] == "A"]                 # drives at 10 m/s, stops at 3 of 15 stops
    assert 8 < a["speed_min"].quantile(0.75) < 11
    assert a["speed_min"].min() < 6                 # the dwells show up as slow steps


def test_each_trip_gets_its_own_bus():
    out, _ = _run()
    trips = out[out["row_type"] == "trip"].set_index("trip_id")
    assert trips.loc["T1", "vehicle_id"] == "A"
    assert trips.loc["T2", "vehicle_id"] == "B"
    assert (trips["median_m"] < g.ASSIGN_MAX_M).all()
    assert (trips["runner_up_m"].isna() | (trips["runner_up_m"] > 1000)).all()   # the other bus is 10 min away


def test_output_shape_and_types():
    out, _ = _run()
    assert list(out.columns) == g.OUT_COLS
    calls = out[out["row_type"] == "call"]
    assert len(calls) == 30
    assert set(calls["gps_class"]) <= set(g.GPS_CLASSES)
    assert set(calls["route_status"].dropna()) <= {"ok", "not_covered", "few_pings", "gps_gap", "stop_offroute",
                                                     "no_route_geometry", "no_vehicle_track"}


def test_stops_lose_time_and_passes_do_not():
    lost_stop, lost_pass = [], []
    for seed in range(1, 6):
        out, _ = _run(seed)
        t1 = out[(out["row_type"] == "call") & (out["trip_id"] == "T1")].set_index("stop_sequence")
        inner = t1.loc[2:14]                      # first/last stop have no interval on both sides
        stopped = inner.index.isin([3, 6, 9])     # stops 2, 5, 8 (0-based) = stop_sequence 3, 6, 9
        lost_stop += list(inner.loc[stopped, "time_lost_s"].dropna())
        lost_pass += list(inner.loc[~stopped, "time_lost_s"].dropna())
    assert np.median(lost_stop) > 12              # ~25 s dwell shows up as lost time
    assert np.median(lost_pass) < 5


def test_bus_that_stops_everywhere_is_never_called_passed_and_loses_time():
    classes, lost = [], []
    for seed in range(1, 6):
        out, _ = _run(seed)
        t2 = out[(out["row_type"] == "call") & (out["trip_id"] == "T2")]
        classes += list(t2["gps_class"])
        lost += list(t2["time_lost_s"].dropna())
    classes = pd.Series(classes)
    assert (classes != "likely_passed").mean() > 0.95
    assert classes.isin(["stopped_confirmed", "likely_stopped"]).mean() > 0.2
    assert np.median(lost) > 12                   # 20 s at every stop


def test_null_intervals_exist():
    out, _ = _run()
    null = out[out["row_type"] == "null"]["time_lost_s"]
    assert len(null) > 5 and null.notna().all()


def test_line_without_gps_gives_no_vehicle():
    left, pings = line_day()
    out = g.process_line(left, pings.iloc[0:0].assign(speed_min=[], ratio_min=[]))
    calls = out[out["row_type"] == "call"]
    assert (calls["gps_class"] == "no_vehicle").all()
    assert (out["row_type"] == "trip").sum() == 0


def test_empty_line():
    left, pings = line_day()
    out = g.process_line(left[left["kind"] != "call"], pings)
    assert out.empty and list(out.columns) == g.OUT_COLS
