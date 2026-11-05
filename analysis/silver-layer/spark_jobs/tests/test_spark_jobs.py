"""End-to-end test of the Spark pipeline on a small simulated day (tests/fixtures.py).

Runs raw -> bronze -> clean -> stop_analysis in local mode against local folders (DATA_ROOT),
exactly as the CronJob runs it against Swift, and checks the results the notebooks would give.
Needs Java 17 and pyspark (the CI workflow installs both).
"""
import os
import sys
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pytest

pyspark = pytest.importorskip("pyspark")

from spark_jobs.tests import fixtures as fx  # noqa: E402

DAY, NEXT = fx.DAY, fx.NEXT
OSLO = ZoneInfo("Europe/Oslo")


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    root = tmp_path_factory.mktemp("data")
    os.environ.update({"DATA_ROOT": str(root), "SPARK_LOCAL_DIR": str(root / "_spark"),
                       "SPARK_SHUFFLE_PARTITIONS": "4"})
    os.environ.pop("S3_ENDPOINT", None)
    facts = fx.write_raw(root)
    from spark_jobs import run_pipeline
    argv = sys.argv
    sys.argv = ["run_pipeline", "--start", DAY, "--end", NEXT]
    try:
        with pytest.raises(SystemExit) as ex:
            run_pipeline.main()
    finally:
        sys.argv = argv
    from spark_jobs import common
    spark = common.spark_session("tests")
    yield {"root": root, "facts": facts, "exit": ex.value.code, "spark": spark}
    spark.stop()


def table(r, bucket, name, key, day=DAY):
    return r["spark"].read.parquet(str(r["root"] / bucket / name / f"{key}={day}")).toPandas()


def test_pipeline_succeeds_and_marks_days(run):
    assert run["exit"] == 0
    done = run["root"] / "buspulse-silver" / "_done"
    assert (done / "bronze" / DAY).exists() and (done / "bronze" / NEXT).exists()
    assert (done / "clean" / DAY).exists() and (done / "stop_analysis" / DAY).exists()
    assert not (done / "clean" / NEXT).exists()          # waits for bronze of the day after


def test_bronze_tables(run):
    vp = table(run, "buspulse-bronze", "vehicle_positions", "date")
    assert len(vp) == run["facts"]["n_vp_rows"]
    sv = table(run, "buspulse-bronze", "stop_visits", "date")
    assert sv["source_file"].nunique() == run["facts"]["n_et_files_day"]
    assert {"T1", "T2", "FT1", "FT2"} <= set(sv["journey_ref"])
    assert len(table(run, "buspulse-bronze", "service_alerts", "date")) == 1
    trips = table(run, "buspulse-bronze", "gtfs_trips", "date")
    assert "route_id" in trips.columns                   # byte-order mark stripped from the header
    assert len(table(run, "buspulse-bronze", "gtfs_stop_times", "date")) == 15 * 3 + 2 * 2


def test_clean_visits(run):
    c = table(run, "buspulse-silver", "clean_stop_visits", "service_date")
    assert set(c["journey_ref"]) == {"T1", "T2"}         # ferries gone, also FT2 without a vehicle_mode
    assert len(c) == 30                                  # one row per call after dedup
    t1 = c[c["journey_ref"] == "T1"].set_index("stop_order")
    assert t1.loc[1, "actual_departure"] is not None     # rescued from an older snapshot
    t2 = c[c["journey_ref"] == "T2"]
    assert t2["actual_departure"].notna().all()          # late actuals from the next day's file
    assert c["delay_s"].between(-1800, 10800).all()
    assert (c["local_hour"] == 8).any()                  # 07:xx UTC = 08:xx in Norway after 25 Oct
    assert set(c["period"]) == {"am_peak"}
    assert set(c["delay_class"]) <= {"early", "on_time", "late"}
    assert c["is_monitored"].all()
    assert c["delay_change_s"].notna().sum() == 28       # every stop after the first of each trip


def test_visit_time_is_utc_and_converted_once(run):
    """visit_time stays the UTC instant of the feed's actual time; local_hour is derived from it once."""
    c = table(run, "buspulse-silver", "clean_stop_visits", "service_date")
    dep = run["facts"]["trips"]["T1"][1]
    t1 = c[c["journey_ref"] == "T1"].sort_values("stop_order")
    got = [pd_ts.timestamp() for pd_ts in t1["visit_time"]]          # naive = UTC (session time zone)
    assert max(abs(g - e) for g, e in zip(got, dep)) < 1.0
    expect = [(datetime.fromtimestamp(e, timezone.utc).astimezone(OSLO)).hour for e in dep]
    assert list(t1["local_hour"]) == expect


def test_local_time_across_dst(run):
    from pyspark.sql import functions as F
    from spark_jobs import common
    rows = [("2026-10-24T22:30:00Z", 0, "2026-10-25", "Sunday"),   # CEST (+2): just after local midnight
            ("2026-10-25T00:30:00Z", 2, "2026-10-25", "Sunday"),   # 02:30 CEST, before the change
            ("2026-10-25T01:30:00Z", 2, "2026-10-25", "Sunday"),   # 02:30 CET, after it (hour repeats)
            ("2026-10-25T02:30:00Z", 3, "2026-10-25", "Sunday"),
            ("2026-10-30T07:00:00Z", 8, "2026-10-30", "Friday"),
            ("2026-07-01T07:00:00Z", 9, "2026-07-01", "Wednesday")]  # summer: +2
    df = run["spark"].createDataFrame([(r[0],) for r in rows], "t string").withColumn("t", common.ts("t"))
    got = df.withColumns(common.local_fields("t")).collect()
    assert [(g["local_hour"], g["local_date"], g["weekday"]) for g in got] == [r[1:] for r in rows]


def test_day_complete():
    from spark_jobs import common
    assert common.day_complete("2026-10-30")
    assert not common.day_complete(datetime.now(timezone.utc).date().isoformat())


def test_stop_event_status(run):
    s = table(run, "buspulse-silver", "stop_event_status", "service_date")
    counts = s["final_status"].value_counts().to_dict()
    assert counts.get("observed") == 30
    assert counts.get("trip_not_in_feed") == 15          # T3 is scheduled but never in the feed
    assert not s["trip_id"].str.startswith("F").any()
    log = table(run, "buspulse-silver", "removal_log", "service_date")
    assert (log["removed"] >= 0).all()


def test_stop_headways(run):
    h = table(run, "buspulse-silver", "stop_headways", "service_date")
    assert len(h) == 15                                  # T2 follows T1 at each of the 15 stops
    assert (h["trip_id"] == "T2").all() and (h["previous_trip_id"] == "T1").all()
    assert (h["scheduled_headway_s"] == 600).all()
    assert h["headway_s"].between(550, 1000).all()
    assert not h["is_bunched"].any() and not h["is_large_gap"].any()
    c = table(run, "buspulse-silver", "clean_stop_visits", "service_date")
    assert c["headway_s"].notna().sum() == 15


def test_trip_segments(run):
    s = table(run, "buspulse-silver", "trip_segments", "service_date")
    assert len(s) == 28 and s["is_valid"].all()          # 14 stop-to-stop runs per trip
    assert s["segment_distance_m"].between(395, 405).all()
    assert (s["scheduled_running_time_s"] == 50).all()
    assert s["average_speed_kmh"].between(15, 40).all()


def test_stop_analysis(run):
    tv = table(run, "buspulse-silver", "trip_vehicle", "service_date").set_index("trip_id")
    assert tv.loc["T1", "vehicle_id"] == "A" and tv.loc["T2", "vehicle_id"] == "B"
    g = table(run, "buspulse-silver", "stop_gps_status", "service_date")
    assert len(g) == 30
    assert set(g["gps_class"]) <= {"stopped_confirmed", "likely_stopped", "likely_passed", "near_unclear",
                                   "no_ping_at_stop", "no_gps", "no_vehicle"}
    ok = g[g["route_status"] == "ok"]
    assert len(ok) > 20
    assert g["p_excess"].dropna().between(0, 1).all()
    assert len(table(run, "buspulse-silver", "time_lost_null", "service_date")) > 0
    assert len(table(run, "buspulse-silver", "per_stop_evidence", "service_date")) == 15
