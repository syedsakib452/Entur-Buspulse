"""Did the bus actually stop? analyzing_parquet_files_v6.ipynb as a Spark job, one service date at a time.

Reads   <silver>/stop_event_status/service_date=D        (from clean.py: observed calls)
        <bronze>/vehicle_positions/date=D-1, D, D+1       (GPS around the day's calls)
        <bronze>/gtfs_stops, gtfs_trips, gtfs_shapes, gtfs_stop_times (nearest day)
Writes  <silver>/stop_gps_status/service_date=D     one row per observed call + GPS verdict + route evidence
        <silver>/trip_vehicle/service_date=D        which bus ran which trip
        <silver>/per_stop_evidence/service_date=D   stopped / passed counts per stop
        <silver>/time_lost_null/service_date=D      stop-free ping intervals (the p_excess comparison)

How memory stays small:
- GPS de-duplication, the time window and all joins are Spark operations (spill to disk if needed).
- Speeds are derived per vehicle (groupBy vehicle -> pandas), the stop test per line
  (cogroup of that line's calls/geometry with that line's GPS -> pandas): one vehicle or one line
  in memory at a time, in parallel worker processes.
- Only the stop-free time losses (one number per ping interval) come back to the driver, to rank
  each call's time loss against them (p_excess).

Differences from the notebook: no charts and no KS test (scipy) - this is a batch job; the summary
numbers are printed instead. Thresholds are in stop_geometry.py, unchanged.

Usage:  python -m spark_jobs.stop_analysis --date 2026-10-30
"""
import argparse

import numpy as np
import pandas as pd
from pyspark import StorageLevel
from pyspark.sql import functions as F

from spark_jobs import common
from spark_jobs import stop_geometry as geo

GPS_PAD_S = 2 * 3600          # GPS read from 2 h before the first to 2 h after the last call of the day
NULL_CAP = 2_000_000          # stop-free intervals brought to the driver at most (sampled above this)
LEFT_SCHEMA = ("kind string, service_date string, trip_id string, route_id string, route_short_name string, "
               "stop_id string, stop_name string, stop_sequence int, occ int, sched_no_stop boolean, "
               "visit_time_s double, x double, y double, shape_id string, seq double")


def _xy(lat, lon, lat0, lon0):
    m_lon = geo.M_LAT * float(np.cos(np.radians(lat0)))
    return (F.col(lon).cast("double") - lon0) * m_lon, (F.col(lat).cast("double") - lat0) * geo.M_LAT


def build_left(spark, day, obs, lat0, lon0):
    """Observed calls (kind 'call') + the route geometry of their trips (kind 'shape' / 'tripstop')."""
    def g(name):
        return spark.read.parquet(common.nearest_partition(spark, common.BRONZE, f"gtfs_{name}", day))

    stops = g("stops").dropDuplicates(["stop_id"]).select("stop_id", "stop_name", "stop_lat", "stop_lon")
    trips = g("trips")
    trips = trips.select("trip_id", (F.col("shape_id") if "shape_id" in trips.columns
                                     else F.lit(None).cast("string")).alias("shape_id")).dropDuplicates(["trip_id"])

    x, y = _xy("stop_lat", "stop_lon", lat0, lon0)
    calls = (obs.drop("stop_name").join(stops, "stop_id").join(trips, "trip_id", "left")
             .select(F.lit("call").alias("kind"), F.lit(day).alias("service_date"), "trip_id", "route_id",
                     "route_short_name", "stop_id", "stop_name", F.col("stop_sequence").cast("int"),
                     F.col("occ").cast("int"), F.col("sched_no_stop").cast("boolean"),
                     common.epoch_s("visit_time").alias("visit_time_s"),
                     x.alias("x"), y.alias("y"), "shape_id", F.lit(None).cast("double").alias("seq")))

    parts = [calls]
    used = calls.select("route_id", "shape_id").where(F.col("shape_id").isNotNull()).distinct()
    try:
        shapes = g("shapes")
        sx, sy = _xy("shape_pt_lat", "shape_pt_lon", lat0, lon0)
        shape_rows = (shapes.join(F.broadcast(used), "shape_id")
                      .select(F.lit("shape").alias("kind"), F.lit(None).cast("string").alias("service_date"),
                              F.lit(None).cast("string").alias("trip_id"), "route_id",
                              *[F.lit(None).cast(t).alias(c) for c, t in
                                [("route_short_name", "string"), ("stop_id", "string"), ("stop_name", "string"),
                                 ("stop_sequence", "int"), ("occ", "int"), ("sched_no_stop", "boolean"),
                                 ("visit_time_s", "double")]],
                              sx.alias("x"), sy.alias("y"), "shape_id",
                              F.col("shape_pt_sequence").cast("double").alias("seq"))
                      .where(F.col("x").isNotNull() & F.col("y").isNotNull()))
        parts.append(shape_rows)
        # trips without a shape_id, or whose shape is missing from gtfs_shapes
        no_shape_trips = (calls.select("trip_id", "route_id", "shape_id").distinct()
                          .join(shapes.select("shape_id").distinct(), "shape_id", "left_anti"))
    except RuntimeError:
        print("  no gtfs_shapes: every trip uses its stop-to-stop line")
        no_shape_trips = calls.select("trip_id", "route_id").distinct()

    # stop-to-stop line for trips without a shape: all their scheduled stops, in order
    st = g("stop_times").select("trip_id", "stop_id", F.col("stop_sequence").cast("double").alias("seq"))
    trip_rows = (st.join(F.broadcast(no_shape_trips.select("trip_id", "route_id")), "trip_id")
                 .join(stops.drop("stop_name"), "stop_id")
                 .select(F.lit("tripstop").alias("kind"), F.lit(None).cast("string").alias("service_date"),
                         "trip_id", "route_id",
                         *[F.lit(None).cast(t).alias(c) for c, t in
                           [("route_short_name", "string"), ("stop_id", "string"), ("stop_name", "string"),
                            ("stop_sequence", "int"), ("occ", "int"), ("sched_no_stop", "boolean"),
                            ("visit_time_s", "double")]],
                         x.alias("x"), y.alias("y"), F.lit(None).cast("string").alias("shape_id"), "seq"))
    parts.append(trip_rows)
    left = parts[0]
    for p in parts[1:]:
        left = left.unionByName(p)
    return left


def build_pings(spark, day, t_min, t_max, lat0, lon0):
    """De-duplicated GPS pings in the time window, with derived speeds (per vehicle, in pandas)."""
    src = common.path(common.BRONZE, "vehicle_positions")
    days = [d for d in [common.shift(day, -1), day, common.shift(day, 1)]
            if common.has_files(spark, f"{src}/date={d}", ".parquet")]
    if not days:
        raise RuntimeError(f"no bronze vehicle_positions around {day}")
    vp = (spark.read.option("basePath", src).parquet(*[f"{src}/date={d}" for d in days])
          .select("snapshot_time", "vehicle_id", "last_updated", "line_ref", "latitude", "longitude"))
    vp = vp.withColumn("ts", F.coalesce(common.ts("last_updated"), F.col("snapshot_time")))
    vp = vp.where(F.col("vehicle_id").isNotNull() & F.col("line_ref").isNotNull()
                  & F.col("latitude").isNotNull() & F.col("longitude").isNotNull() & F.col("ts").isNotNull())
    vp = vp.withColumn("t", common.epoch_s("ts")).where(F.col("t").between(t_min, t_max))
    # a position repeats in every snapshot until the bus reports a new one: keep one per vehicle + time
    vp = (vp.groupBy("vehicle_id", "t")
          .agg(F.max_by(F.struct("line_ref", "latitude", "longitude"), "snapshot_time").alias("p"))
          .select("vehicle_id", "t", "p.*"))
    x, y = _xy("latitude", "longitude", lat0, lon0)
    vp = vp.select("vehicle_id", "line_ref", "t", x.alias("x"), y.alias("y"))
    schema = "vehicle_id string, line_ref string, t double, x double, y double, speed_min double, ratio_min double"
    return vp.groupBy("vehicle_id").applyInPandas(geo.vehicle_speeds, schema)


def run(spark, day):
    print(f"=== stop analysis {day}")
    src = common.path(common.SILVER, "stop_event_status", f"service_date={day}")
    if not common.has_files(spark, src, ".parquet"):
        raise RuntimeError(f"no stop_event_status for {day}: run clean.py first")
    obs = spark.read.parquet(src).where(F.col("final_status") == "observed").where(F.col("visit_time").isNotNull())
    r = obs.agg(F.count("*").alias("n"), F.min(common.epoch_s("visit_time")).alias("t0"),
                F.max(common.epoch_s("visit_time")).alias("t1")).first()
    if not r["n"]:
        print("  no observed calls, nothing to do")
        return 0
    print(f"  observed calls: {r['n']:,}")

    stops = spark.read.parquet(common.nearest_partition(spark, common.BRONZE, "gtfs_stops", day))
    c = stops.agg(F.percentile_approx(F.col("stop_lat").cast("double"), 0.5).alias("lat"),
                  F.percentile_approx(F.col("stop_lon").cast("double"), 0.5).alias("lon")).first()
    lat0, lon0 = float(c["lat"]), float(c["lon"])

    left = build_left(spark, day, obs, lat0, lon0)
    pings = build_pings(spark, day, r["t0"] - GPS_PAD_S, r["t1"] + GPS_PAD_S, lat0, lon0) \
        .withColumnRenamed("line_ref", "route_id")

    res = (left.groupBy("route_id").cogroup(pings.groupBy("route_id"))
           .applyInPandas(lambda l, p: geo.process_line(l, p), geo.OUT_SCHEMA))
    res = res.persist(StorageLevel.DISK_ONLY)

    # ---- p_excess: share of stop-free intervals that lost at least as much time as this call
    null = res.where(F.col("row_type") == "null").select("time_lost_s")
    n_null = null.count()
    if n_null > NULL_CAP:
        null = null.sample(fraction=NULL_CAP / n_null, seed=1)
    null_sorted = np.sort(null.toPandas()["time_lost_s"].to_numpy(float)) if n_null else np.array([])
    bc = spark.sparkContext.broadcast(null_sorted)

    @F.pandas_udf("double")
    def p_excess(tl: pd.Series) -> pd.Series:
        arr = bc.value
        if not len(arr):
            return pd.Series(np.nan, index=tl.index)
        v = tl.to_numpy(dtype=float, na_value=np.nan)
        ge = len(arr) - np.searchsorted(arr, v, "left")
        return pd.Series(np.where(np.isnan(v), np.nan, (ge + 1) / (len(arr) + 1)), index=tl.index)

    calls = res.where(F.col("row_type") == "call")
    calls = calls.withColumns({
        "visit_time": F.timestamp_micros((F.col("visit_time_s") * 1e6).cast("long")),
        "p_excess": p_excess("time_lost_s"),
        "ts_offset_enter_s": F.col("gps_enter_t") - F.col("visit_time_s"),   # ET time vs bus entering the stop zone
        "ts_offset_leave_s": F.col("gps_leave_t") - F.col("visit_time_s"),   # ET time vs bus leaving it
    })
    calls = calls.withColumns({
        "gps_enter_time": F.timestamp_micros((F.col("gps_enter_t") * 1e6).cast("long")),
        "gps_leave_time": F.timestamp_micros((F.col("gps_leave_t") * 1e6).cast("long")),
        "local_hour": F.hour(common.local_time("visit_time")),          # UTC -> Oslo once, per row
    })
    keep = ["trip_id", "route_id", "route_short_name", "stop_id", "stop_name", "stop_sequence", "occ",
            "sched_no_stop", "visit_time", "vehicle_id", "assign_median_m", "d_at_T_m", "min_dist_m",
            "n_pings_window", "n_near", "n_near_slow", "slowest_near_ms", "slowest_near_ratio", "gps_class",
            "local_hour", "route_status", "geom_src", "stop_offroute_m", "local_gap_s", "time_lost_s", "p_excess",
            "ts_offset_enter_s", "ts_offset_leave_s", "gps_enter_time", "gps_leave_time"]
    out_calls = common.write(calls.select(*keep), common.SILVER, "stop_gps_status", "service_date", day, files=2)

    trips = res.where(F.col("row_type") == "trip").select("trip_id", "vehicle_id", "n_stops", "median_m", "runner_up_m")
    common.write(trips, common.SILVER, "trip_vehicle", "service_date", day, files=1)
    common.write(res.where(F.col("row_type") == "null").select("time_lost_s"),
                 common.SILVER, "time_lost_null", "service_date", day, files=1)

    # per stop: counts of evidence, not probabilities (a standing bus is more likely to be pinged)
    sg = spark.read.parquet(out_calls)
    per_stop = (sg.groupBy("stop_id")
                .agg(F.count("*").alias("calls"),
                     F.sum(F.col("gps_class").isin("stopped_confirmed", "likely_stopped").cast("int")).alias("stopped"),
                     F.sum((F.col("gps_class") == "likely_passed").cast("int")).alias("passed"))
                .join(stops.dropDuplicates(["stop_id"]).select("stop_id", "stop_name"), "stop_id", "left")
                .withColumn("conclusive", F.col("stopped") + F.col("passed"))
                .withColumn("passed_share", F.round(F.col("passed") / F.col("conclusive"), 3)))
    common.write(per_stop, common.SILVER, "per_stop_evidence", "service_date", day, files=1)

    # ---- summary (what the notebook printed)
    n_trips_obs = obs.select("trip_id").distinct().count()
    n_matched = spark.read.parquet(common.path(common.SILVER, "trip_vehicle", f"service_date={day}")).count()
    print(f"  trips with a matched vehicle: {n_matched:,} of {n_trips_obs:,}")
    classes = {r_["gps_class"]: r_["count"] for r_ in sg.groupBy("gps_class").count().collect()}
    total = sum(classes.values()) or 1
    for k in geo.GPS_CLASSES:
        print(f"    {k:<18} {classes.get(k, 0):>8,}  {classes.get(k, 0) / total:.1%}")
    okr = sg.where(F.col("route_status") == "ok")
    q = okr.agg(F.percentile_approx("ts_offset_enter_s", 0.5).alias("enter"),
                F.percentile_approx("ts_offset_leave_s", 0.5).alias("leave"),
                F.percentile_approx("time_lost_s", 0.5).alias("tl")).first()
    print(f"  median GPS-minus-ET offset: enter {q['enter']}, leave {q['leave']} s "
          "(the one closer to 0 tells whether ET records arrival or departure)")
    print(f"  time lost around stops: median {q['tl']} s | stop-free intervals: {n_null:,}, "
          f"median {np.median(null_sorted) if len(null_sorted) else float('nan'):.1f} s")
    res.unpersist()
    return total


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", required=True, help="service date, e.g. 2026-10-30")
    a = p.parse_args()
    spark = common.spark_session("buspulse-stop-analysis")
    run(spark, a.date)
    common.mark_done(spark, "stop_analysis", a.date)


if __name__ == "__main__":
    main()
