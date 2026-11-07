"""Silver for one service date: the cleaning of building_silver_layer_v2.ipynb (v3) in Spark,
plus headways and stop-to-stop segments.

Reads   <bronze>/stop_visits/date=D and date=D+1      (only the needed columns)
        <bronze>/gtfs_*/date=D                         (or the nearest day that has a timetable)
Writes  <silver>/clean_stop_visits/service_date=D      one row per observed bus visit
        <silver>/stop_event_status/service_date=D      every scheduled bus call + what the feed says
        <silver>/expected_per_stop/service_date=D     scheduled bus visits per stop
        <silver>/removal_log/service_date=D           rows removed by each cleaning rule
        <silver>/stop_headways/service_date=D         time since the previous bus of the line at the stop
        <silver>/trip_segments/service_date=D         stop-to-stop running times of each trip

Time: every timestamp is stored as a UTC instant and never overwritten. local_date, local_hour,
weekday and period are derived from visit_time (the actual event) once, row by row, with the
Europe/Oslo rules (common.local_fields), so days with a DST change (25 Oct 2026) are right.

Why D and D+1: a trip of service date D (the feed's data_frame_ref) gets its final, actual times
in snapshots up to the early hours of D+1 (UTC). So D+1's raw data must exist before D is cleaned.

Rules (same as v3):
 1. keep service date D, drop rows without data_frame_ref / journey_ref / stop_order / stop_ref
 2. one row per visit (data_frame_ref, journey_ref, stop_order): the newest snapshot, with actual
    times taken from the newest snapshot that has them (they can be missing in the very last one)
 3. exclude ferry lines (by the feed's vehicle_mode or by GTFS route_type)
 4. keep bus route types only (3, 700-799), and stops that exist in GTFS
 5. visit_status: cancelled / observed (has an actual time) / unconfirmed
 6. compare with the timetable: every scheduled bus call of D gets final_status

Memory: the millions of snapshot rows are reduced to one row per visit with a grouped aggregation,
which Spark does in partial steps and spills to disk; nothing is collected to the driver except
the small calendar tables.

Usage:  python -m spark_jobs.silver --date 2026-10-30 [--force]
        Refuses to run until bronze of D and D+1 is complete (see common.bronze_ready); --force skips
        that check.
"""
import argparse
from datetime import date
from pathlib import Path

import pandas as pd
from pyspark.sql import Window, functions as F

from spark_jobs import common

KEY = ["data_frame_ref", "journey_ref", "stop_order"]          # one real visit = this key
ACTUALS = ["actual_arrival", "actual_departure"]
TIME_COLS = ["aimed_arrival", "expected_arrival", "actual_arrival",
             "aimed_departure", "expected_departure", "actual_departure"]
ET_COLS = ["snapshot_time", "data_frame_ref", "journey_ref", "line_ref", "direction_ref", "vehicle_mode",
           "monitored", "journey_cancelled", "call_cancelled", "call_type", "stop_order", "stop_ref",
           *TIME_COLS, "arrival_status", "departure_status", "prediction_inaccurate"]
BRONZE = Path(common.tables_dir() or "tables") / "bronze"
GTFS_REQUIRED = ["routes", "stops", "trips", "stop_times"]

DELAY_MIN_S, DELAY_MAX_S = -30 * 60, 3 * 3600         # outlier bounds: > 30 min early or > 3 h late
ON_TIME_EARLY_S, ON_TIME_LATE_S = -60, 180            # on time = 60 s early ... 180 s late (inclusive)
EARLY_S, LATE_S = -60, 300                            # delay_class: early < -60 s, late > 300 s (as in gold)
BUNCH_RATIO, GAP_RATIO = 0.25, 2.0                    # headway < 0.25 x scheduled = bunched, > 2 x = gap
SEGMENT_MAX_S = 3600                                  # a stop-to-stop run longer than 1 h is not plausible
EARTH_M = 6_371_000.0
BUS_ROUTE_TYPES = ["3"] + [str(x) for x in range(700, 800)]
ROUTE_TYPE_NAMES = {"704": "Local bus", "705": "Night bus", "712": "School bus",
                    "1004": "Local car ferry", "1008": "Local passenger ferry",
                    "1014": "High-speed passenger ferry"}
WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]


# ------------------------------------------------------------------ timetable
def _find_file(day_dir: Path, name: str):
    for ext in (".parquet", ".csv", ".txt"):
        for stem in (f"gtfs_{name}", name):
            p = day_dir / f"{stem}{ext}"
            if p.exists():
                return p
    return None


def _gtfs_dir(day: str) -> Path:
    """Latest local snapshot <= day that has the GTFS files; if none (first days), the earliest after."""
    candidates = []
    for d in BRONZE.iterdir():
        try:
            date.fromisoformat(d.name)
        except ValueError:
            continue
        if d.is_dir() and all(_find_file(d, n) for n in GTFS_REQUIRED):
            candidates.append(d.name)
    if not candidates:
        raise FileNotFoundError(f"no GTFS files found anywhere under {BRONZE}")
    candidates.sort()
    before = [c for c in candidates if c <= day]
    return BRONZE / (before[-1] if before else candidates[0])


def read_gtfs(spark, day):
    d = _gtfs_dir(day)
    print(f"gtfs for {day}: using snapshot {d.name}")

    def t(name, required=True):
        p = _find_file(d, name)
        if p is None:
            if required:
                raise FileNotFoundError(f"gtfs {name} missing in {d}")
            return None
        uri = p.resolve().as_uri()                      # file:///... so Spark reads local disk
        if p.suffix == ".parquet":
            return spark.read.parquet(uri)
        return spark.read.option("header", True).csv(uri)   # all strings: keeps "25:10:00" and "0012" intact

    name_map = F.create_map(*[x for k, v in ROUTE_TYPE_NAMES.items() for x in (F.lit(k), F.lit(v))])
    routes = (t("routes").dropDuplicates(["route_id"])
              .select("route_id", "route_short_name", F.trim("route_type").alias("route_type"))
              .withColumns({"route_type_name": F.coalesce(name_map[F.col("route_type")], F.lit("unknown")),
                            "is_bus": F.col("route_type").isin(BUS_ROUTE_TYPES)}))
    stops_raw = t("stops")
    stops = (stops_raw.dropDuplicates(["stop_id"])
             .select("stop_id", "stop_name", F.col("stop_lat").cast("double").alias("stop_lat"),
                     F.col("stop_lon").cast("double").alias("stop_lon"),
                     (F.col("parent_station") if "parent_station" in stops_raw.columns
                      else F.lit(None).cast("string")).alias("parent_station")))
    trips = t("trips").select("trip_id", "route_id", "service_id")
    st_cols = ["trip_id", "stop_id", "arrival_time", "departure_time", "stop_sequence"]
    st = t("stop_times")
    st = st.select(*st_cols,
                   *[F.col(c) if c in st.columns else F.lit(None).cast("string").alias(c)
                     for c in ["pickup_type", "drop_off_type"]])

    cal = t("calendar", required=False)                  # small: a few hundred rows
    cal_dates = t("calendar_dates", required=False)
    calendar = cal.toPandas() if cal is not None else pd.DataFrame()
    calendar_dates = cal_dates.toPandas() if cal_dates is not None else pd.DataFrame()
    return routes, stops, trips, st, calendar, calendar_dates

def active_service_ids(calendar, calendar_dates, service_date):
    """service_ids running on service_date: calendar range + weekday, plus added, minus removed."""
    d = pd.Timestamp(service_date)
    ds = d.strftime("%Y%m%d")
    wd = d.day_name().lower()
    base = set()
    if {"service_id", "start_date", "end_date", wd} <= set(calendar.columns):
        on = calendar[wd].astype(str).str.strip().eq("1")
        base = set(calendar[
            (calendar["start_date"] <= ds) & (calendar["end_date"] >= ds) & on
        ]["service_id"])
    added, removed = set(), set()
    if {"date", "exception_type", "service_id"} <= set(calendar_dates.columns):
        exc = calendar_dates[calendar_dates["date"] == ds]
        typ = exc["exception_type"].astype(str).str.strip()
        added = set(exc.loc[typ == "1", "service_id"])
        removed = set(exc.loc[typ == "2", "service_id"])
    return (base | added) - removed


def expected_calls(spark, day, routes, trips, st, calendar, calendar_dates):
    """Every scheduled bus call that the feed labels with service date `day`.
    GTFS counts a trip starting after midnight ("24:30:00") under the previous service day, but the
    feed labels it with the clock date of its start, so the previous day is built too."""
    # ordered stop calls per trip; occ = repeated call at the same stop (loops)
    w_trip = Window.partitionBy("trip_id").orderBy("stop_sequence")
    st = (st.withColumn("stop_sequence", F.col("stop_sequence").cast("int"))
          .withColumns({"occ": F.row_number().over(Window.partitionBy("trip_id", "stop_id")
                                                   .orderBy("stop_sequence")) - 1,
                        "stop_rank": F.row_number().over(w_trip),
                        "first_dep": F.first("departure_time").over(
                            w_trip.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)),
                        "sched_no_stop": (F.trim("pickup_type") == "1") & (F.trim("drop_off_type") == "1")}))
    st = st.withColumn("day_shift", (F.substring("first_dep", 1, 2).cast("int") / 24).cast("int"))

    # (service_id, GTFS service date, local "noon minus 12 h" anchor in UTC seconds) for D-1 and D
    rows = []
    for s in [common.shift(day, -1), day]:
        anchor = (pd.Timestamp(f"{s} 12:00", tz=common.LOCAL_TZ) - pd.Timedelta(hours=12)).tz_convert("UTC")
        for sid in active_service_ids(calendar, calendar_dates, s):
            rows.append((sid, s, int(anchor.timestamp())))
    active = spark.createDataFrame(rows, "service_id string, gtfs_service_date string, anchor_s long")

    bus_trips = (trips.join(routes.where("is_bus").select("route_id", "route_short_name", "route_type",
                                                          "route_type_name", "is_bus"), "route_id")
                 .join(F.broadcast(active), "service_id"))
    ev = st.join(bus_trips, "trip_id")
    ev = ev.withColumn("service_date", F.date_format(
        F.date_add(F.to_date("gtfs_service_date"), F.coalesce(F.col("day_shift"), F.lit(0))), "yyyy-MM-dd"))
    ev = ev.where(F.col("service_date") == day)
    return ev.withColumns({
        "sched_arrival": F.timestamp_seconds(F.col("anchor_s") + common.gtfs_seconds("arrival_time")),
        "sched_departure": F.timestamp_seconds(F.col("anchor_s") + common.gtfs_seconds("departure_time")),
    }).drop("anchor_s", "first_dep", "day_shift")


# ------------------------------------------------------------------ real-time
def read_feed(spark, day):
    """Snapshot rows of service date `day` from the two bronze days that contain them."""
    days = [d for d in [day, common.shift(day, 1)]
            if common.has_files(spark, common.path(common.BRONZE, "stop_visits", f"date={d}"), ".parquet")]
    if day not in days:
        raise RuntimeError(f"no bronze stop_visits for {day}")
    if len(days) < 2:
        print(f"  WARNING: no bronze stop_visits for {common.shift(day, 1)} yet: late trips of {day} "
              "may miss their final times")
    if common.tables_dir():
        raw = spark.read.parquet(*[
            common.path(common.BRONZE, "stop_visits", f"date={d}") for d in days
        ])
    else:
        src = common.path(common.BRONZE, "stop_visits")
        raw = spark.read.option("basePath", src).parquet(*[f"{src}/date={d}" for d in days])
    not_ferry = F.col("vehicle_mode").isNull() | (F.col("vehicle_mode") != "ferry")   # null-safe
    return raw, raw.where(not_ferry).select(*ET_COLS)


def clean_visits(spark, day, raw, et, routes, stops, log):
    # counts for the removal log in one pass (columnar scan of 2-3 columns)
    keyed = F.col("data_frame_ref").isNotNull() & F.col("journey_ref").isNotNull() \
        & F.col("stop_order").isNotNull() & F.col("stop_ref").isNotNull()
    c = et.agg(F.count("*").alias("n"),
               F.sum((F.col("data_frame_ref") == day).cast("int")).alias("n_day"),
               F.sum(((F.col("data_frame_ref") == day) & keyed).cast("int")).alias("n_key")).first()
    log("keep chosen service dates", c["n"], c["n_day"] or 0)
    log("drop rows with null key / stop_ref", c["n_day"] or 0, c["n_key"] or 0)

    et = et.where((F.col("data_frame_ref") == day) & keyed) \
           .withColumns({a: common.blank_to_null(a) for a in ACTUALS})

    # rule SV-01: newest snapshot per visit; actual_* from the newest snapshot that has them
    others = [x for x in ET_COLS if x not in KEY + ["snapshot_time"] + ACTUALS]
    agg = [F.max_by(F.struct(*others), "snapshot_time").alias("s"),
           F.max("snapshot_time").alias("snapshot_time"),
           F.count("*").alias("n_snapshots")]
    agg += [F.max_by(a, F.when(F.col(a).isNotNull(), F.col("snapshot_time"))).alias(a) for a in ACTUALS]
    v = et.groupBy(*KEY).agg(*agg).select(*KEY, "snapshot_time", "n_snapshots", *ACTUALS, "s.*")
    v = v.persist()
    n_visits = v.count()
    log("de-duplicate snapshots (keep latest per visit)", c["n_key"] or 0, n_visits)

    v = v.withColumns({col: common.ts(col) for col in TIME_COLS})

    # ferry lines: by the feed's vehicle_mode (any snapshot) or by GTFS route_type
    rt = F.col("route_type").cast("int")
    ferry_by_type = routes.where((rt == 4) | rt.between(1000, 1099) | rt.between(1200, 1299)) \
                          .select(F.col("route_id").alias("line_ref"))
    ferry_by_mode = raw.where(F.col("vehicle_mode") == "ferry").select("line_ref").distinct()
    ferry = ferry_by_type.union(ferry_by_mode).distinct()
    v2 = v.join(F.broadcast(ferry), "line_ref", "left_anti")
    n_ferry = v2.count()
    log("exclude ferry lines (vehicle_mode or route_type)", n_visits, n_ferry)

    v2 = (v2.join(F.broadcast(routes.select(F.col("route_id").alias("line_ref"), "route_short_name",
                                            "route_type", "route_type_name", "is_bus")), "line_ref", "left")
          .join(F.broadcast(stops.select(F.col("stop_id").alias("stop_ref"), "stop_name", "stop_lat",
                                         "stop_lon", "parent_station")), "stop_ref", "left"))
    v3 = v2.where(F.coalesce("is_bus", F.lit(False)))
    n_bus = v3.count()
    log("keep bus route types only (3, 700-799)", n_ferry, n_bus)
    v4 = v3.where(F.col("stop_name").isNotNull())
    n_stop = v4.count()
    log("keep stops that exist in GTFS", n_bus, n_stop)

    # classify the visit, one event per visit: departure if it has an actual departure, else arrival
    cancelled = common.to_bool("journey_cancelled") | common.to_bool("call_cancelled")
    has_dep, has_arr = F.col("actual_departure").isNotNull(), F.col("actual_arrival").isNotNull()
    v4 = v4.withColumns({
        "cancelled": cancelled,
        "visit_status": F.when(cancelled, "cancelled").when(has_dep | has_arr, "observed").otherwise("unconfirmed"),
        "is_actual": F.coalesce(F.col("call_type") == "recorded", F.lit(False)),
        "delay_event": F.when(has_dep, "departure").when(has_arr, "arrival"),
        "visit_time": F.coalesce("actual_departure", "actual_arrival"),
        "aimed_time": F.when(has_dep, F.col("aimed_departure"))
                       .when(has_arr, F.col("aimed_arrival"))
                       .otherwise(F.coalesce("aimed_departure", "aimed_arrival")),
    })
    secs = lambda a, b: F.unix_micros(F.col(a)) / 1e6 - F.unix_micros(F.col(b)) / 1e6
    v4 = v4.withColumns({
        "delay_s": secs("visit_time", "aimed_time"),
        "delay_arrival_s": secs("actual_arrival", "aimed_arrival"),
        "delay_departure_s": secs("actual_departure", "aimed_departure"),
    })
    # local fields: from the UTC event time, converted once per row (common.local_fields).
    # Unobserved visits have no visit_time, so they fall back to the aimed time.
    event_time = F.coalesce("visit_time", "aimed_time")
    d = F.col("delay_s")
    v4 = v4.withColumns({
        "delay_outlier": F.coalesce((d > DELAY_MAX_S) | (d < DELAY_MIN_S), F.lit(False)),
        "on_time": F.coalesce(d.between(ON_TIME_EARLY_S, ON_TIME_LATE_S), F.lit(False)),
        "delay_class": F.when(d.isNull(), None).when(d < EARLY_S, "early").when(d > LATE_S, "late").otherwise("on_time"),
        "is_monitored": common.to_bool("monitored"),
        "no_prediction": ~F.col("is_actual") & F.col("expected_arrival").isNull() & F.col("expected_departure").isNull(),
        **common.local_fields(event_time),
        "hour_utc": F.date_trunc("hour", event_time),
        # occurrence of a stop inside a trip, to match GTFS stop calls (loops)
        "occ": F.row_number().over(Window.partitionBy("data_frame_ref", "journey_ref", "stop_ref")
                                   .orderBy("stop_order")) - 1,
    })
    v.unpersist()
    return v4


# ------------------------------------------------------------------ headways and segments
def secs_between(later, earlier):
    return F.unix_micros(later) / 1e6 - F.unix_micros(earlier) / 1e6


def headways(clean):
    """Per stop and line direction: time since the previous bus (by actual time) vs the timetable's
    gap between the same two trips. Outlier delays are left out, they would distort the order."""
    w = Window.partitionBy("line_ref", "direction_ref", "stop_ref").orderBy("visit_time", "journey_ref")
    h = clean.where(~F.col("delay_outlier"))
    h = h.withColumns({"previous_visit_time": F.lag("visit_time").over(w),
                       "previous_aimed_time": F.lag("aimed_time").over(w),
                       "previous_trip_id": F.lag("journey_ref").over(w)})
    h = h.where(F.col("previous_visit_time").isNotNull() & (F.col("previous_trip_id") != F.col("journey_ref")))
    sched = secs_between(F.col("aimed_time"), F.col("previous_aimed_time"))
    h = h.withColumns({"headway_s": secs_between(F.col("visit_time"), F.col("previous_visit_time")),
                       "sched_headway_s": F.when(sched > 0, sched)})       # <= 0: buses swapped order
    ratio = F.col("headway_s") / F.col("sched_headway_s")
    return h.withColumns({"headway_dev_s": F.col("headway_s") - F.col("sched_headway_s"),
                          "headway_ratio": F.round(ratio, 3),
                          "is_bunched": ratio < BUNCH_RATIO,
                          "is_gapped": ratio > GAP_RATIO})


def haversine_m(lat1, lon1, lat2, lon2):
    r = F.radians
    a = (F.sin((r(lat2) - r(lat1)) / 2) ** 2
         + F.cos(r(lat1)) * F.cos(r(lat2)) * F.sin((r(lon2) - r(lon1)) / 2) ** 2)
    return 2 * EARTH_M * F.asin(F.sqrt(a))


def segments(clean):
    """Consecutive observed stops of a trip (stop_order n -> n+1): departure at the first, arrival
    at the next, vs the timetable. Where traffic slows buses down, this is where it shows."""
    w = Window.partitionBy("journey_ref").orderBy("stop_order")
    nxt = {f"to_{c}": F.lead(c).over(w) for c in
           ["stop_order", "stop_ref", "actual_arrival", "actual_departure", "aimed_arrival",
            "aimed_departure", "stop_lat", "stop_lon", "delay_outlier"]}
    s = clean.withColumns(nxt).where(F.col("to_stop_order") == F.col("stop_order") + 1)
    dep = F.coalesce("actual_departure", "actual_arrival")
    arr = F.coalesce("to_actual_arrival", "to_actual_departure")
    elapsed = secs_between(arr, dep)
    sched = secs_between(F.coalesce("to_aimed_arrival", "to_aimed_departure"),
                         F.coalesce("aimed_departure", "aimed_arrival"))
    dist = haversine_m(F.col("stop_lat"), F.col("stop_lon"), F.col("to_stop_lat"), F.col("to_stop_lon"))
    valid = (elapsed > 0) & (elapsed <= SEGMENT_MAX_S) & ~F.col("delay_outlier") & ~F.col("to_delay_outlier")
    return s.select(
        F.col("journey_ref").alias("trip_id"), F.col("line_ref").alias("route_id"), "route_short_name",
        "direction_ref", F.col("stop_order").alias("from_stop_order"),
        F.col("stop_ref").alias("from_stop_id"), F.col("to_stop_ref").alias("to_stop_id"),
        dep.alias("from_visit_time"), arr.alias("to_visit_time"),
        elapsed.alias("observed_elapsed_s"), sched.alias("scheduled_running_time_s"),
        (elapsed - sched).alias("running_time_deviation_s"),
        F.round(dist, 1).alias("segment_distance_m"),
        F.when(valid, F.round(dist / elapsed * 3.6, 2)).alias("average_speed_kmh"),
        "local_hour", "period", "weekday", F.coalesce(valid, F.lit(False)).alias("is_valid"))


# ------------------------------------------------------------------ job
def run(spark, day):
    print(f"=== clean {day}")
    removal_log = []

    def log(rule, before, after):
        removal_log.append((rule, int(before), int(after), int(before) - int(after)))
        print(f"  {rule:<52} {before:>12,} -> {after:>12,}   removed {before - after:>12,}")

    routes, stops, trips, st, calendar, calendar_dates = read_gtfs(spark, day)
    raw, et = read_feed(spark, day)
    visits = clean_visits(spark, day, raw, et, routes, stops, log).persist()

    # timetable vs feed
    expected = expected_calls(spark, day, routes, trips, st, calendar, calendar_dates).persist()
    feed_trips = visits.select(F.col("data_frame_ref").alias("service_date"),
                               F.col("journey_ref").alias("trip_id")).distinct() \
                       .withColumn("trip_in_feed", F.lit(True))
    et_cols = visits.select(F.col("data_frame_ref").alias("service_date"), F.col("journey_ref").alias("trip_id"),
                            F.col("stop_ref").alias("stop_id"), "occ", "visit_status", "visit_time",
                            "aimed_time", "delay_s", "delay_outlier", "n_snapshots")
    cmp = (expected.join(et_cols, ["service_date", "trip_id", "stop_id", "occ"], "left")
           .join(feed_trips, ["service_date", "trip_id"], "left"))
    cmp = cmp.withColumn("final_status", F.when(F.col("trip_in_feed").isNull(), "trip_not_in_feed")
                         .otherwise(F.coalesce("visit_status", F.lit("stop_not_in_feed")))).drop("trip_in_feed")

    expected_per_stop = (expected.groupBy("service_date", "stop_id").agg(F.count("*").alias("expected_visits"))
                         .join(stops.select("stop_id", "stop_name", "stop_lat", "stop_lon"), "stop_id", "left"))

    clean_cols = ["data_frame_ref", "journey_ref", "line_ref", "route_short_name", "route_type_name",
                  "direction_ref", "stop_order", "stop_ref", "stop_name", "stop_lat", "stop_lon",
                  "aimed_arrival", "actual_arrival", "aimed_departure", "actual_departure",
                  "visit_time", "aimed_time", "delay_event", "delay_s", "delay_arrival_s", "delay_departure_s",
                  "delay_outlier", "on_time", "delay_class", "is_actual", "is_monitored", "n_snapshots",
                  "local_date", "local_hour", "weekday", "period", "hour_utc"]
    clean = visits.where(F.col("visit_status") == "observed").select(*clean_cols) \
                  .withColumnRenamed("data_frame_ref", "service_date")
    # change of delay since the previous stop of the trip: > 0 = time lost on the way here
    w_trip = Window.partitionBy("journey_ref").orderBy("stop_order")
    clean = clean.withColumn("delay_change_s", F.when(
        (F.lag("stop_order").over(w_trip) == F.col("stop_order") - 1) & ~F.col("delay_outlier")
        & ~F.lag("delay_outlier").over(w_trip), F.col("delay_s") - F.lag("delay_s").over(w_trip)))
    clean = clean.persist()

    hw = headways(clean).persist()
    hw_cols = ["headway_s", "sched_headway_s", "headway_dev_s", "headway_ratio", "is_bunched", "is_gapped"]
    clean_out = clean.join(hw.select("journey_ref", "stop_order", *hw_cols), ["journey_ref", "stop_order"], "left")
    stop_headways = hw.select(
        "line_ref", "direction_ref", F.col("stop_ref").alias("stop_id"), "stop_name",
        F.col("journey_ref").alias("trip_id"), "visit_time", "previous_trip_id", "previous_visit_time",
        "headway_s", F.col("sched_headway_s").alias("scheduled_headway_s"),
        F.col("headway_dev_s").alias("headway_deviation_s"), "headway_ratio", "is_bunched",
        F.col("is_gapped").alias("is_large_gap"), "local_hour", "period")
    trip_segments = segments(clean)

    # service_date is the partition folder, so it is dropped from the files themselves
    out = {}
    out["clean_stop_visits"] = common.write(clean_out.drop("service_date"), common.SILVER, "clean_stop_visits",
                                            "service_date", day, files=2)
    out["stop_headways"] = common.write(stop_headways, common.SILVER, "stop_headways", "service_date", day, files=1)
    out["trip_segments"] = common.write(trip_segments, common.SILVER, "trip_segments", "service_date", day, files=1)
    out["stop_event_status"] = common.write(cmp.drop("service_date"), common.SILVER, "stop_event_status",
                                            "service_date", day, files=2)
    out["expected_per_stop"] = common.write(expected_per_stop.drop("service_date"), common.SILVER,
                                            "expected_per_stop", "service_date", day, files=1)
    common.write(spark.createDataFrame(removal_log, "rule string, rows_before long, rows_after long, removed long"),
                 common.SILVER, "removal_log", "service_date", day, files=1)

    status = {r["final_status"]: r["count"] for r in
              spark.read.parquet(out["stop_event_status"]).groupBy("final_status").count().collect()}
    n_clean = spark.read.parquet(out["clean_stop_visits"]).count()
    print(f"  clean_stop_visits: {n_clean:,} observed bus visits")
    print(f"  stop_event_status: {sum(status.values()):,} scheduled bus calls {status}")
    h = spark.read.parquet(out["stop_headways"]).agg(
        F.count("*").alias("n"), F.avg(F.col("is_bunched").cast("int")).alias("b"),
        F.avg(F.col("is_large_gap").cast("int")).alias("g")).first()
    print(f"  stop_headways: {h['n']:,} headways, bunched {h['b'] or 0:.1%}, large gaps {h['g'] or 0:.1%}")
    sg = spark.read.parquet(out["trip_segments"]).agg(
        F.count("*").alias("n"), F.sum(F.col("is_valid").cast("int")).alias("ok"),
        F.percentile_approx("average_speed_kmh", 0.5).alias("v")).first()
    print(f"  trip_segments: {sg['n']:,} segments ({sg['ok'] or 0:,} valid), median speed {sg['v']} km/h")
    hw.unpersist()
    clean.unpersist()
    visits.unpersist()
    expected.unpersist()
    return n_clean


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", required=True, help="service date (the feed's data_frame_ref), e.g. 2026-10-30")
    p.add_argument("--force", action="store_true", help="run even if bronze of D or D+1 is not complete yet")
    a = p.parse_args()
    date.fromisoformat(a.date)
    spark = common.spark_session("buspulse-silver")
    if not a.force:
        missing = [d for d in (a.date, common.shift(a.date, 1)) if not common.bronze_ready(spark, d)]
        if missing:
            # reading now would give a silver day without its last snapshots: stop instead
            print(f"bronze not complete for {missing}: not building silver {a.date} yet (use --force to override)")
            spark.stop()
            raise SystemExit(3)
    run(spark, a.date)
    common.mark_done(spark, "clean", a.date)


if __name__ == "__main__":
    main()
