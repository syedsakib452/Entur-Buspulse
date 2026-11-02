import csv
import math
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

# ============================================================
# CONFIGURATION
# ============================================================

TABLES_PATH = Path(__file__).resolve().parents[1] / "tables" / "parquet_2026-10-30"
VEHICLE_PATH = str(TABLES_PATH / "vehicle_positions.parquet")
STOPS_PATH = str(TABLES_PATH / "gtfs_stops.parquet")
SIRI_PATH = str(TABLES_PATH / "stop_visits.parquet")

OUTPUT_PATH = str(Path(__file__).resolve().parent / "gps_stop_detection_v2")

# Vehicle position columns
VEHICLE_ID = "vehicle_id"
VEHICLE_TIME = "last_updated"
VEHICLE_LAT = "latitude"
VEHICLE_LON = "longitude"
VEHICLE_LINE = "line_ref"

# GTFS stop columns
STOP_ID = "stop_id"
STOP_NAME = "stop_name"
STOP_LAT = "stop_lat"
STOP_LON = "stop_lon"

# SIRI columns
SIRI_STOP = "stop_ref"
SIRI_ARRIVAL = "actual_arrival"
SIRI_LINE = "line_ref"

# Stop filtering: keep only boarding positions (Quays), drop parent StopPlaces
# that would otherwise duplicate every quay event. Set to None to keep all.
STOP_ID_PREFIX = "NSR:Quay:"

# Detection parameters
STOP_RADIUS_METERS = 60
MAX_CONSECUTIVE_GAP_SECONDS = 180     # a longer silence ends the event
MIN_DWELL_SECONDS = 10                # minimum observed time near stop
SLOW_SPEED_MPS = 1.5                  # "stationary" speed (~5.4 km/h)
MAX_SPEED_GAP_SECONDS = 120           # speed is unreliable across longer gaps

# SIRI matching
SIRI_MATCH_TOLERANCE_SECONDS = 600
MATCH_ON_LINE = True                  # require the same (normalised) line
GPS_TIME_FOR_MATCH = "first_observed_near"  # or "closest_observed_time"

spark = (
    SparkSession.builder
    .appName("GPS Stop Visit Detection v2")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)


# ============================================================
# HELPERS
# ============================================================

def haversine_m(lat1, lon1, lat2, lon2):
    """Haversine distance in metres between two columns of degrees."""
    p1, p2 = F.radians(lat1), F.radians(lat2)
    dlat = p2 - p1
    dlon = F.radians(lon2) - F.radians(lon1)
    a = (
        F.pow(F.sin(dlat / 2), 2)
        + F.cos(p1) * F.cos(p2) * F.pow(F.sin(dlon / 2), 2)
    )
    return 2 * 6371000 * F.asin(F.sqrt(F.least(F.lit(1.0), a)))


def last_segment(col):
    """'SKY:Line:1234' -> '1234', so line IDs from different feeds compare."""
    return F.regexp_extract(col, r"([^:]+)$", 1)


def save_csv(dataframe, output_file):
    """Stream a Spark DataFrame to a local CSV without Hadoop's file writer."""
    path = Path(output_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(dataframe.columns)
        for row in dataframe.toLocalIterator():
            writer.writerow(["" if v is None else str(v) for v in row])
    print(f"Saved CSV: {path}")


# ============================================================
# 1. LOAD DATA
# ============================================================

vehicles = spark.read.parquet(VEHICLE_PATH)
stops = spark.read.parquet(STOPS_PATH)
siri = spark.read.parquet(SIRI_PATH)

print("Vehicle observations:", vehicles.count())
print("GTFS stops (raw):", stops.count())
print("SIRI stop visits:", siri.count())


# ============================================================
# 2. STANDARDISE VEHICLE AND STOP DATA
# ============================================================

positions = (
    vehicles
    .select(
        F.col(VEHICLE_ID).alias("vehicle_id"),
        F.to_timestamp(VEHICLE_TIME).alias("observation_time"),
        F.col(VEHICLE_LAT).cast("double").alias("vehicle_lat"),
        F.col(VEHICLE_LON).cast("double").alias("vehicle_lon"),
        F.col(VEHICLE_LINE).alias("line_ref"),
    )
    .filter(F.col("vehicle_id").isNotNull())
    .filter(F.col("observation_time").isNotNull())
    .filter(F.col("vehicle_lat").between(-90, 90))
    .filter(F.col("vehicle_lon").between(-180, 180))
    .filter(~((F.col("vehicle_lat") == 0) & (F.col("vehicle_lon") == 0)))
    .dropDuplicates(["vehicle_id", "observation_time"])
)

gtfs_stops = (
    stops
    .select(
        F.col(STOP_ID).alias("stop_id"),
        F.col(STOP_NAME).alias("stop_name"),
        F.col(STOP_LAT).cast("double").alias("stop_lat"),
        F.col(STOP_LON).cast("double").alias("stop_lon"),
    )
    .filter(F.col("stop_id").isNotNull())
    .filter(F.col("stop_lat").isNotNull())
    .filter(F.col("stop_lon").isNotNull())
    .dropDuplicates(["stop_id"])
)

if STOP_ID_PREFIX:
    gtfs_stops = gtfs_stops.filter(F.col("stop_id").startswith(STOP_ID_PREFIX))

print("Clean GTFS stops used:", gtfs_stops.count())


# ============================================================
# 3. FULL TRAJECTORY: GAPS, STEP MOVEMENT AND SPEED PER PING
# ============================================================
# Computed on ALL pings of a vehicle (not just those near stops), so a bus
# that leaves the radius and returns is correctly seen as two visits.

vehicle_window = Window.partitionBy("vehicle_id").orderBy("observation_time")

trajectory = (
    positions
    .withColumn("previous_time", F.lag("observation_time").over(vehicle_window))
    .withColumn("previous_lat", F.lag("vehicle_lat").over(vehicle_window))
    .withColumn("previous_lon", F.lag("vehicle_lon").over(vehicle_window))
    .withColumn(
        "gap_seconds",
        F.col("observation_time").cast("long") - F.col("previous_time").cast("long"),
    )
    .withColumn(
        "step_m",
        F.when(
            F.col("previous_time").isNotNull(),
            haversine_m(
                F.col("previous_lat"), F.col("previous_lon"),
                F.col("vehicle_lat"), F.col("vehicle_lon"),
            ),
        ),
    )
    .withColumn(
        "speed_mps",
        F.when(
            (F.col("gap_seconds") > 0)
            & (F.col("gap_seconds") <= MAX_SPEED_GAP_SECONDS),
            F.col("step_m") / F.col("gap_seconds"),
        ),
    )
    .drop("previous_lat", "previous_lon")
    .cache()
)

print("Clean vehicle observations:", trajectory.count())

# Diagnostic: how sparse are the pings?
gap_stats = trajectory.select(
    F.percentile_approx("gap_seconds", [0.25, 0.5, 0.75, 0.9, 0.99]).alias("p")
).first()["p"]
print(
    "Ping interval seconds (p25, p50, p75, p90, p99):", gap_stats,
    "\n  -> at 30 km/h a bus moves ~8.3 m/s, so p50 x 8.3 is the distance "
    "between pings; compare it to the 2 x radius window."
)


# ============================================================
# 4. ASSIGN EACH PING TO ITS NEAREST STOP (GRID JOIN)
# ============================================================
# Stops are copied into their 3x3 neighbouring grid cells, so a ping only needs
# an equi-join on its own cell. Cells are >= the radius in both dimensions.

mean_lat = gtfs_stops.agg(F.avg("stop_lat")).first()[0]
LAT_CELL = STOP_RADIUS_METERS * 1.1 / 111320.0
LON_CELL = LAT_CELL / math.cos(math.radians(mean_lat))

stops_grid = (
    gtfs_stops
    .withColumn("d_lat", F.explode(F.array(F.lit(-1), F.lit(0), F.lit(1))))
    .withColumn("d_lon", F.explode(F.array(F.lit(-1), F.lit(0), F.lit(1))))
    .withColumn("cell_lat", F.floor(F.col("stop_lat") / LAT_CELL) + F.col("d_lat"))
    .withColumn("cell_lon", F.floor(F.col("stop_lon") / LON_CELL) + F.col("d_lon"))
    .drop("d_lat", "d_lon")
)

pings_with_cell = (
    trajectory
    .select(
        "vehicle_id", "observation_time", "vehicle_lat", "vehicle_lon",
        F.floor(F.col("vehicle_lat") / LAT_CELL).alias("cell_lat"),
        F.floor(F.col("vehicle_lon") / LON_CELL).alias("cell_lon"),
    )
)

nearest_window = (
    Window.partitionBy("vehicle_id", "observation_time")
    .orderBy(F.col("distance_m").asc(), F.col("stop_id").asc())
)

nearest_stop = (
    pings_with_cell
    .join(F.broadcast(stops_grid), ["cell_lat", "cell_lon"])
    .withColumn(
        "distance_m",
        haversine_m(
            F.col("vehicle_lat"), F.col("vehicle_lon"),
            F.col("stop_lat"), F.col("stop_lon"),
        ),
    )
    .filter(F.col("distance_m") <= STOP_RADIUS_METERS)
    .withColumn("rank", F.row_number().over(nearest_window))
    .filter(F.col("rank") == 1)
    .select("vehicle_id", "observation_time", "stop_id", "stop_name", "distance_m")
)

# Every ping, with its nearest stop (NULL when outside all radii).
assigned = (
    trajectory
    .join(nearest_stop, ["vehicle_id", "observation_time"], "left")
    .cache()
)

print("Pings assigned to a stop:", assigned.filter(F.col("stop_id").isNotNull()).count())


# ============================================================
# 5. BUILD EVENTS: CONSECUTIVE PINGS ON THE SAME STOP
# ============================================================
# A new event starts when the nearest stop changes (including leaving the
# radius entirely) or when there is a long silence in the vehicle's own data.

assigned = (
    assigned
    .withColumn("previous_stop", F.lag("stop_id").over(vehicle_window))
    .withColumn(
        "new_event",
        (
            (~F.col("stop_id").eqNullSafe(F.col("previous_stop")))
            | (F.col("gap_seconds") > MAX_CONSECUTIVE_GAP_SECONDS)
            | F.col("previous_time").isNull()
        ).cast("int"),
    )
    .withColumn(
        "event_number",
        F.sum("new_event").over(
            vehicle_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ),
    )
)

events = (
    assigned
    .filter(F.col("stop_id").isNotNull())
    .groupBy("vehicle_id", "stop_id", "stop_name", "event_number")
    .agg(
        F.first("line_ref", ignorenulls=True).alias("line_ref"),
        F.min("observation_time").alias("first_observed_near"),
        F.max("observation_time").alias("last_observed_near"),
        F.min("distance_m").alias("min_distance_m"),
        F.min(F.struct("distance_m", "observation_time"))
         .getField("observation_time").alias("closest_observed_time"),
        F.count("*").alias("observation_count"),
        F.min("speed_mps").alias("min_speed_mps"),
        F.max("step_m").alias("max_step_m"),
    )
    .withColumn(
        "observed_near_span_seconds",
        F.col("last_observed_near").cast("long")
        - F.col("first_observed_near").cast("long"),
    )
    .withColumn("line_norm", last_segment(F.col("line_ref")))
)

slow = F.col("min_speed_mps") <= SLOW_SPEED_MPS

events = events.withColumn(
    "inference_status",
    F.when(
        (F.col("observation_count") >= 2)
        & (F.col("observed_near_span_seconds") >= MIN_DWELL_SECONDS)
        & slow,
        "probable_stop",
    )
    .when(F.col("observation_count") >= 2, "possible_stop_or_slow_pass")
    .when(slow, "single_slow_observation")
    .otherwise("single_nearby_observation"),
).withColumn(
    "event_id",
    F.sha2(
        F.concat_ws(
            "|", "vehicle_id", "stop_id",
            F.col("first_observed_near").cast("string"),
        ),
        256,
    ),
).cache()

print("Inferred events:", events.count())
events.groupBy("inference_status").count().orderBy("inference_status").show(truncate=False)


# ============================================================
# 6. LOAD SIRI ACTUAL ARRIVALS (+ FORMAT DIAGNOSTICS)
# ============================================================

siri_actual_all = (
    siri
    .filter(F.col(SIRI_ARRIVAL).isNotNull())
    .select(
        F.col(SIRI_STOP).alias("siri_stop_id"),
        F.to_timestamp(SIRI_ARRIVAL).alias("siri_actual_arrival"),
        F.col(SIRI_LINE).alias("siri_line_ref"),
    )
    .filter(F.col("siri_actual_arrival").isNotNull())
)

# Only compare SIRI arrivals inside the GPS observation window, otherwise
# recall is understated for reasons unrelated to the method.
bounds = trajectory.agg(
    F.min("observation_time").alias("t0"), F.max("observation_time").alias("t1")
).first()

siri_actual = (
    siri_actual_all
    .filter(
        (F.col("siri_actual_arrival") >= F.lit(bounds["t0"]))
        & (F.col("siri_actual_arrival") <= F.lit(bounds["t1"]))
    )
    .withColumn("siri_line_norm", last_segment(F.col("siri_line_ref")))
    .withColumn("siri_id", F.monotonically_increasing_id())
    .cache()
)

print("SIRI arrivals inside GPS window:", siri_actual.count())

# ID-format diagnostics: if these overlaps are near zero, matching cannot work.
gps_stop_ids = events.select("stop_id").distinct()
siri_stop_ids = siri_actual.select("siri_stop_id").distinct()
print(
    "Distinct SIRI stop refs:", siri_stop_ids.count(),
    "| also present among detected GPS stops:",
    siri_stop_ids.join(gps_stop_ids, siri_stop_ids.siri_stop_id == gps_stop_ids.stop_id).count(),
)
print("SIRI stop_ref type prefixes:")
siri_actual.groupBy(F.split("siri_stop_id", ":").getItem(1).alias("type")).count().show()
print("Sample line refs (vehicle feed vs SIRI):")
events.select("line_ref", "line_norm").distinct().show(5, truncate=False)
siri_actual.select("siri_line_ref", "siri_line_norm").distinct().show(5, truncate=False)


# ============================================================
# 7. ONE-TO-ONE MATCHING (MUTUAL NEAREST IN TIME)
# ============================================================

gps_time = F.col(GPS_TIME_FOR_MATCH)
signed_diff = gps_time.cast("long") - F.col("siri_actual_arrival").cast("long")

join_condition = (
    (events.stop_id == siri_actual.siri_stop_id)
    & (F.abs(signed_diff) <= SIRI_MATCH_TOLERANCE_SECONDS)
)
if MATCH_ON_LINE:
    join_condition = join_condition & (events.line_norm == siri_actual.siri_line_norm)

candidates = (
    events.join(siri_actual, join_condition, "inner")
    .withColumn("signed_diff_seconds", signed_diff)
    .withColumn("abs_diff_seconds", F.abs(signed_diff))
)

# A pair is kept only if each side is the other's closest candidate.
rank_by_event = Window.partitionBy("event_id").orderBy("abs_diff_seconds", "siri_id")
rank_by_siri = Window.partitionBy("siri_id").orderBy("abs_diff_seconds", "event_id")

matches = (
    candidates
    .withColumn("rank_event", F.row_number().over(rank_by_event))
    .withColumn("rank_siri", F.row_number().over(rank_by_siri))
    .filter((F.col("rank_event") == 1) & (F.col("rank_siri") == 1))
    .select("event_id", "siri_id", "siri_actual_arrival",
            "signed_diff_seconds", "abs_diff_seconds")
    .cache()
)

comparison = (
    events
    .join(matches, "event_id", "left")
    .withColumn(
        "comparison_status",
        F.when(F.col("siri_id").isNotNull(), "matched").otherwise("no_siri_match"),
    )
    .select(
        "event_id", "vehicle_id", "stop_id", "stop_name", "line_ref",
        "first_observed_near", "closest_observed_time", "last_observed_near",
        "min_distance_m", "observation_count", "observed_near_span_seconds",
        "min_speed_mps", "inference_status",
        "siri_actual_arrival", "signed_diff_seconds", "abs_diff_seconds",
        "comparison_status",
    )
    .cache()
)

siri_unmatched = (
    siri_actual
    .join(matches.select("siri_id"), "siri_id", "left_anti")
    .select("siri_id", "siri_stop_id", "siri_line_ref", "siri_actual_arrival")
)


# ============================================================
# 8. SAVE CSV OUTPUTS
# ============================================================

save_csv(
    nearest_stop.orderBy("vehicle_id", "observation_time"),
    f"{OUTPUT_PATH}/vehicle_stop_proximity.csv",
)
save_csv(events, f"{OUTPUT_PATH}/inferred_stop_visits.csv")
save_csv(comparison, f"{OUTPUT_PATH}/gps_siri_comparison.csv")
save_csv(siri_unmatched, f"{OUTPUT_PATH}/siri_without_gps_event.csv")


# ============================================================
# 9. SUMMARY
# ============================================================

print("\nGPS events: match rate by inference status (precision-like)")
(
    comparison
    .groupBy("inference_status")
    .agg(
        F.count("*").alias("events"),
        F.sum((F.col("comparison_status") == "matched").cast("int")).alias("matched"),
    )
    .withColumn("match_rate", F.round(F.col("matched") / F.col("events"), 3))
    .orderBy("inference_status")
    .show(truncate=False)
)

print("\nSigned time difference (GPS - SIRI) in seconds, matched events")
print("Positive = GPS later than SIRI, negative = GPS earlier.")
(
    comparison
    .filter(F.col("comparison_status") == "matched")
    .groupBy("inference_status")
    .agg(
        F.percentile_approx("signed_diff_seconds", [0.1, 0.25, 0.5, 0.75, 0.9])
         .alias("p10_p25_p50_p75_p90"),
        F.round(F.avg("signed_diff_seconds"), 1).alias("mean"),
    )
    .show(truncate=False)
)

n_siri = siri_actual.count()
n_siri_matched = matches.count()
print(
    f"\nSIRI arrivals with a GPS event (recall-like): "
    f"{n_siri_matched} / {n_siri} = {n_siri_matched / max(n_siri, 1):.1%}"
)

print("\nSIRI arrivals with no GPS event: top stops")
siri_unmatched.groupBy("siri_stop_id").count().orderBy(F.desc("count")).show(15, truncate=False)

print("\nMost frequently inferred stops (probable_stop only)")
(
    events
    .filter(F.col("inference_status") == "probable_stop")
    .groupBy("stop_id", "stop_name")
    .agg(
        F.count("*").alias("inferred_visits"),
        F.countDistinct("vehicle_id").alias("unique_vehicles"),
    )
    .orderBy(F.desc("inferred_visits"))
    .show(30, truncate=False)
)

print("\nCompleted GPS stop detection.")

spark.stop()