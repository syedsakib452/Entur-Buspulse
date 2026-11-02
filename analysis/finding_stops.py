
import csv
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

OUTPUT_PATH = str(Path(__file__).resolve().parent / "gps_stop_detection")

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

# Detection parameters
STOP_RADIUS_METERS = 60
MAX_CONSECUTIVE_GAP_SECONDS = 360
MAX_STATIONARY_MOVEMENT_METERS = 30
SIRI_MATCH_TOLERANCE_SECONDS = 600

spark = (
    SparkSession.builder
    .appName("GPS Stop Visit Detection")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)


# ============================================================
# 1. LOAD DATA
# ============================================================

vehicles = spark.read.parquet(VEHICLE_PATH)
stops = spark.read.parquet(STOPS_PATH)
siri = spark.read.parquet(SIRI_PATH)

print("Vehicle observations:", vehicles.count())
print("GTFS stops:", stops.count())
print("SIRI stop visits:", siri.count())


# ============================================================
# 2. STANDARDISE VEHICLE DATA
# ============================================================

positions = (
    vehicles
    .select(
        F.col(VEHICLE_ID).alias("vehicle_id"),
        F.to_timestamp(VEHICLE_TIME).alias("observation_time"),
        F.col(VEHICLE_LAT).cast("double").alias("vehicle_lat"),
        F.col(VEHICLE_LON).cast("double").alias("vehicle_lon"),
        F.col(VEHICLE_LINE).alias("line_ref")
    )
    .filter(F.col("vehicle_id").isNotNull())
    .filter(F.col("observation_time").isNotNull())
    .filter(F.col("vehicle_lat").between(-90, 90))
    .filter(F.col("vehicle_lon").between(-180, 180))
    .dropDuplicates([
        "vehicle_id",
        "observation_time"
    ])
)

gtfs_stops = (
    stops
    .select(
        F.col(STOP_ID).alias("stop_id"),
        F.col(STOP_NAME).alias("stop_name"),
        F.col(STOP_LAT).cast("double").alias("stop_lat"),
        F.col(STOP_LON).cast("double").alias("stop_lon")
    )
    .filter(F.col("stop_id").isNotNull())
    .filter(F.col("stop_lat").isNotNull())
    .filter(F.col("stop_lon").isNotNull())
    .dropDuplicates(["stop_id"])
)

print("Clean vehicle observations:", positions.count())
print("Clean GTFS stops:", gtfs_stops.count())


# ============================================================
# 3. FIND NEARBY STOPS
# ============================================================

# Bounding-box filtering reduces unnecessary distance calculations.
# Approximately 0.001 degrees latitude is 111 metres.

vehicles_with_stops = (
    positions.alias("v")
    .crossJoin(F.broadcast(gtfs_stops).alias("s"))
    .filter(
        (F.abs(F.col("v.vehicle_lat") - F.col("s.stop_lat")) < 0.001)
        &
        (F.abs(F.col("v.vehicle_lon") - F.col("s.stop_lon")) < 0.002)
    )
)

# Haversine distance in metres

lat1 = F.radians(F.col("vehicle_lat"))
lon1 = F.radians(F.col("vehicle_lon"))
lat2 = F.radians(F.col("stop_lat"))
lon2 = F.radians(F.col("stop_lon"))

a = (
    F.pow(F.sin((lat2 - lat1) / 2), 2)
    +
    F.cos(lat1) * F.cos(lat2)
    * F.pow(F.sin((lon2 - lon1) / 2), 2)
)

distance = (
    2 * 6371000 *
    F.asin(F.sqrt(F.least(F.lit(1.0), a)))
)

proximity = (
    vehicles_with_stops
    .withColumn("distance_m", distance)
    .filter(F.col("distance_m") <= STOP_RADIUS_METERS)
    .select(
        "vehicle_id",
        "observation_time",
        "line_ref",
        "vehicle_lat",
        "vehicle_lon",
        "stop_id",
        "stop_name",
        "distance_m"
    )
)

print("Vehicle observations near stops:", proximity.count())


# ============================================================
# 4. TRACK CONSECUTIVE OBSERVATIONS
# ============================================================

vehicle_stop_window = (
    Window
    .partitionBy("vehicle_id", "stop_id")
    .orderBy("observation_time")
)

tracked = (
    proximity
    .withColumn(
        "previous_time",
        F.lag("observation_time").over(vehicle_stop_window)
    )
    .withColumn(
        "previous_lat",
        F.lag("vehicle_lat").over(vehicle_stop_window)
    )
    .withColumn(
        "previous_lon",
        F.lag("vehicle_lon").over(vehicle_stop_window)
    )
)

tracked = tracked.withColumn(
    "gap_seconds",
    F.col("observation_time").cast("long")
    - F.col("previous_time").cast("long")
)

# Calculate movement between consecutive GPS observations

movement_a = (
    F.pow(
        F.sin(
            (
                F.radians(F.col("vehicle_lat"))
                - F.radians(F.col("previous_lat"))
            ) / 2
        ),
        2
    )
    +
    F.cos(F.radians(F.col("previous_lat")))
    * F.cos(F.radians(F.col("vehicle_lat")))
    *
    F.pow(
        F.sin(
            (
                F.radians(F.col("vehicle_lon"))
                - F.radians(F.col("previous_lon"))
            ) / 2
        ),
        2
    )
)

movement = (
    2 * 6371000
    * F.asin(F.sqrt(F.least(F.lit(1.0), movement_a)))
)

tracked = tracked.withColumn(
    "movement_since_previous_m",
    F.when(
        F.col("previous_time").isNotNull(),
        movement
    )
)

# A new event begins if there is no previous observation
# near this stop, or the gap is too large.

tracked = tracked.withColumn(
    "new_event",
    F.when(
        F.col("previous_time").isNull()
        | (F.col("gap_seconds") > MAX_CONSECUTIVE_GAP_SECONDS),
        1
    ).otherwise(0)
)

event_window = (
    Window
    .partitionBy("vehicle_id", "stop_id")
    .orderBy("observation_time")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

tracked = tracked.withColumn(
    "event_number",
    F.sum("new_event").over(event_window)
)


# ============================================================
# 5. GENERATE INFERRED STOP VISITS
# ============================================================

events = (
    tracked
    .groupBy(
        "vehicle_id",
        "stop_id",
        "stop_name",
        "line_ref",
        "event_number"
    )
    .agg(
        F.min("observation_time").alias("first_observed_near"),
        F.max("observation_time").alias("last_observed_near"),
        F.min("distance_m").alias("min_distance_m"),
        F.count("*").alias("observation_count"),
        F.max("movement_since_previous_m")
         .alias("max_step_movement_m")
    )
    .withColumn(
        "observed_near_span_seconds",
        F.col("last_observed_near").cast("long")
        - F.col("first_observed_near").cast("long")
    )
)

events = events.withColumn(
    "inference_status",
    F.when(
        (F.col("observation_count") >= 2)
        &
        (F.col("max_step_movement_m")
         <= MAX_STATIONARY_MOVEMENT_METERS),
        "probable_stop"
    )
    .when(
        F.col("observation_count") >= 2,
        "possible_stop_or_slow_pass"
    )
    .otherwise("single_nearby_observation")
)

events = events.withColumn(
    "event_id",
    F.sha2(
        F.concat_ws(
            "|",
            "vehicle_id",
            "stop_id",
            F.col("first_observed_near").cast("string")
        ),
        256
    )
)

events = events.cache()

print("Inferred events:", events.count())

events.groupBy("inference_status").count().show()


# ============================================================
# 6. LOAD AND STANDARDISE SIRI ACTUAL ARRIVALS
# ============================================================

siri_actual = (
    siri
    .filter(F.col(SIRI_ARRIVAL).isNotNull())
    .select(
        F.col(SIRI_STOP).alias("siri_stop_id"),
        F.to_timestamp(SIRI_ARRIVAL).alias("siri_actual_arrival"),
        F.col(SIRI_LINE).alias("siri_line_ref")
    )
    .filter(F.col("siri_actual_arrival").isNotNull())
)


# ============================================================
# 7. COMPARE GPS EVENTS WITH SIRI
# ============================================================

g = events.alias("g")
s = siri_actual.alias("s")

time_difference = F.abs(
    F.col("g.first_observed_near").cast("long")
    - F.col("s.siri_actual_arrival").cast("long")
)

comparison_candidates = (
    g.join(
        s,
        (
            (F.col("g.stop_id") == F.col("s.siri_stop_id"))
            &
            (
                time_difference
                <= SIRI_MATCH_TOLERANCE_SECONDS
            )
        ),
        "left"
    )
    .withColumn("time_difference_seconds", time_difference)
)

# Keep the closest SIRI arrival for each GPS event.

match_window = (
    Window
    .partitionBy(F.col("g.event_id"))
    .orderBy(
        F.col("time_difference_seconds").asc_nulls_last()
    )
)

comparison = (
    comparison_candidates
    .withColumn(
        "match_rank",
        F.row_number().over(match_window)
    )
    .filter(F.col("match_rank") == 1)
    .select(
        F.col("g.event_id"),
        F.col("g.vehicle_id"),
        F.col("g.stop_id"),
        F.col("g.stop_name"),
        F.col("g.line_ref"),
        F.col("g.first_observed_near"),
        F.col("g.last_observed_near"),
        F.col("g.min_distance_m"),
        F.col("g.observation_count"),
        F.col("g.inference_status"),
        F.col("s.siri_actual_arrival"),
        F.col("time_difference_seconds"),
        F.when(
            F.col("s.siri_actual_arrival").isNotNull(),
            "matched"
        ).otherwise("no_siri_match").alias("comparison_status")
    )
)


# ============================================================
# 8. SAVE CSV OUTPUTS
# ============================================================


def save_csv(dataframe, output_file):
    """Stream a Spark DataFrame to a local CSV without Hadoop's file writer."""
    path = Path(output_file)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(dataframe.columns)
        for row in dataframe.toLocalIterator():
            writer.writerow(
                ["" if value is None else str(value) for value in row]
            )

    print(f"Saved CSV: {path}")


save_csv(proximity, f"{OUTPUT_PATH}/vehicle_stop_proximity.csv")
save_csv(events, f"{OUTPUT_PATH}/inferred_stop_visits.csv")
save_csv(comparison, f"{OUTPUT_PATH}/gps_siri_comparison.csv")


# ============================================================
# 9. PRINT SUMMARY
# ============================================================

print("\nGPS inference summary")

events.groupBy(
    "inference_status"
).count().show()

print("\nGPS versus SIRI comparison")

comparison.groupBy(
    "comparison_status"
).count().show()

print("\nMost frequently inferred stops")

(
    events
    .groupBy("stop_id", "stop_name")
    .agg(
        F.count("*").alias("inferred_visits"),
        F.countDistinct("vehicle_id").alias("unique_vehicles")
    )
    .orderBy(F.desc("inferred_visits"))
    .show(30, truncate=False)
)

print("\nCompleted GPS stop detection.")

spark.stop()
