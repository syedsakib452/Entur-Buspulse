# %% [markdown]
# # Gold Layer ETL (DuckDB)
#
# Builds the **Gold layer** for Kolumbus/Entur bus analytics from the cleaned Silver parquet files.
# Writes ZSTD-compressed Parquet files + `gold.duckdb` (including backward-compatible views).
#
# ### Silver inputs
# - Required: `clean_stop_visits`
# - Operational: `stop_headways`, `trip_segments`
# - Auxiliary: `per_stop_evidence`, `trip_vehicle`, `stop_gps_status`, `time_lost_null`,
#              `expected_per_stop`, `stop_event_status`
#
# ### Gold outputs (24 Parquet tables + 3 compatibility views)
# - **Dimensions, Filters & Rules:** `dim_line`, `dim_stop`, `dim_date`, `gold_filters`, `gold_thresholds`
# - **Core Fact & Additive Hourly Cubes:** `fact_stop_visits`, `gold_delay_hour`, `gold_headway_hour`, `gold_speed_hour`
# - **Trip, Stop & Road Analytics:** `gold_trip_metrics`, `gold_busy_roads`, `gold_bunching_stops`, `gold_stop_coverage`
# - **Route Reliability, Completeness & Anomalies:** `gold_route_reliability_daily`, `gold_route_reliability_weekly`,
#   `gold_route_comparison`, `gold_service_completeness`, `gold_anomaly_daily`
# - **Fleet, Evidence & Audit:** `gold_fleet_activity`, `gold_stop_evidence`, `gold_time_lost_reference`,
#   `gold_event_status`, `gold_data_quality`, `gold_meta`
# - **Compatibility Views (in gold.duckdb):** `gold_trip_summary`, `gold_headway`, `gold_segment_hour`

# %%
import os
from pathlib import Path
import datetime as dt
import duckdb

# ---------------- Directory Settings ----------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = Path(os.getenv("SILVER_DIR", PROJECT_ROOT / "tables" / "silver-layer"))
GOLD_DIR = Path(os.getenv("GOLD_DIR", PROJECT_ROOT / "tables" / "gold-layer"))

# ---------------- Analysis Configuration ----------------
LOCAL_TZ = "Europe/Oslo"

# ---------------- Thresholds & Rules ----------------
EARLY_S, LATE_S = -60, 300                        # early: delay < -60 s | on time: -60 <= delay < 300 | late: >= 300 s
BUNCH_RATIO, GAP_RATIO = 0.25, 2.0                # bunched: headway < 0.25 x scheduled | large gap: > 2 x scheduled
HEADWAY_MIN_S, HEADWAY_MAX_S = 120, 3600          # headways judged only where timetable gap is 2..60 min
SEG_MAX_S = 1800                                  # stop-to-stop run > 30 min is implausible
ROAD_MIN_M = 50                                   # busy roads: ignore segments shorter than 50 m
ROAD_MAX_KMH = 90                                 # busy roads: ignore runs faster than 90 km/h
ROAD_MIN_RUNS = 100                               # busy roads: minimum total runs to rank
ROAD_MIN_PEAK_RUNS = 20                           # busy roads: minimum weekday peak runs to rank
FREE_FLOW_Q = 0.85                                # free-flow speed = 85th percentile of segment speeds

# Composite Reliability Score weights (must sum to 1.0)
W_OTP = 0.40                                      # weight for on-time percentage (higher = better)
W_DELAY = 0.25                                    # weight for normalized mean delay (lower = better)
W_BUNCH = 0.20                                    # weight for non-bunching rate (1 - pct_bunched)
W_SPEED = 0.15                                    # weight for normalized average speed (higher = better)

# Daily Anomaly Detection thresholds (rolling 7-day baseline)
ANOMALY_WINDOW_DAYS = 7
ANOMALY_MIN_HISTORY = 3
ANOMALY_Z_MODERATE = 2.0
ANOMALY_Z_HIGH = 2.5
ANOMALY_Z_CRITICAL = 3.5

DELAY_EDGES = [-120, -60, 0, 60, 180, 300, 600]
TL_EDGES = [0, 5, 10, 20, 40]
HOUR_BANDS = {"night": (0, 5), "am_peak": (6, 8), "midday": (9, 14), "pm_peak": (15, 17), "evening": (18, 23)}

THRESHOLDS = [
    ("early_s", EARLY_S, "early: delay_s < early_s"),
    ("late_s", LATE_S, "on time: early_s <= delay_s < late_s; late: delay_s >= late_s"),
    ("bunch_ratio", BUNCH_RATIO, "bunched: actual headway < bunch_ratio x scheduled headway"),
    ("gap_ratio", GAP_RATIO, "large gap: actual headway > gap_ratio x scheduled headway"),
    ("headway_min_s", HEADWAY_MIN_S, "headways judged only where scheduled headway >= this"),
    ("headway_max_s", HEADWAY_MAX_S, "...and <= this"),
    ("segment_max_s", SEG_MAX_S, "stop-to-stop runs longer than this are excluded"),
    ("road_min_m", ROAD_MIN_M, "busy roads: segments shorter than this are excluded"),
    ("road_max_kmh", ROAD_MAX_KMH, "busy roads: runs faster than this are excluded"),
    ("road_min_runs", ROAD_MIN_RUNS, "busy roads: ranked only with at least this many runs"),
    ("road_min_peak_runs", ROAD_MIN_PEAK_RUNS, "busy roads: ...and this many weekday peak-hour runs"),
    ("free_flow_quantile", FREE_FLOW_Q, "free-flow speed = this quantile of segment run speeds"),
    ("reliability_w_otp", W_OTP, "route reliability score weight: on-time percentage"),
    ("reliability_w_delay", W_DELAY, "route reliability score weight: inverted min-max normalized avg delay"),
    ("reliability_w_bunch", W_BUNCH, "route reliability score weight: 1 - pct_bunched"),
    ("reliability_w_speed", W_SPEED, "route reliability score weight: min-max normalized avg speed"),
    ("anomaly_z_moderate", ANOMALY_Z_MODERATE, "daily anomaly detection threshold (rolling 7d z-score)"),
]

GOLD_DIR.mkdir(parents=True, exist_ok=True)
print("Silver:", SILVER_DIR.resolve())
print("Gold  :", GOLD_DIR.resolve())

# %%
db_path = GOLD_DIR / "gold.duckdb"
db_path.unlink(missing_ok=True)
con = duckdb.connect(str(db_path))
con.execute("SET TimeZone='UTC'")
try:
    con.execute("LOAD icu")
except Exception:
    con.execute("INSTALL icu")
    con.execute("LOAD icu")

LOG = []      # audit log: (kind, label, ok, severity, service_date, detail)
TABLES = {}   # gold table -> row count


def check(label, ok, detail="", severity="error", service_date=None):
    sev = "info" if ok else severity
    LOG.append(("check", label, bool(ok), sev, service_date, str(detail)))
    print(f"{'PASS' if ok else 'FAIL':4s} [{sev.upper():7s}] | {label} | {detail}")


def info(label, detail="", service_date=None):
    LOG.append(("info", label, True, "info", service_date, str(detail)))
    print(f"INFO [INFO   ] | {label} | {detail}")


def scalar(sql):
    return con.execute(sql).fetchone()[0]


def read(name, required=True):
    p = SILVER_DIR / f"{name}.parquet"
    if not p.exists():
        if required:
            raise FileNotFoundError(f"Missing required parquet file: {p}")
        print(f"Note: {p} not found, dependent tables skipped.")
        return None
    con.execute(f"CREATE OR REPLACE TEMP VIEW silver_{name} AS SELECT * FROM read_parquet('{p.as_posix()}')")
    return f"silver_{name}"


def schema(rel):
    return {r[0]: r[1].upper() for r in con.execute(f"DESCRIBE {rel}").fetchall()}


def ts(s, c):
    if c not in s:
        return "CAST(NULL AS TIMESTAMP)"
    t = s[c]
    if t.startswith("TIMESTAMP WITH TIME ZONE") or t.startswith("TIMESTAMPTZ"):
        return f"(\"{c}\" AT TIME ZONE 'UTC')"
    if t.startswith("TIMESTAMP") or t == "DATE":
        return f"CAST(\"{c}\" AS TIMESTAMP)"
    if t == "BIGINT":
        return f"make_timestamp(\"{c}\" // 1000)"
    return f"TRY_CAST(\"{c}\" AS TIMESTAMP)"


def boolean(s, c):
    if c not in s:
        return "FALSE"
    if s[c] == "BOOLEAN":
        return f"COALESCE(\"{c}\", FALSE)"
    return f"COALESCE(lower(trim(CAST(\"{c}\" AS VARCHAR))) IN ('true','1','1.0','t','yes'), FALSE)"


def num(s, c):
    if c not in s:
        return "CAST(NULL AS DOUBLE)"
    x = f"TRY_CAST(\"{c}\" AS DOUBLE)"
    return f"CASE WHEN isnan({x}) THEN NULL ELSE {x} END"


def label(s, c):
    if c not in s:
        return "'unknown'"
    x = f"CAST(\"{c}\" AS VARCHAR)"
    return f"CASE WHEN {x} IS NULL OR {x} IN ('None','nan','') THEN 'unknown' ELSE {x} END"


def to_local(expr):
    return f"(({expr} AT TIME ZONE 'UTC') AT TIME ZONE '{LOCAL_TZ}')"


HOUR_BAND_SQL = (
    "CASE "
    + " ".join(f"WHEN local_hour BETWEEN {lo} AND {hi} THEN '{b}'" for b, (lo, hi) in HOUR_BANDS.items())
    + " ELSE 'unknown' END"
)
WEEKDAY_NAME_SQL = "(['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'])[weekday + 1]"


def hist_cols(prefix, column, cond, edges):
    parts = []
    for i in range(len(edges) + 1):
        if i == 0:
            c = f"{column} < {edges[0]}"
        elif i == len(edges):
            c = f"{column} >= {edges[-1]}"
        else:
            c = f"{column} >= {edges[i-1]} AND {column} < {edges[i]}"
        parts.append(f"SUM(CASE WHEN ({cond}) AND {c} THEN 1 ELSE 0 END) AS {prefix}{i}")
    return ",\n  ".join(parts)


def emit(name, sql):
    """Materialise a gold table inside gold.duckdb AND write gold/<name>.parquet"""
    con.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")
    out = (GOLD_DIR / f"{name}.parquet").as_posix()
    con.execute(f"COPY {name} TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    TABLES[name] = scalar(f"SELECT COUNT(*) FROM {name}")
    print(f"  [EMIT] {name}: {TABLES[name]:,} rows")


def emit_rows(name, ddl, rows):
    con.execute(f"CREATE OR REPLACE TABLE {name} ({ddl})")
    if rows:
        ph = ",".join("?" * len(rows[0]))
        con.executemany(f"INSERT INTO {name} VALUES ({ph})", rows)
    out = (GOLD_DIR / f"{name}.parquet").as_posix()
    con.execute(f"COPY {name} TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    TABLES[name] = len(rows)
    print(f"  [EMIT] {name}: {TABLES[name]:,} rows")


print("Helpers ready.")

# %%
print("Staging clean_stop_visits...")
rel = read("clean_stop_visits")
s = schema(rel)

con.execute(f"""
CREATE OR REPLACE TEMP TABLE sv AS
WITH a AS (
  SELECT
    TRY_CAST(service_date AS DATE)            AS service_date,
    {label(s,'journey_ref')}                  AS journey_ref,
    {label(s,'line_ref')}                     AS line_ref,
    {label(s,'route_short_name')}             AS route_short_name,
    {label(s,'route_type_name')}              AS route_type_name,
    {label(s,'direction_ref')}                AS direction_ref,
    TRY_CAST(stop_order AS INTEGER)           AS stop_order,
    {label(s,'stop_ref')}                     AS stop_ref,
    {label(s,'stop_name')}                    AS stop_name,
    {num(s,'stop_lat')}                       AS stop_lat,
    {num(s,'stop_lon')}                       AS stop_lon,
    {ts(s,'aimed_arrival')}                   AS aimed_arrival,
    {ts(s,'actual_arrival')}                  AS actual_arrival,
    {ts(s,'aimed_departure')}                 AS aimed_departure,
    {ts(s,'actual_departure')}                AS actual_departure,
    {ts(s,'visit_time')}                      AS visit_time,
    {ts(s,'aimed_time')}                      AS aimed_time,
    {boolean(s,'is_actual')}                  AS is_actual,
    {boolean(s,'delay_outlier')}              AS delay_outlier,
    {boolean(s,'on_time')}                    AS on_time_silver,
    {num(s,'delay_s')}                        AS delay_s,
    {num(s,'delay_arrival_s')}                AS delay_arrival_s,
    {num(s,'delay_departure_s')}              AS delay_departure_s
  FROM {rel}
), l AS (
  SELECT *, {to_local('visit_time')} AS visit_local FROM a
), h AS (
  SELECT *,
    COALESCE(hour(visit_local), -1)           AS local_hour,
    COALESCE(isodow(visit_local) - 1, -1)     AS weekday,
    COALESCE(minute(visit_local), 0)          AS local_minute
  FROM l
)
SELECT * EXCLUDE (local_minute, visit_local),
  {HOUR_BAND_SQL}                                                AS hour_band,
  CASE WHEN local_hour >= 0 THEN local_hour * 4 + local_minute // 15 ELSE -1 END AS local_quarter,
  CASE WHEN local_hour >= 0 THEN printf('%02d:%02d', local_hour, (local_minute // 15) * 15) ELSE 'unknown' END AS local_time_bin,
  (is_actual AND NOT delay_outlier AND delay_s IS NOT NULL)      AS valid
FROM h
""")

n_sv = scalar("SELECT COUNT(*) FROM sv")
dates = [str(r[0]) for r in con.execute("SELECT DISTINCT service_date FROM sv WHERE service_date IS NOT NULL ORDER BY 1").fetchall()]
print(f"Staged clean_stop_visits: {n_sv:,} rows | service dates: {dates}")

# Sanity checks
r = con.execute("""
  SELECT avg((abs((epoch(actual_departure)-epoch(aimed_departure)) - delay_s) <= 2)::INT),
         avg((abs((epoch(actual_departure)-epoch(aimed_departure)) + delay_s) <= 2)::INT)
  FROM sv WHERE is_actual AND actual_departure IS NOT NULL AND aimed_departure IS NOT NULL AND delay_s IS NOT NULL""").fetchone()
agree, flipped = (r[0] or 0.0), (r[1] or 0.0)
check("delay_s matches actual_departure - aimed_departure", agree >= 0.85,
      f"agree {agree:.1%}, sign flipped {flipped:.1%}", severity="warning")

# Per-date data quality check for valid call coverage
for d_str, n_tot, n_val in con.execute("""
  SELECT CAST(service_date AS VARCHAR), COUNT(*), SUM(valid::INT)
  FROM sv WHERE service_date IS NOT NULL GROUP BY 1 ORDER BY 1""").fetchall():
    share = n_val / n_tot if n_tot else 0.0
    check(f"valid call coverage >= 50% on {d_str}", share >= 0.50,
          f"{n_val:,}/{n_tot:,} valid ({share:.1%})", severity="warning", service_date=d_str)

# %%
print("Building dimensions, filter table & thresholds...")
emit("dim_line", """
    SELECT line_ref, ANY_VALUE(route_short_name) AS route_short_name, ANY_VALUE(route_type_name) AS route_type_name
    FROM sv GROUP BY line_ref ORDER BY line_ref""")

emit("dim_stop", """
    SELECT stop_ref, ANY_VALUE(stop_name) AS stop_name, AVG(stop_lat) AS stop_lat, AVG(stop_lon) AS stop_lon
    FROM sv GROUP BY stop_ref ORDER BY stop_ref""")

emit("dim_date", """
    WITH daily AS (
      SELECT service_date, SUM(valid::INT) AS n_valid_visits
      FROM sv WHERE service_date IS NOT NULL GROUP BY 1
    )
    SELECT service_date,
           isodow(service_date) - 1 AS weekday,
           strftime(service_date, '%A') AS weekday_name,
           isodow(service_date) >= 6 AS is_weekend,
           weekofyear(service_date) AS iso_week,
           isoyear(service_date) AS iso_year,
           n_valid_visits,
           (n_valid_visits >= 0.2 * MAX(n_valid_visits) OVER ()) AS is_full_date
    FROM daily ORDER BY service_date""")

# Cascading filter table for dashboard sidebar (weeks, dates, routes, stops, bus types)
emit("gold_filters", """
    SELECT
      d.iso_year,
      d.iso_week,
      sv.service_date,
      d.weekday,
      d.weekday_name,
      d.is_full_date,
      sv.line_ref,
      ANY_VALUE(sv.route_short_name) AS route_short_name,
      ANY_VALUE(sv.route_type_name)  AS route_type_name,
      sv.direction_ref,
      sv.stop_ref,
      ANY_VALUE(sv.stop_name)        AS stop_name,
      COUNT(*)                       AS n_calls,
      SUM(sv.valid::INT)             AS n_valid
    FROM sv
    JOIN dim_date d USING (service_date)
    WHERE sv.service_date IS NOT NULL
    GROUP BY d.iso_year, d.iso_week, sv.service_date, d.weekday, d.weekday_name, d.is_full_date,
             sv.line_ref, sv.direction_ref, sv.stop_ref
    ORDER BY sv.service_date, sv.line_ref, sv.direction_ref, sv.stop_ref""")

emit_rows("gold_thresholds", "rule VARCHAR, value DOUBLE, meaning VARCHAR", [(r, float(v), m) for r, v, m in THRESHOLDS])

# %%
print("Building core fact table and additive delay cube...")
emit("fact_stop_visits", """
    SELECT service_date, local_hour, local_quarter, local_time_bin, hour_band, weekday, journey_ref, line_ref,
           route_short_name, route_type_name, direction_ref, stop_order, stop_ref, stop_name, stop_lat, stop_lon,
           aimed_arrival, actual_arrival, aimed_departure, actual_departure, visit_time,
           delay_s, delay_arrival_s, delay_departure_s, is_actual, delay_outlier, valid
    FROM sv""")

MEASURES = f"""
  COUNT(*) AS n_calls,
  SUM(is_actual::INT) AS n_actual,
  SUM((is_actual AND delay_outlier)::INT) AS n_outlier,
  SUM(valid::INT) AS n_valid,
  SUM((valid AND delay_s < {EARLY_S})::INT) AS n_early,
  SUM((valid AND delay_s >= {EARLY_S} AND delay_s < {LATE_S})::INT) AS n_on_time,
  SUM((valid AND delay_s >= {LATE_S})::INT) AS n_late,
  COALESCE(SUM(delay_s) FILTER (WHERE valid), 0) AS sum_delay_s,
  COALESCE(SUM(delay_s * delay_s) FILTER (WHERE valid), 0) AS sum_delay_sq,
  CASE WHEN SUM(valid::INT) > 1
       THEN sqrt(greatest((SUM(delay_s * delay_s) FILTER (WHERE valid)
            - pow(SUM(delay_s) FILTER (WHERE valid), 2) / SUM(valid::INT)) / (SUM(valid::INT) - 1), 0))
       ELSE 0.0 END AS delay_stddev_s,
  {hist_cols('d', 'delay_s', 'valid', DELAY_EDGES)}"""

emit("gold_delay_hour", f"""
    SELECT d.*, l.route_short_name, l.route_type_name FROM (
      SELECT service_date, isoyear(service_date) AS iso_year, weekofyear(service_date) AS iso_week,
             weekday, local_hour, hour_band, line_ref, direction_ref, stop_ref, {MEASURES}
      FROM sv GROUP BY ALL) d
    LEFT JOIN dim_line l USING (line_ref)""")

# %%
print("Building unified headway and speed tables...")
hw_rel = read("stop_headways", required=False)
seg_rel = read("trip_segments", required=False)

# 1. Headways: canonical source is silver stop_headways; fallback derives from sv ordered by actual departure
if hw_rel:
    hs = schema(hw_rel)
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE hw2 AS
    WITH a AS (
      SELECT TRY_CAST(service_date AS DATE) AS service_date,
             {label(hs,'line_ref')} AS line_ref,
             {label(hs,'direction_ref')} AS direction_ref,
             {label(hs,'stop_id')} AS stop_ref,
             {label(hs,'trip_id')} AS trip_id,
             {num(hs,'headway_s')} AS h,
             {num(hs,'scheduled_headway_s')} AS sched,
             {to_local(ts(hs,'visit_time'))} AS visit_local
      FROM {hw_rel})
    SELECT service_date, line_ref, direction_ref, stop_ref, trip_id, h, sched,
           COALESCE(hour(visit_local), -1) AS local_hour,
           isodow(service_date) - 1 AS weekday,
           weekofyear(service_date) AS iso_week,
           isoyear(service_date) AS iso_year,
           {HOUR_BAND_SQL.replace('local_hour', 'COALESCE(hour(visit_local), -1)')} AS hour_band
    FROM a WHERE h > 0 AND sched BETWEEN {HEADWAY_MIN_S} AND {HEADWAY_MAX_S}""")
else:
    info("stop_headways not in silver; deriving headways from clean_stop_visits")
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE hw2 AS
    WITH v AS (SELECT * FROM sv WHERE valid),
    p AS (SELECT *, LAG(aimed_departure) OVER w AS pa, LAG(actual_departure) OVER w AS po
          FROM v WINDOW w AS (PARTITION BY service_date, line_ref, direction_ref, stop_ref ORDER BY actual_departure)),
    a AS (SELECT service_date, line_ref, direction_ref, stop_ref, journey_ref AS trip_id,
                 epoch(actual_departure) - epoch(po) AS h,
                 epoch(aimed_departure) - epoch(pa) AS sched,
                 local_hour, weekday, weekofyear(service_date) AS iso_week, isoyear(service_date) AS iso_year, hour_band
          FROM p WHERE po IS NOT NULL)
    SELECT * FROM a WHERE h > 0 AND sched BETWEEN {HEADWAY_MIN_S} AND {HEADWAY_MAX_S}""")

n_hw_pairs = scalar("SELECT COUNT(*) FROM hw2")

HW_SUMS = f"""
  COUNT(*) AS n_headways,
  COUNT(*) AS n_pairs,
  SUM(h) AS sum_headway_s,
  SUM(h) AS sum_actual_headway_s,
  SUM(sched) AS sum_sched_headway_s,
  SUM(sched) AS sum_aimed_headway_s,
  SUM(h * h) AS sum_headway_sq,
  SUM(h * h) AS sum_actual_headway_sq,
  SUM(sched * sched) AS sum_sched_headway_sq,
  SUM(sched * sched) AS sum_aimed_headway_sq,
  SUM(abs(h - sched)) AS sum_abs_deviation_s,
  SUM((h < {BUNCH_RATIO} * sched)::INT) AS n_bunched,
  SUM((h > {GAP_RATIO} * sched)::INT) AS n_large_gap"""

HW_RATIOS = """
  sum_headway_s / NULLIF(n_headways, 0) AS avg_headway_s,
  sum_sched_headway_s / NULLIF(n_headways, 0) AS avg_sched_headway_s,
  sqrt(greatest(sum_headway_sq / NULLIF(n_headways, 0) - pow(sum_headway_s / NULLIF(n_headways, 0), 2), 0))
    / NULLIF(sum_headway_s / NULLIF(n_headways, 0), 0) AS headway_cv,
  sum_headway_sq / NULLIF(2 * sum_headway_s, 0) - sum_sched_headway_sq / NULLIF(2 * sum_sched_headway_s, 0) AS excess_wait_s,
  n_bunched::DOUBLE / NULLIF(n_headways, 0) AS pct_bunched,
  n_large_gap::DOUBLE / NULLIF(n_headways, 0) AS pct_large_gap"""

emit("gold_headway_hour", f"""
    SELECT h.*, l.route_short_name FROM (
      SELECT service_date, iso_year, iso_week, weekday, local_hour, hour_band,
             line_ref, direction_ref, stop_ref, {HW_SUMS}
      FROM hw2 GROUP BY ALL) h
    LEFT JOIN dim_line l USING (line_ref)""")

emit("gold_bunching_stops", f"""
    SELECT b.*, d.stop_name, d.stop_lat, d.stop_lon,
           CASE WHEN n_headways >= 50 THEN rank() OVER (PARTITION BY n_headways >= 50 ORDER BY pct_bunched DESC) END
             AS bunching_rank
    FROM (SELECT *, {HW_RATIOS} FROM (
            SELECT stop_ref,
                   COUNT(DISTINCT service_date) AS observation_days,
                   COUNT(DISTINCT line_ref) AS n_lines,
                   {HW_SUMS}
            FROM hw2 GROUP BY ALL)) b
    LEFT JOIN dim_stop d USING (stop_ref)
    ORDER BY bunching_rank NULLS LAST""")

# Backward-compatible DuckDB view (no duplicate parquet file)
con.execute("CREATE OR REPLACE VIEW gold_headway AS SELECT * FROM gold_headway_hour")

# 2. Segment speeds & busy roads: canonical source is silver trip_segments
n_road_runs = None
if seg_rel:
    gs = schema(seg_rel)
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE road2 AS
    WITH a AS (
      SELECT TRY_CAST(service_date AS DATE) AS service_date,
             {label(gs,'from_stop_id')} AS from_stop_ref,
             {label(gs,'to_stop_id')} AS to_stop_ref,
             {label(gs,'route_short_name')} AS route_short_name,
             {label(gs,'route_id')} AS line_ref,
             {label(gs,'trip_id')} AS trip_id,
             {label(gs,'direction_ref')} AS direction_ref,
             {num(gs,'observed_elapsed_s')} AS run_s,
             {num(gs,'scheduled_running_time_s')} AS sched_s,
             {num(gs,'segment_distance_m')} AS dist_m,
             {num(gs,'average_speed_kmh')} AS kmh,
             {boolean(gs,'is_valid')} AS is_valid,
             {to_local(ts(gs,'to_visit_time'))} AS arrive_local
      FROM {seg_rel}),
    b AS (
      SELECT * EXCLUDE (arrive_local),
             COALESCE(hour(arrive_local), -1) AS local_hour,
             CASE WHEN isodow(service_date) >= 6 THEN 'weekend' ELSE 'weekday' END AS day_type,
             isodow(service_date) - 1 AS weekday,
             weekofyear(service_date) AS iso_week,
             isoyear(service_date) AS iso_year
      FROM a
      WHERE is_valid AND run_s > 0 AND run_s <= {SEG_MAX_S} AND dist_m >= {ROAD_MIN_M}
        AND kmh > 0 AND kmh <= {ROAD_MAX_KMH})
    SELECT *, {HOUR_BAND_SQL} AS hour_band FROM b""")

    n_road_runs = scalar("SELECT COUNT(*) FROM road2")

    emit("gold_speed_hour", f"""
        WITH ff AS (
          SELECT from_stop_ref, to_stop_ref, quantile_cont(kmh, {FREE_FLOW_Q}) AS free_flow_kmh
          FROM road2 GROUP BY ALL
        )
        SELECT service_date, iso_year, iso_week, weekday, day_type, local_hour, hour_band,
               line_ref, ANY_VALUE(route_short_name) AS route_short_name, direction_ref,
               from_stop_ref, to_stop_ref,
               COUNT(*) AS n_runs,
               COUNT(*) AS n,
               SUM(dist_m) AS sum_distance_m,
               SUM(run_s) AS sum_run_s,
               SUM(run_s) AS sum_actual_s,
               SUM(GREATEST(sched_s, 0)) AS sum_sched_s,
               SUM(GREATEST(sched_s, 0)) AS sum_aimed_s,
               SUM(greatest(run_s - dist_m / (ff.free_flow_kmh / 3.6), 0)) AS sum_excess_s,
               SUM(dist_m) / NULLIF(SUM(run_s), 0) * 3.6 AS avg_kmh,
               CASE WHEN SUM(GREATEST(sched_s, 0)) > 0
                    THEN GREATEST(1.0 - (SUM(GREATEST(sched_s, 0)) / NULLIF(SUM(run_s), 0)), 0.0)
                    ELSE NULL END AS speed_loss_pct
        FROM road2 JOIN ff USING (from_stop_ref, to_stop_ref)
        GROUP BY service_date, iso_year, iso_week, weekday, day_type, local_hour, hour_band,
                 line_ref, direction_ref, from_stop_ref, to_stop_ref""")

    emit("gold_busy_roads", f"""
        WITH ff AS (
          SELECT from_stop_ref, to_stop_ref, quantile_cont(kmh, {FREE_FLOW_Q}) AS free_flow_kmh
          FROM road2 GROUP BY ALL),
        r AS (
          SELECT road2.*, ff.free_flow_kmh,
                 greatest(run_s - dist_m / (ff.free_flow_kmh / 3.6), 0) AS excess_s,
                 (day_type = 'weekday' AND hour_band IN ('am_peak', 'pm_peak')) AS is_peak
          FROM road2 JOIN ff USING (from_stop_ref, to_stop_ref)),
        seg AS (
          SELECT from_stop_ref, to_stop_ref,
                 COUNT(*) AS n_runs,
                 COUNT(*) FILTER (WHERE is_peak) AS n_peak_runs,
                 COUNT(DISTINCT service_date) AS observation_days,
                 COUNT(DISTINCT service_date) AS n_days,
                 COUNT(DISTINCT line_ref) AS n_lines,
                 string_agg(DISTINCT route_short_name, ', ' ORDER BY route_short_name) AS lines,
                 median(dist_m) AS distance_m,
                 ANY_VALUE(free_flow_kmh) AS free_flow_kmh,
                 median(kmh) AS median_kmh,
                 median(kmh) FILTER (WHERE is_peak)               AS peak_kmh,
                 median(kmh) FILTER (WHERE hour_band = 'am_peak') AS am_peak_kmh,
                 median(kmh) FILTER (WHERE hour_band = 'midday')  AS midday_kmh,
                 median(kmh) FILTER (WHERE hour_band = 'pm_peak') AS pm_peak_kmh,
                 median(kmh) FILTER (WHERE hour_band = 'evening') AS evening_kmh,
                 median(kmh) FILTER (WHERE hour_band = 'night')   AS night_kmh,
                 median(run_s - sched_s) FILTER (WHERE is_peak)   AS peak_vs_timetable_s,
                 SUM(excess_s) / 3600                             AS vehicle_hours_lost,
                 SUM(excess_s) FILTER (WHERE is_peak) / 3600      AS peak_vehicle_hours_lost
          FROM r GROUP BY from_stop_ref, to_stop_ref),
        x AS (
          SELECT seg.*,
                 free_flow_kmh / NULLIF(peak_kmh, 0)     AS travel_time_index,
                 1 - peak_kmh / NULLIF(free_flow_kmh, 0) AS peak_speed_loss,
                 (n_runs >= {ROAD_MIN_RUNS} AND n_peak_runs >= {ROAD_MIN_PEAK_RUNS}) AS ranked
          FROM seg)
        SELECT x.*,
               CASE WHEN ranked THEN rank() OVER (PARTITION BY ranked ORDER BY vehicle_hours_lost DESC) END AS rank_hours_lost,
               CASE WHEN ranked THEN rank() OVER (PARTITION BY ranked ORDER BY travel_time_index DESC) END AS rank_slowdown,
               fs.stop_name AS from_stop_name, fs.stop_lat AS from_lat, fs.stop_lon AS from_lon,
               tsd.stop_name AS to_stop_name, tsd.stop_lat AS to_lat, tsd.stop_lon AS to_lon
        FROM x
        LEFT JOIN dim_stop fs  ON fs.stop_ref  = x.from_stop_ref
        LEFT JOIN dim_stop tsd ON tsd.stop_ref = x.to_stop_ref
        ORDER BY rank_hours_lost NULLS LAST""")

    con.execute("""
        CREATE OR REPLACE VIEW gold_segment_hour AS
        SELECT service_date, weekday, local_hour, hour_band, line_ref, route_short_name, direction_ref,
               from_stop_ref, to_stop_ref, n, sum_actual_s, sum_aimed_s
        FROM gold_speed_hour""")

    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE trip_runs AS
    SELECT TRY_CAST(service_date AS DATE) AS service_date, {label(gs,'trip_id')} AS trip_id,
           COUNT(*) AS n_segments,
           SUM({num(gs,'segment_distance_m')}) AS distance_m,
           SUM({num(gs,'observed_elapsed_s')}) AS segments_run_s,
           SUM({num(gs,'scheduled_running_time_s')}) AS segments_sched_s
    FROM {seg_rel}
    WHERE {boolean(gs,'is_valid')} AND {num(gs,'observed_elapsed_s')} > 0 AND {num(gs,'observed_elapsed_s')} <= {SEG_MAX_S}
    GROUP BY ALL""")
else:
    info("trip_segments not in silver", "speed and busy-road tables skipped")

# %%
print("Building unified gold_trip_metrics (with completion status & delay recovery)...")
con.execute("""
CREATE OR REPLACE TEMP TABLE trip_base AS
WITH sched_counts AS (
  SELECT service_date, journey_ref, COUNT(*) AS expected_trip_stops
  FROM sv GROUP BY 1, 2
),
obs AS (
  SELECT service_date, journey_ref,
         ANY_VALUE(line_ref) AS line_ref,
         ANY_VALUE(route_short_name) AS route_short_name,
         ANY_VALUE(direction_ref) AS direction_ref,
         arg_min(local_hour, stop_order) AS start_hour,
         arg_min(weekday, stop_order) AS weekday,
         weekofyear(service_date) AS iso_week,
         isoyear(service_date) AS iso_year,
         COUNT(*) AS n_stops,
         MIN(stop_order) AS first_stop_order,
         MAX(stop_order) AS last_stop_order,
         arg_min(stop_ref, stop_order) AS first_stop_ref,
         arg_max(stop_ref, stop_order) AS last_stop_ref,
         arg_min(delay_s, stop_order) AS start_delay_s,
         arg_max(delay_s, stop_order) AS end_delay_s,
         MAX(delay_s) AS max_delay_s,
         arg_max(delay_s, stop_order) - arg_min(delay_s, stop_order) AS delay_gained_s,
         epoch(arg_max(actual_departure, stop_order)) - epoch(arg_min(actual_departure, stop_order)) AS runtime_actual_s,
         epoch(arg_max(aimed_departure, stop_order))  - epoch(arg_min(aimed_departure, stop_order))  AS runtime_aimed_s
  FROM sv WHERE valid
  GROUP BY service_date, journey_ref
)
SELECT o.*,
       sc.expected_trip_stops,
       CASE
         WHEN o.n_stops = 1 THEN 'single_stop'
         WHEN o.n_stops >= sc.expected_trip_stops THEN 'complete'
         ELSE 'partial'
       END AS trip_completion_status,
       CASE
         WHEN o.start_delay_s > 0 THEN ROUND((o.start_delay_s - o.end_delay_s)::DOUBLE / o.start_delay_s, 4)
         ELSE NULL
       END AS delay_recovery_pct
FROM obs o
JOIN sched_counts sc USING (service_date, journey_ref)
""")

trip_parts = ["t.*"]
trip_joins = ""
if seg_rel:
    trip_parts += [
        "r.n_segments", "r.distance_m", "r.segments_run_s", "r.segments_sched_s",
        "r.distance_m / NULLIF(r.segments_run_s, 0) * 3.6 AS avg_kmh",
    ]
    trip_joins += " LEFT JOIN trip_runs r ON r.service_date = t.service_date AND r.trip_id = t.journey_ref"

con.execute(f"""CREATE OR REPLACE TEMP TABLE trip_hw AS
    SELECT service_date, trip_id, COUNT(*) AS n_headways,
           SUM((h < {BUNCH_RATIO} * sched)::INT) AS n_bunched,
           SUM((h > {GAP_RATIO} * sched)::INT) AS n_large_gap
    FROM hw2 GROUP BY ALL""")
trip_parts += [
    "COALESCE(b.n_headways, 0) AS n_headways",
    "COALESCE(b.n_bunched, 0) AS n_bunched",
    "COALESCE(b.n_large_gap, 0) AS n_large_gap",
    "b.n_bunched::DOUBLE / NULLIF(b.n_headways, 0) AS pct_bunched",
]
trip_joins += " LEFT JOIN trip_hw b ON b.service_date = t.service_date AND b.trip_id = t.journey_ref"

emit("gold_trip_metrics", f"SELECT {', '.join(trip_parts)} FROM trip_base t{trip_joins}")
con.execute("CREATE OR REPLACE VIEW gold_trip_summary AS SELECT * FROM gold_trip_metrics")

# %%
print("Building fleet, GPS evidence, stop coverage and event-status tables...")

tv = read("trip_vehicle", required=False)
if tv:
    emit("gold_fleet_activity", f"""
        SELECT TRY_CAST(service_date AS DATE) AS service_date, COUNT(*) AS n_trips,
               COUNT(DISTINCT vehicle_id) AS n_vehicles, SUM(n_stops) AS sum_stops, AVG(median_m) AS avg_median_m
        FROM {tv} GROUP BY 1 ORDER BY 1""")

gps = read("stop_gps_status", required=False)
if gps:
    g = schema(gps)
    emit("gold_stop_evidence", f"""
        WITH ev AS (
          SELECT TRY_CAST(service_date AS DATE) AS service_date,
                 {label(g,'route_short_name')} AS route_short_name,
                 CAST(route_id AS VARCHAR) AS gtfs_route_id,
                 CAST(stop_id AS VARCHAR)  AS stop_ref,
                 {label(g,'gps_class')}    AS gps_class,
                 {num(g,'time_lost_s')}    AS time_lost_s,
                 {num(g,'p_excess')}       AS p_excess,
                 COALESCE(hour({to_local(ts(g,'visit_time'))}), -1) AS local_hour
          FROM {gps})
        SELECT service_date, {HOUR_BAND_SQL} AS hour_band, route_short_name, gtfs_route_id, stop_ref, gps_class,
               COUNT(*) AS n_calls, COUNT(time_lost_s) AS n_time_lost,
               COALESCE(SUM(time_lost_s), 0) AS sum_time_lost_s,
               SUM((p_excess <= 0.05)::INT) AS n_p_excess_le_05,
               {hist_cols('tl', 'time_lost_s', 'time_lost_s IS NOT NULL', TL_EDGES)}
        FROM ev GROUP BY ALL""")

nul = read("time_lost_null", required=False)
if nul:
    x = num(schema(nul), "time_lost_s")
    bucket = " + ".join(f"CASE WHEN x >= {e} THEN 1 ELSE 0 END" for e in TL_EDGES)
    counts = dict(con.execute(
        f"SELECT b, COUNT(*) FROM (SELECT {bucket} AS b FROM (SELECT {x} AS x FROM {nul}) WHERE x IS NOT NULL) GROUP BY b"
    ).fetchall())
    emit_rows("gold_time_lost_reference", "bucket VARCHAR, n_stop_free_intervals BIGINT",
              [(f"tl{i}", int(counts.get(i, 0))) for i in range(len(TL_EDGES) + 1)])

exp = read("expected_per_stop", required=False)
if exp:
    m = scalar(f"SELECT COUNT(DISTINCT stop_ref) FROM sv WHERE stop_ref IN (SELECT CAST(stop_id AS VARCHAR) FROM {exp})")
    info("expected_per_stop.stop_id matches clean_stop_visits.stop_ref", f"{m:,} visited stops matched")
    if m > 0:
        emit("gold_stop_coverage", f"""
            SELECT e.service_date,
                   e.stop_ref,
                   e.expected_visits,
                   COALESCE(o.n_calls, 0) AS n_observed_calls,
                   GREATEST(e.expected_visits - COALESCE(o.n_calls, 0), 0) AS missing_visits,
                   COALESCE(o.n_calls, 0)::DOUBLE / NULLIF(e.expected_visits, 0) AS observed_to_expected
            FROM (SELECT TRY_CAST(service_date AS DATE) AS service_date, CAST(stop_id AS VARCHAR) AS stop_ref,
                         SUM(expected_visits) AS expected_visits FROM {exp} GROUP BY 1, 2) e
            LEFT JOIN (SELECT service_date, stop_ref, SUM(is_actual::INT) AS n_calls FROM sv GROUP BY 1, 2) o
                   USING (service_date, stop_ref)""")

ses = read("stop_event_status", required=False)
if ses:
    q_ses = schema(ses)
    emit("gold_event_status", f"""
        SELECT TRY_CAST(service_date AS DATE) AS service_date, {label(q_ses,'route_short_name')} AS route_short_name,
               CAST(stop_id AS VARCHAR) AS stop_ref, {label(q_ses,'visit_status')} AS visit_status,
               {label(q_ses,'final_status')} AS final_status, COUNT(*) AS n_events
        FROM {ses} GROUP BY ALL""")

# %%
print("Building route reliability (daily & weekly), completeness, comparison & daily anomalies...")

# A. gold_route_reliability_daily
speed_daily_cte = """
    SELECT service_date, line_ref, direction_ref,
           SUM(sum_distance_m) AS sum_distance_m,
           SUM(sum_run_s) AS sum_run_s,
           SUM(sum_distance_m) / NULLIF(SUM(sum_run_s), 0) * 3.6 AS avg_speed_kmh
    FROM gold_speed_hour GROUP BY 1, 2, 3
""" if seg_rel else """
    SELECT service_date, line_ref, direction_ref,
           CAST(NULL AS DOUBLE) AS sum_distance_m,
           CAST(NULL AS DOUBLE) AS sum_run_s,
           CAST(NULL AS DOUBLE) AS avg_speed_kmh
    FROM sv WHERE FALSE
"""

emit("gold_route_reliability_daily", f"""
    WITH sv_agg AS (
      SELECT service_date, line_ref, ANY_VALUE(route_short_name) AS route_short_name, direction_ref,
             COUNT(DISTINCT journey_ref) FILTER (WHERE valid) AS n_trips,
             COUNT(*) AS n_expected_calls,
             SUM(is_actual::INT) AS n_actual_calls,
             SUM(valid::INT) AS n_valid_calls,
             SUM((valid AND delay_s < {EARLY_S})::INT) AS n_early,
             SUM((valid AND delay_s >= {EARLY_S} AND delay_s < {LATE_S})::INT) AS n_on_time,
             SUM((valid AND delay_s >= {LATE_S})::INT) AS n_late,
             COALESCE(SUM(delay_s) FILTER (WHERE valid), 0) AS sum_delay_s,
             AVG(delay_s) FILTER (WHERE valid) AS avg_delay_s,
             quantile_cont(delay_s, 0.5) FILTER (WHERE valid) AS median_delay_s,
             quantile_cont(delay_s, 0.9) FILTER (WHERE valid) AS p90_delay_s,
             SUM((valid AND delay_s >= {EARLY_S} AND delay_s < {LATE_S})::INT)::DOUBLE
               / NULLIF(SUM(valid::INT), 0) AS pct_on_time,
             SUM((valid AND delay_s < {EARLY_S})::INT)::DOUBLE
               / NULLIF(SUM(valid::INT), 0) AS pct_early,
             SUM((valid AND delay_s >= {LATE_S})::INT)::DOUBLE
               / NULLIF(SUM(valid::INT), 0) AS pct_late
      FROM sv WHERE service_date IS NOT NULL
      GROUP BY service_date, line_ref, direction_ref
    ),
    hw_agg AS (
      SELECT service_date, line_ref, direction_ref,
             SUM(n_headways) AS n_headways,
             SUM(sum_headway_s) AS sum_headway_s,
             SUM(sum_sched_headway_s) AS sum_sched_headway_s,
             SUM(sum_headway_sq) AS sum_headway_sq,
             SUM(n_bunched) AS n_bunched,
             SUM(n_large_gap) AS n_large_gap,
             SUM(sum_headway_s) / NULLIF(SUM(n_headways), 0) AS avg_headway_s,
             SUM(sum_sched_headway_s) / NULLIF(SUM(n_headways), 0) AS avg_sched_headway_s,
             sqrt(greatest(SUM(sum_headway_sq) / NULLIF(SUM(n_headways), 0)
               - pow(SUM(sum_headway_s) / NULLIF(SUM(n_headways), 0), 2), 0))
               / NULLIF(SUM(sum_headway_s) / NULLIF(SUM(n_headways), 0), 0) AS headway_cv,
             SUM(n_bunched)::DOUBLE / NULLIF(SUM(n_headways), 0) AS pct_bunched,
             SUM(n_large_gap)::DOUBLE / NULLIF(SUM(n_headways), 0) AS pct_large_gap
      FROM gold_headway_hour GROUP BY 1, 2, 3
    ),
    tr_agg AS (
      SELECT service_date, line_ref, direction_ref,
             AVG(runtime_actual_s) FILTER (WHERE n_stops >= 2) AS avg_trip_runtime_s,
             AVG(runtime_aimed_s) FILTER (WHERE n_stops >= 2)  AS avg_trip_runtime_aimed_s
      FROM gold_trip_metrics GROUP BY 1, 2, 3
    ),
    sp_agg AS ({speed_daily_cte})
    SELECT
      s.service_date, s.line_ref, s.route_short_name, s.direction_ref,
      s.n_trips, s.n_expected_calls, s.n_actual_calls,
      s.avg_delay_s, s.median_delay_s, s.p90_delay_s,
      s.pct_on_time, s.pct_early, s.pct_late,
      h.avg_headway_s, h.avg_sched_headway_s, h.headway_cv,
      h.pct_bunched, h.pct_large_gap,
      t.avg_trip_runtime_s, t.avg_trip_runtime_aimed_s,
      sp.avg_speed_kmh
    FROM sv_agg s
    LEFT JOIN hw_agg h  USING (service_date, line_ref, direction_ref)
    LEFT JOIN tr_agg t  USING (service_date, line_ref, direction_ref)
    LEFT JOIN sp_agg sp USING (service_date, line_ref, direction_ref)
    ORDER BY s.service_date, s.line_ref, s.direction_ref""")

# B. gold_route_reliability_weekly (aggregated from underlying counts/denominators, never avg of daily %)
speed_weekly_cte = """
    SELECT iso_year, iso_week, line_ref,
           SUM(sum_distance_m) / NULLIF(SUM(sum_run_s), 0) * 3.6 AS avg_speed_kmh
    FROM gold_speed_hour GROUP BY 1, 2, 3
""" if seg_rel else """
    SELECT CAST(NULL AS INTEGER) AS iso_year, CAST(NULL AS INTEGER) AS iso_week,
           CAST(NULL AS VARCHAR) AS line_ref, CAST(NULL AS DOUBLE) AS avg_speed_kmh
    WHERE FALSE
"""

emit("gold_route_reliability_weekly", f"""
    WITH sv_wk AS (
      SELECT isoyear(service_date) AS iso_year,
             weekofyear(service_date) AS iso_week,
             line_ref,
             ANY_VALUE(route_short_name) AS route_short_name,
             COUNT(DISTINCT service_date) AS n_service_dates,
             COUNT(DISTINCT (service_date, journey_ref)) FILTER (WHERE valid) AS n_trips,
             SUM(valid::INT) AS n_calls,
             SUM(delay_s) FILTER (WHERE valid) / NULLIF(SUM(valid::INT), 0) AS avg_delay_s,
             quantile_cont(delay_s, 0.5) FILTER (WHERE valid) AS median_delay_s,
             quantile_cont(delay_s, 0.9) FILTER (WHERE valid) AS p90_delay_s,
             SUM((valid AND delay_s >= {EARLY_S} AND delay_s < {LATE_S})::INT)::DOUBLE
               / NULLIF(SUM(valid::INT), 0) AS pct_on_time,
             SUM((valid AND delay_s >= {LATE_S})::INT)::DOUBLE
               / NULLIF(SUM(valid::INT), 0) AS pct_late
      FROM sv WHERE service_date IS NOT NULL
      GROUP BY 1, 2, 3
    ),
    hw_wk AS (
      SELECT iso_year, iso_week, line_ref,
             SUM(sum_headway_s) / NULLIF(SUM(n_headways), 0) AS avg_headway_s,
             sqrt(greatest(SUM(sum_headway_sq) / NULLIF(SUM(n_headways), 0)
               - pow(SUM(sum_headway_s) / NULLIF(SUM(n_headways), 0), 2), 0))
               / NULLIF(SUM(sum_headway_s) / NULLIF(SUM(n_headways), 0), 0) AS headway_cv,
             SUM(n_bunched)::DOUBLE / NULLIF(SUM(n_headways), 0) AS pct_bunched
      FROM gold_headway_hour GROUP BY 1, 2, 3
    ),
    sp_wk AS ({speed_weekly_cte})
    SELECT
      s.iso_year, s.iso_week, s.line_ref, s.route_short_name,
      s.n_service_dates, s.n_trips, s.n_calls,
      s.avg_delay_s, s.median_delay_s, s.p90_delay_s,
      s.pct_on_time, s.pct_late,
      h.avg_headway_s, h.headway_cv, h.pct_bunched,
      sp.avg_speed_kmh
    FROM sv_wk s
    LEFT JOIN hw_wk h  USING (iso_year, iso_week, line_ref)
    LEFT JOIN sp_wk sp USING (iso_year, iso_week, line_ref)
    ORDER BY s.iso_year, s.iso_week, s.line_ref""")

# D. gold_service_completeness (distinguishing cancelled events from uncaptured telemetry)
cancel_cte = f"""
    SELECT TRY_CAST(service_date AS DATE) AS service_date,
           {label(schema(ses),'route_short_name')} AS route_short_name,
           COUNT(*) FILTER (WHERE lower({label(schema(ses),'final_status')}) LIKE '%cancel%'
                               OR lower({label(schema(ses),'visit_status')}) LIKE '%cancel%') AS cancelled_stop_visits
    FROM {ses} GROUP BY 1, 2
""" if ses else """
    SELECT CAST(NULL AS DATE) AS service_date, CAST(NULL AS VARCHAR) AS route_short_name,
           0::BIGINT AS cancelled_stop_visits WHERE FALSE
"""

emit("gold_service_completeness", f"""
    WITH base AS (
      SELECT service_date, line_ref, ANY_VALUE(route_short_name) AS route_short_name, direction_ref,
             COUNT(DISTINCT journey_ref) AS expected_trips,
             COUNT(DISTINCT journey_ref) FILTER (WHERE is_actual) AS observed_trips,
             COUNT(*) AS expected_stop_visits,
             SUM(is_actual::INT) AS observed_stop_visits
      FROM sv WHERE service_date IS NOT NULL
      GROUP BY service_date, line_ref, direction_ref
    ),
    canc AS ({cancel_cte})
    SELECT b.service_date, b.line_ref, b.route_short_name, b.direction_ref,
           b.expected_trips, b.observed_trips,
           GREATEST(b.expected_trips - b.observed_trips, 0) AS missing_trips,
           b.expected_stop_visits, b.observed_stop_visits,
           GREATEST(b.expected_stop_visits - b.observed_stop_visits, 0) AS missing_stop_visits,
           COALESCE(c.cancelled_stop_visits, 0) AS cancelled_stop_visits,
           GREATEST(b.expected_stop_visits - b.observed_stop_visits - COALESCE(c.cancelled_stop_visits, 0), 0)
             AS uncaptured_stop_visits,
           b.observed_trips::DOUBLE / NULLIF(b.expected_trips, 0) AS trip_completion_pct,
           b.observed_stop_visits::DOUBLE / NULLIF(b.expected_stop_visits, 0) AS stop_completion_pct
    FROM base b
    LEFT JOIN canc c ON b.service_date = c.service_date AND b.route_short_name = c.route_short_name
    ORDER BY b.service_date, b.line_ref, b.direction_ref""")

# E. gold_route_comparison (whole-period ranking with documented min-max normalized composite score)
speed_comp_cte = """
    SELECT line_ref, SUM(sum_distance_m) / NULLIF(SUM(sum_run_s), 0) * 3.6 AS avg_speed_kmh
    FROM gold_speed_hour GROUP BY 1
""" if seg_rel else """
    SELECT CAST(NULL AS VARCHAR) AS line_ref, CAST(NULL AS DOUBLE) AS avg_speed_kmh WHERE FALSE
"""

emit("gold_route_comparison", f"""
    WITH bounds AS (
      SELECT MIN(service_date) AS period_start, MAX(service_date) AS period_end
      FROM sv WHERE service_date IS NOT NULL
    ),
    sv_r AS (
      SELECT line_ref, ANY_VALUE(route_short_name) AS route_short_name,
             COUNT(DISTINCT (service_date, journey_ref)) FILTER (WHERE valid) AS n_trips,
             SUM(valid::INT) AS n_valid_calls,
             SUM(delay_s) FILTER (WHERE valid) / NULLIF(SUM(valid::INT), 0) AS avg_delay_s,
             SUM((valid AND delay_s >= {EARLY_S} AND delay_s < {LATE_S})::INT)::DOUBLE
               / NULLIF(SUM(valid::INT), 0) AS pct_on_time,
             SUM((valid AND delay_s >= {LATE_S})::INT)::DOUBLE
               / NULLIF(SUM(valid::INT), 0) AS pct_late
      FROM sv WHERE valid GROUP BY 1
    ),
    hw_r AS (
      SELECT line_ref,
             sqrt(greatest(SUM(sum_headway_sq) / NULLIF(SUM(n_headways), 0)
               - pow(SUM(sum_headway_s) / NULLIF(SUM(n_headways), 0), 2), 0))
               / NULLIF(SUM(sum_headway_s) / NULLIF(SUM(n_headways), 0), 0) AS headway_cv,
             SUM(n_bunched)::DOUBLE / NULLIF(SUM(n_headways), 0) AS pct_bunched
      FROM gold_headway_hour GROUP BY 1
    ),
    sp_r AS ({speed_comp_cte}),
    joined AS (
      SELECT b.period_start, b.period_end,
             s.line_ref, s.route_short_name, s.n_trips, s.n_valid_calls,
             s.avg_delay_s, s.pct_on_time, s.pct_late,
             sp.avg_speed_kmh, h.headway_cv, h.pct_bunched
      FROM sv_r s
      CROSS JOIN bounds b
      LEFT JOIN hw_r h  USING (line_ref)
      LEFT JOIN sp_r sp USING (line_ref)
    ),
    norm AS (
      SELECT *,
             -- Inverted min-max normalized delay (1.0 = lowest avg delay across routes)
             CASE WHEN MAX(avg_delay_s) OVER () > MIN(avg_delay_s) OVER ()
                  THEN 1.0 - (avg_delay_s - MIN(avg_delay_s) OVER ()) / (MAX(avg_delay_s) OVER () - MIN(avg_delay_s) OVER ())
                  ELSE 1.0 END AS norm_delay,
             -- Min-max normalized speed (1.0 = fastest avg speed across routes)
             CASE WHEN avg_speed_kmh IS NULL THEN NULL
                  WHEN MAX(avg_speed_kmh) OVER () > MIN(avg_speed_kmh) OVER ()
                  THEN (avg_speed_kmh - MIN(avg_speed_kmh) OVER ()) / (MAX(avg_speed_kmh) OVER () - MIN(avg_speed_kmh) OVER ())
                  ELSE 1.0 END AS norm_speed,
             CASE WHEN pct_bunched IS NULL THEN NULL ELSE 1.0 - LEAST(GREATEST(pct_bunched, 0.0), 1.0) END AS norm_bunch
      FROM joined
    ),
    scored AS (
      SELECT period_start, period_end, line_ref, route_short_name,
             avg_delay_s, pct_on_time, pct_late, avg_speed_kmh, headway_cv, pct_bunched,
             ROUND(100.0 * (
               {W_OTP} * COALESCE(pct_on_time, 0)
               + {W_DELAY} * COALESCE(norm_delay, 0)
               + CASE WHEN norm_bunch IS NOT NULL THEN {W_BUNCH} * norm_bunch ELSE 0 END
               + CASE WHEN norm_speed IS NOT NULL THEN {W_SPEED} * norm_speed ELSE 0 END
             ) / NULLIF(
               {W_OTP} + {W_DELAY}
               + CASE WHEN norm_bunch IS NOT NULL THEN {W_BUNCH} ELSE 0 END
               + CASE WHEN norm_speed IS NOT NULL THEN {W_SPEED} ELSE 0 END, 0
             ), 2) AS reliability_score
      FROM norm
    )
    SELECT *, rank() OVER (ORDER BY reliability_score DESC NULLS LAST) AS reliability_rank
    FROM scored
    ORDER BY reliability_rank""")

# C. gold_anomaly_daily (rolling 7-day statistical baseline across routes, stops & road segments)
seg_anomaly_union = """
    UNION ALL
    SELECT service_date,
           'segment' AS entity_type,
           from_stop_ref || '->' || to_stop_ref AS entity_ref,
           'avg_speed_kmh' AS metric,
           SUM(sum_distance_m) / NULLIF(SUM(sum_run_s), 0) * 3.6 AS observed_value
    FROM gold_speed_hour
    GROUP BY service_date, from_stop_ref, to_stop_ref
    HAVING SUM(n_runs) >= 10
""" if seg_rel else ""

emit("gold_anomaly_daily", f"""
    WITH raw_metrics AS (
      -- 1. Route average delay
      SELECT service_date, 'route' AS entity_type, line_ref AS entity_ref,
             'avg_delay_s' AS metric,
             SUM(delay_s) FILTER (WHERE valid) / NULLIF(SUM(valid::INT), 0) AS observed_value
      FROM sv WHERE valid GROUP BY service_date, line_ref HAVING SUM(valid::INT) >= 20

      UNION ALL
      -- 2. Route headway CV
      SELECT service_date, 'route' AS entity_type, line_ref AS entity_ref,
             'headway_cv' AS metric,
             sqrt(greatest(SUM(sum_headway_sq) / NULLIF(SUM(n_headways), 0)
               - pow(SUM(sum_headway_s) / NULLIF(SUM(n_headways), 0), 2), 0))
               / NULLIF(SUM(sum_headway_s) / NULLIF(SUM(n_headways), 0), 0) AS observed_value
      FROM gold_headway_hour GROUP BY service_date, line_ref HAVING SUM(n_headways) >= 15

      UNION ALL
      -- 3. Stop missed visits rate
      SELECT service_date, 'stop' AS entity_type, stop_ref AS entity_ref,
             'missed_visits_pct' AS metric,
             SUM((NOT is_actual)::INT)::DOUBLE / NULLIF(COUNT(*), 0) AS observed_value
      FROM sv WHERE service_date IS NOT NULL GROUP BY service_date, stop_ref HAVING COUNT(*) >= 15
      {seg_anomaly_union}
    ),
    baselined AS (
      SELECT *,
             AVG(observed_value) OVER w AS baseline_value,
             stddev_samp(observed_value) OVER w AS baseline_std,
             COUNT(observed_value) OVER w AS history_days
      FROM raw_metrics
      WINDOW w AS (
        PARTITION BY entity_type, entity_ref, metric
        ORDER BY service_date
        ROWS BETWEEN {ANOMALY_WINDOW_DAYS} PRECEDING AND 1 PRECEDING
      )
    ),
    scored AS (
      SELECT service_date, entity_type, entity_ref, metric,
             observed_value,
             baseline_value,
             observed_value - baseline_value AS deviation,
             (observed_value - baseline_value) / NULLIF(abs(baseline_value), 0) AS deviation_pct,
             (observed_value - baseline_value) / NULLIF(baseline_std, 0) AS z_score
      FROM baselined
      WHERE history_days >= {ANOMALY_MIN_HISTORY}
        AND baseline_std IS NOT NULL
        AND baseline_std > 1e-6
    )
    SELECT service_date, entity_type, entity_ref, metric,
           ROUND(observed_value, 4) AS observed_value,
           ROUND(baseline_value, 4) AS baseline_value,
           ROUND(deviation, 4) AS deviation,
           ROUND(deviation_pct, 4) AS deviation_pct,
           CASE
             WHEN abs(z_score) >= {ANOMALY_Z_CRITICAL} THEN 'critical'
             WHEN abs(z_score) >= {ANOMALY_Z_HIGH}     THEN 'high'
             ELSE 'moderate'
           END AS severity,
           CASE WHEN z_score > 0 THEN 'unusually_high' ELSE 'unusually_low' END AS anomaly_type
    FROM scored
    WHERE (metric IN ('avg_delay_s', 'headway_cv', 'missed_visits_pct') AND z_score >= {ANOMALY_Z_MODERATE})
       OR (metric = 'avg_speed_kmh' AND z_score <= -{ANOMALY_Z_MODERATE})
    ORDER BY service_date DESC, abs(z_score) DESC""")

# %%
print("Running reconciliation audit...")
n_buckets = len(DELAY_EDGES) + 1
check("fact_stop_visits holds every call", scalar("SELECT COUNT(*) FROM fact_stop_visits") == n_sv)
check("gold_delay_hour holds every call", scalar("SELECT SUM(n_calls) FROM gold_delay_hour") == n_sv)
check("early + on_time + late = valid", scalar("SELECT SUM(n_early + n_on_time + n_late - n_valid) FROM gold_delay_hour") == 0)
check("histogram buckets add up to valid",
      scalar(f"SELECT SUM({' + '.join(f'd{i}' for i in range(n_buckets))} - n_valid) FROM gold_delay_hour") == 0)
check("gold_filters covers all valid calls", scalar("SELECT SUM(n_valid) FROM gold_filters") == scalar("SELECT SUM(valid::INT) FROM sv"))

cube_sum = scalar("SELECT SUM(sum_delay_s) FROM gold_delay_hour") or 0.0
row_sum = scalar("SELECT SUM(delay_s) FROM sv WHERE valid") or 0.0
check("sum of delays matches row-level table", abs(cube_sum - row_sum) <= 1e-4, f"cube {cube_sum:,.1f}s | rows {row_sum:,.1f}s")

check("gold_trip_metrics has one row per observed trip",
      scalar("SELECT COUNT(*) FROM gold_trip_metrics") == scalar("SELECT COUNT(DISTINCT (service_date, journey_ref)) FROM sv WHERE valid"))
check("gold_trip_metrics stop counts add up to valid calls",
      scalar("SELECT SUM(n_stops) FROM gold_trip_metrics") == scalar("SELECT SUM(valid::INT) FROM sv"))

check("gold_headway_hour holds every usable headway", scalar("SELECT SUM(n_headways) FROM gold_headway_hour") == n_hw_pairs, f"{n_hw_pairs:,}")
check("gold_bunching_stops holds every usable headway", scalar("SELECT SUM(n_headways) FROM gold_bunching_stops") == n_hw_pairs)
check("bunched + large gap never exceed headways",
      scalar("SELECT COUNT(*) FROM gold_headway_hour WHERE n_bunched + n_large_gap > n_headways") == 0)

if n_road_runs is not None:
    check("gold_speed_hour holds every usable segment run", scalar("SELECT SUM(n_runs) FROM gold_speed_hour") == n_road_runs, f"{n_road_runs:,}")
    check("gold_busy_roads holds every usable segment run", scalar("SELECT SUM(n_runs) FROM gold_busy_roads") == n_road_runs)
    check("busy roads: free-flow speed >= median speed",
          scalar("SELECT COUNT(*) FROM gold_busy_roads WHERE free_flow_kmh < median_kmh - 1e-9") == 0)

check("weekly reliability calls match daily valid calls",
      scalar("SELECT SUM(n_calls) FROM gold_route_reliability_weekly") == scalar("SELECT SUM(valid::INT) FROM sv"))
check("service completeness expected visits match sv",
      scalar("SELECT SUM(expected_stop_visits) FROM gold_service_completeness") == n_sv)
check("route comparison ranks every valid route",
      scalar("SELECT COUNT(*) FROM gold_route_comparison") == scalar("SELECT COUNT(DISTINCT line_ref) FROM sv WHERE valid"))

# %%
print("Publishing audit log, metadata and headline metrics...")
emit_rows("gold_data_quality",
          "kind VARCHAR, label VARCHAR, ok BOOLEAN, severity VARCHAR, service_date VARCHAR, detail VARCHAR",
          LOG)

n_pass = sum(1 for k, _, ok, _, _, _ in LOG if k == "check" and ok)
n_fail = sum(1 for k, _, ok, _, _, _ in LOG if k == "check" and not ok)
meta = {
    "built_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
    "engine": f"duckdb {duckdb.__version__}",
    "service_dates": ",".join(dates),
    "timezone": LOCAL_TZ,
    "on_time_rule": f"{EARLY_S} <= delay_s < {LATE_S}",
    "bunching_rule": f"headway < {BUNCH_RATIO} x scheduled; large gap > {GAP_RATIO} x scheduled",
    "reliability_formula": f"{W_OTP}*pct_on_time + {W_DELAY}*norm_delay + {W_BUNCH}*(1-pct_bunched) + {W_SPEED}*norm_speed",
    "checks_passed": str(n_pass),
    "checks_failed": str(n_fail),
}
meta.update({f"rows_{k}": str(v) for k, v in TABLES.items()})
emit_rows("gold_meta", "key VARCHAR, value VARCHAR", list(meta.items()))
con.close()

print(f"\ngold.duckdb written with {len(TABLES)} tables -> {db_path.resolve()}")
failed = [l for k, l, ok, sev, _, _ in LOG if k == "check" and not ok and sev == "error"]
assert not failed, f"Failed critical checks: {failed}"