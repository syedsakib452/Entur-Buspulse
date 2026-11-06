# %% [markdown]
# # Gold Layer ETL (DuckDB)
# 
# Builds the **Gold layer** for Kolumbus/Entur bus analytics from the cleaned Silver parquet files.
# 
# **Why DuckDB instead of Spark:** the previous version failed on Windows at the first parquet write
# (`HADOOP_HOME and hadoop.home.dir are unset`, i.e. Spark needs `winutils.exe`). The Silver data is small
# (87k rows), so DuckDB does the same work with no Java/Hadoop and writes the same parquet files + `gold.duckdb`.
# 
# ### Silver inputs
# | file | rows |
# |---|---|
# | `clean_stop_visits` | 87,279 |
# | `expected_per_stop` | 13,780 |
# | `per_stop_evidence` | 2,173 |
# | `stop_event_status` | 87,092 |
# | `stop_gps_status` | 86,241 |
# | `time_lost_null` | 47,480 |
# | `trip_vehicle` | 2,600 |
# 
# ### Gold outputs
# - **Dimensions:** `dim_line`, `dim_stop`, `dim_date`, `dim_hour_band`, `gold_buckets`
# - **Cubes / fact:** `gold_visits_cube`, `gold_stop_cube`, `gold_stop_order_delay`, `fact_stop_visits`
# - **Heatmap-ready (new):** `gold_heat_route_hour`, `gold_heat_route_quarter`, `gold_heat_weekday_hour`, `gold_heat_stop_hour`, `gold_heat_route_stop_hour`, `gold_stop_map`
# - **Operations:** `gold_segment_runtime`, `gold_headway`
# - **Dashboard KPIs:** `gold_segment_hour` (road-segment run time and speed by date and hour), `gold_trip_summary` (one row per trip: start/end delay, delay gained, run time)
# - **Evidence / fleet / coverage:** `gold_stop_evidence`, `gold_time_lost_reference`, `gold_fleet_activity`, `gold_stop_coverage`, `gold_event_status`
# - **Audit:** `gold_data_quality`, `gold_meta`
# 
# Tables that depend on a missing Silver file, or on a key that does not match between files, are skipped (and logged), never guessed.

# %%
import os
from pathlib import Path
import datetime as dt
import duckdb

# ---------------- Directory Settings ----------------
SILVER_DIR = Path("../../tables/parquet_2026-10-24/silver-layer-parquet")          # folder that holds the .parquet files
GOLD_DIR = Path("../../tables/parquet_2026-10-24/gold-layer-parquet")       # cleaned outputs are written here

# ---------------- Analysis Configuration ----------------
LOCAL_TZ = "Europe/Oslo"
EARLY_S, LATE_S = -60, 300                        # on time: -60 <= delay_s < 300
DELAY_EDGES = [-120, -60, 0, 60, 180, 300, 600]   # delay histogram edges (s)
TL_EDGES    = [0, 5, 10, 20, 40]                  # time-lost histogram edges (s)
HOUR_BANDS  = {"night": (0, 5), "am_peak": (6, 8), "midday": (9, 14), "pm_peak": (15, 17), "evening": (18, 23)}
SEG_MAX_S = 1800
HEADWAY_MIN_S, HEADWAY_MAX_S = 120, 3600
BUNCH_RATIO, GAP_RATIO = 0.5, 1.5

BAND_OF_HOUR = {h: b for b, (lo, hi) in HOUR_BANDS.items() for h in range(lo, hi + 1)}
GOLD_DIR.mkdir(parents=True, exist_ok=True)
print("Silver:", SILVER_DIR.resolve())
print("Gold  :", GOLD_DIR.resolve())

# %%
db_path = GOLD_DIR / "gold.duckdb"
db_path.unlink(missing_ok=True)
con = duckdb.connect(str(db_path))
con.execute("SET TimeZone='UTC'")
try:
    con.execute("LOAD icu")                               # time-zone support, normally bundled
except Exception:
    con.execute("INSTALL icu"); con.execute("LOAD icu")

LOG = []      # audit log: checks (pass/fail) and info lines
TABLES = {}   # gold table -> row count

def check(label, ok, detail=""):
    LOG.append(("check", label, bool(ok), str(detail)))
    print(f"{'PASS' if ok else 'FAIL':4s} | {label} | {detail}")

def info(label, detail=""):
    LOG.append(("info", label, True, str(detail)))
    print(f"INFO | {label} | {detail}")

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

# ---- column normalisers: each returns a SQL expression ----
def ts(s, c):        # -> naive UTC TIMESTAMP
    if c not in s: return "CAST(NULL AS TIMESTAMP)"
    t = s[c]
    if t.startswith("TIMESTAMP WITH TIME ZONE") or t.startswith("TIMESTAMPTZ"): return f"(\"{c}\" AT TIME ZONE 'UTC')"
    if t.startswith("TIMESTAMP") or t == "DATE":  return f"CAST(\"{c}\" AS TIMESTAMP)"
    if t == "BIGINT":                              return f"make_timestamp(\"{c}\" // 1000)"   # epoch ns -> us
    return f"TRY_CAST(\"{c}\" AS TIMESTAMP)"

def boolean(s, c):
    if c not in s: return "FALSE"
    if s[c] == "BOOLEAN": return f"COALESCE(\"{c}\", FALSE)"
    return f"COALESCE(lower(trim(CAST(\"{c}\" AS VARCHAR))) IN ('true','1','1.0','t','yes'), FALSE)"

def num(s, c):
    if c not in s: return "CAST(NULL AS DOUBLE)"
    x = f"TRY_CAST(\"{c}\" AS DOUBLE)"
    return f"CASE WHEN isnan({x}) THEN NULL ELSE {x} END"

def label(s, c):
    if c not in s: return "'unknown'"
    x = f"CAST(\"{c}\" AS VARCHAR)"
    return f"CASE WHEN {x} IS NULL OR {x} IN ('None','nan','') THEN 'unknown' ELSE {x} END"

def to_local(expr):  # naive UTC timestamp -> naive local timestamp
    return f"(({expr} AT TIME ZONE 'UTC') AT TIME ZONE '{LOCAL_TZ}')"

HOUR_BAND_SQL = "CASE " + " ".join(
    f"WHEN local_hour BETWEEN {lo} AND {hi} THEN '{b}'" for b, (lo, hi) in HOUR_BANDS.items()) + " ELSE 'unknown' END"
WEEKDAY_NAME_SQL = "(['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'])[weekday + 1]"

def hist_cols(prefix, column, cond, edges):
    parts = []
    for i in range(len(edges) + 1):
        if i == 0:                c = f"{column} < {edges[0]}"
        elif i == len(edges):     c = f"{column} >= {edges[-1]}"
        else:                     c = f"{column} >= {edges[i-1]} AND {column} < {edges[i]}"
        parts.append(f"SUM(CASE WHEN ({cond}) AND {c} THEN 1 ELSE 0 END) AS {prefix}{i}")
    return ",\n  ".join(parts)

def bucket_rows(metric, prefix, edges):
    rows = []
    for i in range(len(edges) + 1):
        lo = None if i == 0 else edges[i-1]
        hi = None if i == len(edges) else edges[i]
        lbl = f"< {hi}" if lo is None else (f">= {lo}" if hi is None else f"{lo} to {hi}")
        rows.append((metric, f"{prefix}{i}", lo, hi, lbl))
    return rows

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
    COALESCE(isodow(visit_local) - 1, -1)     AS weekday,       -- Monday = 0
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

n_sv  = scalar("SELECT COUNT(*) FROM sv")
dates = [str(r[0]) for r in con.execute("SELECT DISTINCT service_date FROM sv WHERE service_date IS NOT NULL ORDER BY 1").fetchall()]
print(f"Staged clean_stop_visits: {n_sv:,} rows | service dates: {dates}")

# ---- sanity: does delay_s equal actual_departure - aimed_departure ? ----
r = con.execute("""
  SELECT avg((abs((epoch(actual_departure)-epoch(aimed_departure)) - delay_s) <= 2)::INT),
         avg((abs((epoch(actual_departure)-epoch(aimed_departure)) + delay_s) <= 2)::INT)
  FROM sv WHERE is_actual AND actual_departure IS NOT NULL AND aimed_departure IS NOT NULL AND delay_s IS NOT NULL""").fetchone()
agree, flipped = (r[0] or 0.0), (r[1] or 0.0)
check("delay_s matches actual_departure - aimed_departure", agree >= 0.85, f"agree {agree:.1%}, sign flipped {flipped:.1%}")

# ---- sanity: recomputed Oslo hour vs the local_hour already stored in silver ----
if "local_hour" in s:
    r = con.execute(f"""
      SELECT avg((CAST(c.local_hour AS INTEGER) = v.local_hour)::INT)
      FROM (SELECT {ts(s,'visit_time')} AS vt, local_hour FROM {rel}) c,
           LATERAL (SELECT COALESCE(hour({to_local('c.vt')}), -1) AS local_hour) v""").fetchone()
    info("recomputed local_hour vs silver local_hour", f"agree {r[0]:.1%}" if r[0] is not None else "n/a")

# %%
print("Building dimensions...")
emit("dim_line", """
    SELECT line_ref, ANY_VALUE(route_short_name) AS route_short_name, ANY_VALUE(route_type_name) AS route_type_name
    FROM sv GROUP BY line_ref ORDER BY line_ref""")

emit("dim_stop", """
    SELECT stop_ref, ANY_VALUE(stop_name) AS stop_name, AVG(stop_lat) AS stop_lat, AVG(stop_lon) AS stop_lon
    FROM sv GROUP BY stop_ref ORDER BY stop_ref""")

emit("dim_date", f"""
    SELECT service_date, isodow(service_date) - 1 AS weekday, strftime(service_date, '%A') AS weekday_name,
           isodow(service_date) >= 6 AS is_weekend
    FROM (SELECT DISTINCT service_date FROM sv WHERE service_date IS NOT NULL) ORDER BY service_date""")

emit_rows("dim_hour_band", "local_hour INTEGER, hour_band VARCHAR", sorted(BAND_OF_HOUR.items()))
emit_rows("gold_buckets", "metric VARCHAR, bucket VARCHAR, lo INTEGER, hi INTEGER, label VARCHAR",
          bucket_rows("delay_s", "d", DELAY_EDGES) + bucket_rows("time_lost_s", "tl", TL_EDGES))

# %%
print("Building core cubes and fact table...")
CORE = f"""
  COUNT(*) AS n_calls,
  SUM(is_actual::INT) AS n_actual,
  SUM((is_actual AND delay_outlier)::INT) AS n_outlier,
  SUM(valid::INT) AS n_valid,
  SUM((valid AND delay_s < {EARLY_S})::INT) AS n_early,
  SUM((valid AND delay_s >= {EARLY_S} AND delay_s < {LATE_S})::INT) AS n_on_time,
  SUM((valid AND delay_s >= {LATE_S})::INT) AS n_late,
  COALESCE(SUM(delay_s) FILTER (WHERE valid), 0) AS sum_delay_s"""
MEASURES = CORE + f""",
  COALESCE(SUM(delay_s * delay_s) FILTER (WHERE valid), 0) AS sum_delay_sq,
  {hist_cols('d', 'delay_s', 'valid', DELAY_EDGES)}"""

emit("gold_visits_cube", f"""
    SELECT service_date, local_hour, hour_band, weekday, line_ref, direction_ref, {MEASURES}
    FROM sv GROUP BY ALL""")

emit("gold_stop_cube", f"""
    SELECT service_date, hour_band, line_ref, stop_ref, {MEASURES}
    FROM sv GROUP BY ALL""")

emit("gold_stop_order_delay", f"""
    SELECT service_date, hour_band, line_ref, direction_ref, stop_order,
      COUNT(*) AS n_calls, SUM(valid::INT) AS n_valid,
      SUM((valid AND delay_s < {EARLY_S})::INT) AS n_early,
      SUM((valid AND delay_s >= {LATE_S})::INT) AS n_late,
      COALESCE(SUM(delay_s) FILTER (WHERE valid), 0) AS sum_delay_s,
      COALESCE(SUM(delay_s * delay_s) FILTER (WHERE valid), 0) AS sum_delay_sq
    FROM sv GROUP BY ALL""")

emit("fact_stop_visits", """
    SELECT service_date, local_hour, local_quarter, local_time_bin, hour_band, weekday, journey_ref, line_ref,
           route_short_name, direction_ref, stop_order, stop_ref, stop_name, stop_lat, stop_lon,
           aimed_arrival, actual_arrival, aimed_departure, actual_departure, visit_time,
           delay_s, delay_arrival_s, delay_departure_s, is_actual, delay_outlier, valid
    FROM sv""")

# %%
print("Building heatmap-ready tables...")
# One row per heatmap cell. Counts/sums are additive; avg/pct/percentiles are precomputed for plotting.
# n_valid is kept so a heatmap can mask cells with too few observations.
RATIOS = """
  sum_delay_s / NULLIF(n_valid, 0)        AS avg_delay_s,
  n_on_time::DOUBLE / NULLIF(n_valid, 0)  AS pct_on_time,
  n_early::DOUBLE   / NULLIF(n_valid, 0)  AS pct_early,
  n_late::DOUBLE    / NULLIF(n_valid, 0)  AS pct_late"""
PCTS = """,
  quantile_cont(delay_s, 0.5) FILTER (WHERE valid) AS median_delay_s,
  quantile_cont(delay_s, 0.9) FILTER (WHERE valid) AS p90_delay_s"""

def heat(name, dims, extra=""):
    # extra = additional select expressions (functionally dependent on dims, or aggregates)
    emit(name, f"""
      SELECT *, {RATIOS} FROM (
        SELECT {dims}{extra}, {CORE}{PCTS} FROM sv GROUP BY ALL)
      ORDER BY {dims}""")

# hour x route   (and 15-minute bins)
heat("gold_heat_route_hour",    "line_ref, route_short_name, local_hour")
heat("gold_heat_route_quarter", "line_ref, route_short_name, local_quarter, local_time_bin")
# weekday x hour  (rows = weekday, columns = hour)
heat("gold_heat_weekday_hour",  "weekday, local_hour", f", {WEEKDAY_NAME_SQL} AS weekday_name, COUNT(DISTINCT service_date) AS n_service_dates")
# stop x hour, and route -> stop x hour (station-hour view for one route)
heat("gold_heat_stop_hour",       "stop_ref, stop_name, local_hour")
heat("gold_heat_route_stop_hour", "line_ref, route_short_name, stop_ref, stop_name, local_hour")

# spatial: one row per stop with coordinates + aggregated metrics
ev_rel = read("per_stop_evidence", required=False)
ev_join, ev_cols = "", ""
if ev_rel:
    ev_join = f"""LEFT JOIN (SELECT CAST(stop_id AS VARCHAR) AS stop_ref, SUM(calls) AS evidence_calls,
                       SUM(stopped) AS evidence_stopped, SUM(passed) AS evidence_passed,
                       ANY_VALUE(passed_share) AS passed_share, ANY_VALUE(conclusive) AS conclusive
                FROM {ev_rel} GROUP BY 1) e ON agg.stop_ref = e.stop_ref"""
    ev_cols = ", e.evidence_calls, e.evidence_stopped, e.evidence_passed, e.passed_share, e.conclusive"
    m = scalar(f"SELECT COUNT(DISTINCT stop_ref) FROM sv WHERE stop_ref IN (SELECT CAST(stop_id AS VARCHAR) FROM {ev_rel})")
    info("per_stop_evidence.stop_id matches clean_stop_visits.stop_ref",
         f"{m:,} of {scalar('SELECT COUNT(DISTINCT stop_ref) FROM sv'):,} visited stops matched")
    dup = scalar(f"SELECT COUNT(*) - COUNT(DISTINCT stop_id) FROM {ev_rel}")
    info("per_stop_evidence duplicate stop_id rows", dup)
emit("gold_stop_map", f"""
    SELECT agg.*,
           agg.sum_delay_s / NULLIF(agg.n_valid, 0)        AS avg_delay_s,
           agg.n_on_time::DOUBLE / NULLIF(agg.n_valid, 0)  AS pct_on_time,
           agg.n_early::DOUBLE   / NULLIF(agg.n_valid, 0)  AS pct_early,
           agg.n_late::DOUBLE    / NULLIF(agg.n_valid, 0)  AS pct_late
           {ev_cols}
    FROM (SELECT stop_ref, ANY_VALUE(stop_name) AS stop_name, AVG(stop_lat) AS stop_lat, AVG(stop_lon) AS stop_lon,
                 COUNT(DISTINCT line_ref) AS n_lines, {CORE}{PCTS}
          FROM sv GROUP BY stop_ref) agg
    {ev_join}""")

# %%
print("Building segment runtime and headway analytics...")
emit("gold_segment_runtime", f"""
    WITH v AS (SELECT * FROM sv WHERE valid),
    p AS (SELECT *, LAG(stop_ref) OVER w AS from_stop_ref, LAG(stop_order) OVER w AS prev_order,
                 LAG(aimed_departure) OVER w AS prev_aimed, LAG(actual_departure) OVER w AS prev_obs
          FROM v WINDOW w AS (PARTITION BY service_date, journey_ref ORDER BY stop_order)),
    s AS (SELECT *, epoch(actual_departure) - epoch(prev_obs)  AS act_s,
                    epoch(aimed_departure)  - epoch(prev_aimed) AS aim_s
          FROM p WHERE prev_order = stop_order - 1)
    SELECT service_date, hour_band, line_ref, from_stop_ref, stop_ref AS to_stop_ref,
           COUNT(*) AS n, SUM(act_s) AS sum_actual_s, SUM(aim_s) AS sum_aimed_s, SUM(act_s * act_s) AS sum_actual_sq
    FROM s WHERE act_s BETWEEN 0 AND {SEG_MAX_S} AND aim_s > 0
    GROUP BY ALL""")

emit("gold_headway", f"""
    WITH v AS (SELECT * FROM sv WHERE valid),
    p AS (SELECT *, LAG(aimed_departure) OVER w AS pa, LAG(actual_departure) OVER w AS po
          FROM v WINDOW w AS (PARTITION BY service_date, line_ref, direction_ref, stop_ref ORDER BY aimed_departure)),
    h AS (SELECT service_date, local_hour, hour_band, line_ref, direction_ref, stop_ref,
                 epoch(aimed_departure) - epoch(pa) AS aim_h, epoch(actual_departure) - epoch(po) AS act_h
          FROM p WHERE pa IS NOT NULL)
    SELECT service_date, local_hour, hour_band, line_ref, direction_ref, stop_ref,
           COUNT(*) AS n_pairs, SUM(act_h) AS sum_actual_headway_s, SUM(aim_h) AS sum_aimed_headway_s,
           SUM((act_h < {BUNCH_RATIO} * aim_h)::INT) AS n_bunched,
           SUM((act_h > {GAP_RATIO} * aim_h)::INT)   AS n_large_gap,
           SUM(act_h * act_h) AS sum_actual_headway_sq, SUM(aim_h * aim_h) AS sum_aimed_headway_sq   -- for excess wait time
    FROM h WHERE aim_h BETWEEN {HEADWAY_MIN_S} AND {HEADWAY_MAX_S}
    GROUP BY ALL""")

# %% [markdown]
# ### Dashboard KPI tables
# 
# - `gold_segment_hour`: one row per road segment (consecutive stops of a journey), service date, local hour, line and direction.
#   Additive sums let the dashboard filter by any combination and then divide: average run time = `sum_actual_s / n`,
#   time lost against the timetable = `(sum_actual_s - sum_aimed_s) / n`. With segment lengths from GTFS (`dashboard/build_map.py`)
#   it gives the speed on every piece of road. Kolumbus reports one time per stop (actual arrival = actual departure), so a
#   segment's run time is driving plus dwell. Legs with 0 s run time (two stops reported in the same second) are left out,
#   because they would mean infinite speed. `local_hour` is the hour the bus reached the second stop.
#   Legs with a **scheduled** time of 0 s are kept: the timetable has one-minute resolution, so consecutive stops often share
#   the same scheduled minute (about a fifth of all legs). Dropping them (as `gold_segment_runtime` does with `aim_s > 0`)
#   removes real driving time and makes buses look faster than the timetable. Compare actual and scheduled time on sums
#   over many legs, not on a single leg.
# - `gold_trip_summary`: one row per observed trip. Delay at the first and last observed stop, the largest delay, delay
#   gained along the trip, and actual vs scheduled run time. This gives *terminus punctuality* and *delay growth* per route.
# - `gold_headway` also stores the sums of squared headways, so the dashboard can compute **excess waiting time**:
#   `EWT = sum(act²)/(2·sum(act)) − sum(aim²)/(2·sum(aim))` (seconds a random passenger waits longer than the timetable promises).

# %%
print("Building dashboard KPI tables...")
emit("gold_segment_hour", f"""
    WITH v AS (SELECT * FROM sv WHERE valid),
    p AS (SELECT *, LAG(stop_ref) OVER w AS from_stop_ref, LAG(stop_order) OVER w AS prev_order,
                 LAG(aimed_departure) OVER w AS prev_aimed, LAG(actual_departure) OVER w AS prev_obs
          FROM v WINDOW w AS (PARTITION BY service_date, journey_ref ORDER BY stop_order)),
    s AS (SELECT *, epoch(actual_departure) - epoch(prev_obs)  AS act_s,
                    epoch(aimed_departure)  - epoch(prev_aimed) AS aim_s
          FROM p WHERE prev_order = stop_order - 1)
    SELECT service_date, weekday, local_hour, hour_band, line_ref, route_short_name, direction_ref,
           from_stop_ref, stop_ref AS to_stop_ref,
           COUNT(*) AS n, SUM(act_s) AS sum_actual_s, SUM(aim_s) AS sum_aimed_s, SUM(act_s * act_s) AS sum_actual_sq
    FROM s WHERE act_s > 0 AND act_s <= {SEG_MAX_S} AND aim_s >= 0
    GROUP BY ALL""")
n_zero = scalar(f"""
    WITH v AS (SELECT * FROM sv WHERE valid),
    p AS (SELECT stop_order, actual_departure, LAG(stop_order) OVER w AS prev_order, LAG(actual_departure) OVER w AS prev_obs
          FROM v WINDOW w AS (PARTITION BY service_date, journey_ref ORDER BY stop_order))
    SELECT COUNT(*) FROM p WHERE prev_order = stop_order - 1 AND epoch(actual_departure) - epoch(prev_obs) = 0""")
info("segment legs with 0 s run time (left out of gold_segment_hour)", n_zero)

emit("gold_trip_summary", """
    SELECT service_date, journey_ref,
           ANY_VALUE(line_ref) AS line_ref, ANY_VALUE(route_short_name) AS route_short_name, ANY_VALUE(direction_ref) AS direction_ref,
           arg_min(local_hour, stop_order) AS start_hour, arg_min(weekday, stop_order) AS weekday,
           COUNT(*) AS n_stops, MIN(stop_order) AS first_stop_order, MAX(stop_order) AS last_stop_order,
           arg_min(stop_ref, stop_order) AS first_stop_ref, arg_max(stop_ref, stop_order) AS last_stop_ref,
           arg_min(delay_s, stop_order) AS start_delay_s, arg_max(delay_s, stop_order) AS end_delay_s,
           MAX(delay_s) AS max_delay_s,
           arg_max(delay_s, stop_order) - arg_min(delay_s, stop_order) AS delay_gained_s,
           epoch(arg_max(actual_departure, stop_order)) - epoch(arg_min(actual_departure, stop_order)) AS runtime_actual_s,
           epoch(arg_max(aimed_departure, stop_order))  - epoch(arg_min(aimed_departure, stop_order))  AS runtime_aimed_s
    FROM sv WHERE valid
    GROUP BY service_date, journey_ref""")


# %%
print("Building fleet, GPS evidence, coverage and event-status tables...")

# 1. Fleet activity (trip_vehicle)
tv = read("trip_vehicle", required=False)
if tv:
    emit("gold_fleet_activity", f"""
        SELECT TRY_CAST(service_date AS DATE) AS service_date, COUNT(*) AS n_trips,
               COUNT(DISTINCT vehicle_id) AS n_vehicles, SUM(n_stops) AS sum_stops, AVG(median_m) AS avg_median_m
        FROM {tv} GROUP BY 1 ORDER BY 1""")

# 2. GPS evidence per stop/class (stop_gps_status)
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
    m = scalar("SELECT COUNT(DISTINCT stop_ref) FROM gold_stop_evidence WHERE stop_ref IN (SELECT stop_ref FROM dim_stop)")
    info("stop_gps_status.stop_id matches clean_stop_visits.stop_ref",
         f"{m:,} of {scalar('SELECT COUNT(DISTINCT stop_ref) FROM gold_stop_evidence'):,} GPS stops matched")

# 3. Time-lost reference (time_lost_null)
nul = read("time_lost_null", required=False)
if nul:
    x = num(schema(nul), "time_lost_s")
    bucket = " + ".join(f"CASE WHEN x >= {e} THEN 1 ELSE 0 END" for e in TL_EDGES)
    counts = dict(con.execute(f"SELECT b, COUNT(*) FROM (SELECT {bucket} AS b FROM (SELECT {x} AS x FROM {nul}) WHERE x IS NOT NULL) GROUP BY b").fetchall())
    emit_rows("gold_time_lost_reference", "bucket VARCHAR, n_stop_free_intervals BIGINT",
              [(f"tl{i}", int(counts.get(i, 0))) for i in range(len(TL_EDGES) + 1)])

# 4. Observed vs expected visits per stop/day (expected_per_stop) - only if keys line up
exp = read("expected_per_stop", required=False)
if exp:
    m = scalar(f"SELECT COUNT(DISTINCT stop_ref) FROM sv WHERE stop_ref IN (SELECT CAST(stop_id AS VARCHAR) FROM {exp})")
    info("expected_per_stop.stop_id matches clean_stop_visits.stop_ref", f"{m:,} visited stops matched")
    if m > 0:
        emit("gold_stop_coverage", f"""
            SELECT e.service_date, e.stop_ref, e.expected_visits,
                   COALESCE(o.n_calls, 0) AS n_observed_calls,
                   COALESCE(o.n_calls, 0)::DOUBLE / NULLIF(e.expected_visits, 0) AS observed_to_expected
            FROM (SELECT TRY_CAST(service_date AS DATE) AS service_date, CAST(stop_id AS VARCHAR) AS stop_ref,
                         SUM(expected_visits) AS expected_visits FROM {exp} GROUP BY 1, 2) e
            LEFT JOIN (SELECT service_date, stop_ref, COUNT(*) AS n_calls FROM sv GROUP BY 1, 2) o
                   USING (service_date, stop_ref)""")
    else:
        print("  gold_stop_coverage skipped: no stop_id overlap with clean_stop_visits.stop_ref")

# 5. stop_event_status: counts per status values exactly as they appear in silver (no assumed meanings)
ses = read("stop_event_status", required=False)
if ses:
    q = schema(ses)
    emit("gold_event_status", f"""
        SELECT TRY_CAST(service_date AS DATE) AS service_date, {label(q,'route_short_name')} AS route_short_name,
               CAST(stop_id AS VARCHAR) AS stop_ref, {label(q,'visit_status')} AS visit_status,
               {label(q,'final_status')} AS final_status, COUNT(*) AS n_events
        FROM {ses} GROUP BY ALL""")
    vals = con.execute("SELECT visit_status, final_status, SUM(n_events) FROM gold_event_status GROUP BY ALL ORDER BY 3 DESC").fetchall()
    info("stop_event_status values (visit_status, final_status, n)", vals)

# %%
print("Running reconciliation audit...")
n_buckets = len(DELAY_EDGES) + 1
check("visits cube holds every call", scalar("SELECT SUM(n_calls) FROM gold_visits_cube") == n_sv)
check("stop cube holds every call",   scalar("SELECT SUM(n_calls) FROM gold_stop_cube") == n_sv)
check("fact table holds every call",  scalar("SELECT COUNT(*) FROM fact_stop_visits") == n_sv)
check("early + on_time + late = valid", scalar("SELECT SUM(n_early + n_on_time + n_late - n_valid) FROM gold_visits_cube") == 0)
check("histogram buckets add up to valid",
      scalar(f"SELECT SUM({' + '.join(f'd{i}' for i in range(n_buckets))} - n_valid) FROM gold_visits_cube") == 0)
check("stop cube valid = visits cube valid",
      scalar("SELECT SUM(n_valid) FROM gold_stop_cube") == scalar("SELECT SUM(n_valid) FROM gold_visits_cube"))
cube_sum = scalar("SELECT SUM(sum_delay_s) FROM gold_visits_cube") or 0.0
row_sum  = scalar("SELECT SUM(delay_s) FROM sv WHERE valid") or 0.0
check("sum of delays matches row-level table", abs(cube_sum - row_sum) <= 1e-4, f"cube {cube_sum:,.1f}s | rows {row_sum:,.1f}s")

for t in ["gold_heat_route_hour", "gold_heat_route_quarter", "gold_heat_weekday_hour",
          "gold_heat_stop_hour", "gold_heat_route_stop_hour", "gold_stop_map"]:
    check(f"{t} holds every call", scalar(f"SELECT SUM(n_calls) FROM {t}") == n_sv)
    check(f"{t} valid = visits cube valid",
          scalar(f"SELECT SUM(n_valid) FROM {t}") == scalar("SELECT SUM(n_valid) FROM gold_visits_cube"))

check("gold_trip_summary has one row per observed trip",
      scalar("SELECT COUNT(*) FROM gold_trip_summary") == scalar("SELECT COUNT(DISTINCT (service_date, journey_ref)) FROM sv WHERE valid"))
check("gold_trip_summary stop counts add up to valid calls",
      scalar("SELECT SUM(n_stops) FROM gold_trip_summary") == scalar("SELECT SUM(n_valid) FROM gold_visits_cube"))
seg_legs = scalar(f"""
    WITH v AS (SELECT * FROM sv WHERE valid),
    p AS (SELECT stop_order, actual_departure, aimed_departure, LAG(stop_order) OVER w AS prev_order,
                 LAG(actual_departure) OVER w AS prev_obs, LAG(aimed_departure) OVER w AS prev_aimed
          FROM v WINDOW w AS (PARTITION BY service_date, journey_ref ORDER BY stop_order)),
    s AS (SELECT epoch(actual_departure) - epoch(prev_obs) AS act_s, epoch(aimed_departure) - epoch(prev_aimed) AS aim_s
          FROM p WHERE prev_order = stop_order - 1)
    SELECT COUNT(*) FROM s WHERE act_s > 0 AND act_s <= {SEG_MAX_S} AND aim_s >= 0""")
check("gold_segment_hour holds every usable segment leg", scalar("SELECT SUM(n) FROM gold_segment_hour") == seg_legs, f"{seg_legs:,}")
info("calls with unknown local_hour (bad/missing visit_time)", scalar("SELECT COUNT(*) FROM sv WHERE local_hour = -1"))
info("stops without coordinates in gold_stop_map", scalar("SELECT COUNT(*) FROM gold_stop_map WHERE stop_lat IS NULL OR stop_lon IS NULL"))

# %%
print("Publishing audit log, metadata and headline metrics...")
emit_rows("gold_data_quality", "kind VARCHAR, label VARCHAR, ok BOOLEAN, detail VARCHAR", LOG)

h = con.execute("SELECT SUM(n_valid), SUM(n_early), SUM(n_on_time), SUM(n_late), SUM(sum_delay_s) FROM gold_visits_cube").fetchone()
if h and h[0]:
    print("HEADLINE PERFORMANCE")
    print(f"  Valid calls : {int(h[0]):,}")
    print(f"  Early       : {h[1] / h[0]:.1%}")
    print(f"  On time     : {h[2] / h[0]:.1%}")
    print(f"  Late        : {h[3] / h[0]:.1%}")
    print(f"  Mean delay  : {h[4] / h[0]:.1f} s")

n_pass = sum(1 for k, _, ok, _ in LOG if k == "check" and ok)
n_fail = sum(1 for k, _, ok, _ in LOG if k == "check" and not ok)
meta = {"built_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(), "engine": f"duckdb {duckdb.__version__}",
        "service_dates": ",".join(dates), "timezone": LOCAL_TZ,
        "on_time_rule": f"{EARLY_S} <= delay_s < {LATE_S}", "valid_rule": "is_actual AND NOT delay_outlier AND delay_s IS NOT NULL",
        "checks_passed": str(n_pass), "checks_failed": str(n_fail)}
meta.update({f"rows_{k}": str(v) for k, v in TABLES.items()})
emit_rows("gold_meta", "key VARCHAR, value VARCHAR", list(meta.items()))
con.close()
print(f"\ngold.duckdb written with {len(TABLES)} tables -> {db_path.resolve()}")

# %%
failed = [l for k, l, ok, _ in LOG if k == "check" and not ok]
print(f"SUCCESS: all {n_pass} reconciliation checks passed. Gold layer ready at {GOLD_DIR.resolve()}" if not failed
      else f"WARNING: {len(failed)} check(s) failed: {failed}")
assert not failed, f"Failed checks: {failed}"


