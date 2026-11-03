"""Build silver Parquet tables from the local bronze tables.

Reads   tables/parquet_2026-10-24/<table>.parquet
Writes  tables/parquet_2026-10-24/silver/<table>.parquet

Same rules as the silver notebook, but without ferries: final state of each stop visit, UTC times,
delay in seconds, flags, local hour/weekday, line numbers and stop coordinates
from GTFS. Existing output files are overwritten.
"""
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

INPUT_DIR = Path(__file__).resolve().parent / "parquet_2026-10-24"
OUTPUT_DIR = INPUT_DIR / "silver"
TZ = "Europe/Oslo"              # Norway's time zone (also Stavanger)
STALE_S = 300                   # vehicle position older than 5 min = stale
MAX_ABS_DELAY_S = 3 * 3600      # delays over 3 h are treated as errors
ON_TIME_EARLY_S, ON_TIME_LATE_S = -60, 180   # on time = 1 min early to 3 min late


def to_utc(series):
    return pd.to_datetime(series, utc=True, format="ISO8601")


def flag(series):
    return series.fillna(False).astype(bool)


def build_dims(routes, stops):
    dim_routes = routes.rename(columns={"route_id": "line_ref", "route_short_name": "public_code",
                                        "route_long_name": "line_name"})[
        ["line_ref", "public_code", "line_name", "route_type"]]
    dim_stops = stops.rename(columns={"stop_id": "stop_ref", "stop_name": "gtfs_stop_name"})[
        ["stop_ref", "gtfs_stop_name", "stop_lat", "stop_lon"]].copy()
    dim_stops["stop_lat"] = pd.to_numeric(dim_stops["stop_lat"], errors="coerce")
    dim_stops["stop_lon"] = pd.to_numeric(dim_stops["stop_lon"], errors="coerce")
    return dim_routes, dim_stops


def ferry_lines(stop_visits, dim_routes):
    """Lines that are ferries: by vehicle_mode in the real-time data, or by GTFS route_type
    (4 = ferry, 1000-1099 = water transport, 1200-1299 = ferry)."""
    by_mode = set(stop_visits.loc[stop_visits["vehicle_mode"].eq("ferry"), "line_ref"].dropna())
    route_type = pd.to_numeric(dim_routes["route_type"], errors="coerce")
    is_ferry = route_type.eq(4) | route_type.between(1000, 1099) | route_type.between(1200, 1299)
    return by_mode | set(dim_routes.loc[is_ferry, "line_ref"])


def latest_stop_visits(path, batch_rows=250_000):
    """Final state of each stop visit, read in batches so a full day fits in memory."""
    key = ["data_frame_ref", "journey_ref", "stop_order"]
    latest = None
    for batch in pq.ParquetFile(path).iter_batches(batch_size=batch_rows):
        df = batch.to_pandas()
        latest = df if latest is None else pd.concat([latest, df], ignore_index=True)
        latest = (latest.sort_values("snapshot_time", kind="stable")
                  .drop_duplicates(key, keep="last"))
    return latest


def silver_stop_visits(sv, dim_routes, dim_stops):
    sv = sv.copy()
    for c in ["aimed_arrival", "expected_arrival", "actual_arrival",
              "aimed_departure", "expected_departure", "actual_departure"]:
        sv[c] = to_utc(sv[c])

    # delay = observed - planned (departure if possible, otherwise arrival)
    sv["is_actual"] = sv["call_type"].eq("recorded")
    obs_dep = sv["actual_departure"].where(sv["is_actual"], sv["expected_departure"])
    obs_arr = sv["actual_arrival"].where(sv["is_actual"], sv["expected_arrival"])
    sv["aimed_time"] = sv["aimed_departure"].fillna(sv["aimed_arrival"])
    sv["observed_time"] = obs_dep.fillna(obs_arr)
    dep_delay = (obs_dep - sv["aimed_departure"]).dt.total_seconds()
    arr_delay = (obs_arr - sv["aimed_arrival"]).dt.total_seconds()
    sv["delay_s"] = dep_delay.fillna(arr_delay)

    # flags
    sv["cancelled"] = flag(sv["call_cancelled"]) | flag(sv["journey_cancelled"])
    sv["no_prediction"] = ~sv["is_actual"] & sv["delay_s"].eq(0)
    sv["delay_outlier"] = sv["delay_s"].abs() > MAX_ABS_DELAY_S
    sv["on_time"] = sv["delay_s"].between(ON_TIME_EARLY_S, ON_TIME_LATE_S)

    # local time for analysis (planned time, Norway)
    local = sv["aimed_time"].dt.tz_convert(TZ)
    sv["service_date"] = local.dt.date.astype("string")
    sv["hour"] = local.dt.hour
    sv["weekday"] = local.dt.day_name()

    # line numbers and stop coordinates from GTFS
    sv = sv.merge(dim_routes, on="line_ref", how="left").merge(dim_stops, on="stop_ref", how="left")
    sv["stop_name"] = sv["stop_name"].fillna(sv["gtfs_stop_name"])

    cols = ["data_frame_ref", "journey_ref", "line_ref", "public_code", "line_name", "vehicle_mode",
            "direction_ref", "operator_ref", "stop_order", "stop_ref", "stop_name", "stop_lat", "stop_lon",
            "destination", "aimed_time", "observed_time", "delay_s", "is_actual", "on_time",
            "cancelled", "no_prediction", "delay_outlier", "service_date", "hour", "weekday",
            "aimed_arrival", "aimed_departure", "actual_arrival", "actual_departure",
            "expected_arrival", "expected_departure", "snapshot_time", "source_file"]
    return sv[cols].sort_values(["journey_ref", "stop_order"]).reset_index(drop=True)


def silver_vehicle_positions(vehicles, line_modes):
    vp = (vehicles.sort_values("snapshot_time", kind="stable")
          .drop_duplicates(["vehicle_id", "last_updated"], keep="first")   # same position seen twice
          .copy())
    vp["last_updated"] = to_utc(vp["last_updated"])
    vp["age_s"] = (vp["snapshot_time"] - vp["last_updated"]).dt.total_seconds()
    vp["is_stale"] = vp["age_s"] > STALE_S
    vp["bearing"] = vp["bearing"] % 360
    vp = vp.merge(line_modes, on="line_ref", how="left")
    vp = vp.dropna(subset=["latitude", "longitude"])
    local = vp["last_updated"].dt.tz_convert(TZ)
    vp["hour"] = local.dt.hour
    vp["weekday"] = local.dt.day_name()
    return vp.drop(columns=["speed"]).reset_index(drop=True)          # speed is always empty


CATEGORIES = [("trafikk", "traffic"), ("forrige rute", "knock-on"), ("teknisk", "technical"),
              ("vær", "weather"), ("korresponder", "waiting for connection"), ("innstil", "cancelled")]


def categorise(summary):
    text = (summary or "").lower()
    for word, category in CATEGORIES:
        if word in text:
            return category
    return "other"


def silver_alerts(alerts):
    keys = ["situation_number", "version_time", "affect_type", "line_ref", "journey_ref"]
    seen = (alerts.groupby(keys, dropna=False)["snapshot_time"]
            .agg(first_seen="min", last_seen="max").reset_index())
    al = (alerts.sort_values("snapshot_time", kind="stable").drop_duplicates(keys, keep="last")
          .drop(columns=["snapshot_time"]).merge(seen, on=keys, how="left"))
    for c in ["creation_time", "version_time", "valid_start", "valid_end", "origin_aimed_departure"]:
        al[c] = to_utc(al[c])
    al["category"] = al["summary"].map(categorise)
    return al.reset_index(drop=True)


# --------------------------------------------------------------- Local I/O
def fetch(table, input_dir):
    """Return a local Parquet table path, or None if the table is optional and absent."""
    path = input_dir / f"{table}.parquet"
    return path if path.is_file() else None


def fetch_gtfs(table, input_dir):
    """Read a required GTFS table from the local Parquet directory."""
    path = fetch(table, input_dir)
    if path is None:
        raise FileNotFoundError(f"required GTFS table not found: {input_dir / f'{table}.parquet'}")
    return pd.read_parquet(path)


def save(df, table, output_dir):
    path = output_dir / f"{table}.parquet"
    df.to_parquet(path, index=False)
    print(f"  {table}: {len(df):,} rows, {path.stat().st_size / 1e6:.1f} MB -> {path}")


# ------------------------------------------------------------------- main
def build_day(input_dir, output_dir):
    print(f"Building silver tables  ({input_dir} -> {output_dir})")
    output_dir.mkdir(parents=True, exist_ok=True)
    dim_routes, dim_stops = build_dims(fetch_gtfs("gtfs_routes", input_dir),
                                       fetch_gtfs("gtfs_stops", input_dir))

    sv_path = fetch("stop_visits", input_dir)
    if sv_path is None:
        raise FileNotFoundError(f"required table not found: {input_dir / 'stop_visits.parquet'}")
    latest = latest_stop_visits(sv_path)

    # ferries are left out of every silver table
    ferries = ferry_lines(latest, dim_routes)
    print(f"  ferries: {len(ferries)} lines left out")
    latest = latest[~latest["line_ref"].isin(ferries)]

    sv = silver_stop_visits(latest, dim_routes, dim_stops)
    save(sv, "stop_visits", output_dir)

    vp_path = fetch("vehicle_positions", input_dir)
    if vp_path:
        vehicles = pd.read_parquet(vp_path)
        vehicles = vehicles[~vehicles["line_ref"].isin(ferries)]
        line_modes = sv.groupby("line_ref")["vehicle_mode"].first().reset_index()
        save(silver_vehicle_positions(vehicles, line_modes), "vehicle_positions", output_dir)
    else:
        print("  vehicle_positions: no bronze table, skipped")

    al_path = fetch("service_alerts", input_dir)
    if al_path:
        alerts = pd.read_parquet(al_path)
        alerts = alerts[~alerts["line_ref"].isin(ferries)]      # alerts without a line are kept
        save(silver_alerts(alerts), "alerts", output_dir)
    else:
        print("  alerts: no bronze table, skipped")

    save(dim_routes[~dim_routes["line_ref"].isin(ferries)], "routes", output_dir)
    save(dim_stops, "stops", output_dir)


def main():
    build_day(INPUT_DIR, OUTPUT_DIR)
    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"FAILED: {error}", file=sys.stderr)
        raise
