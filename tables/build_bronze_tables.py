"""Bronze tables: turn one day of raw files in Swift into Parquet tables.

Reads   buspulse-raw/<feed>/date=YYYY-MM-DD/*.{json,xml,zip}[.gz]
Writes  buspulse-bronze/<table>/date=YYYY-MM-DD/part-0.parquet

Values are copied as they are (timestamps stay as the original text);
cleaning happens later, in silver. Every row gets snapshot_time (from the
file name) and source_file, so it can be traced back to its raw file.
Running it again for the same day overwrites that day, so reruns are safe.

Usage (OpenStack login loaded into the environment first):
    python tables/build_bronze_tables.py                          # yesterday (UTC), all feeds
    python tables/build_bronze_tables.py --date 2026-10-30
    python tables/build_bronze_tables.py --date 2026-10-30 --feeds vehicle_positions --max-files 10
"""
import argparse
import gzip
import io
import json
import os
import re
import tempfile
import zipfile
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import xml.etree.ElementTree as ET

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from swiftclient.client import Connection

RAW_CONTAINER = os.getenv("RAW_CONTAINER", "buspulse-raw")
BRONZE_CONTAINER = os.getenv("BRONZE_CONTAINER", "buspulse-bronze")
BATCH_ROWS = 100_000
# parallel downloads; lower if Swift returns 400 errors
WORKERS = int(os.getenv("WORKERS", "4"))
FEEDS = ["vehicle_positions", "service_alerts",
         "estimated_timetable", "static_timetable"]

UTC_TS = pa.timestamp("s", tz="UTC")
S, F, I, B = pa.string(), pa.float64(), pa.int32(), pa.bool_()


def schema(*fields):
    return pa.schema([pa.field("snapshot_time", UTC_TS), *[pa.field(n, t) for n, t in fields],
                      pa.field("source_file", S)])


SCHEMAS = {
    "vehicle_positions": schema(
        ("vehicle_id", S), ("last_updated", S), ("line_ref", S), ("line_name", S),
        ("public_code", S), ("latitude", F), ("longitude", F), ("bearing", F),
        ("speed", F), ("delay", F), ("occupancy_status", S)),
    "service_alerts": schema(
        ("situation_number", S), ("creation_time",
                                  S), ("version_time", S), ("progress", S),
        ("severity", S), ("report_type", S), ("planned", S), ("summary", S),
        ("valid_start", S), ("valid_end", S), ("affect_type", S), ("line_ref", S),
        ("journey_ref", S), ("operator_ref", S), ("origin_aimed_departure", S)),
    "stop_visits": schema(
        ("recorded_at", S), ("journey_ref",
                             S), ("data_frame_ref", S), ("line_ref", S),
        ("direction_ref", S), ("vehicle_mode",
                               S), ("operator_ref", S), ("monitored", B),
        ("journey_cancelled", B), ("call_type",
                                   S), ("stop_order", I), ("stop_ref", S),
        ("stop_name", S), ("destination", S),
        ("aimed_arrival", S), ("expected_arrival", S), ("actual_arrival", S),
        ("aimed_departure", S), ("expected_departure", S), ("actual_departure", S),
        ("arrival_status", S), ("departure_status", S), ("call_cancelled", B),
        ("prediction_inaccurate", B)),
}


# --------------------------------------------------------------------- helpers
def swift_connection():
    """Same OS_* variables as the uploader (~/openstack.env)."""
    env = os.environ
    return Connection(
        authurl=env["OS_AUTH_URL"], user=env.get("OS_USERNAME"), key=env.get("OS_PASSWORD"),
        auth_version="3",
        os_options={k: v for k, v in {
            "project_id": env.get("OS_PROJECT_ID"),
            "project_name": env.get("OS_PROJECT_NAME"),
            "user_domain_name": env.get("OS_USER_DOMAIN_NAME"),
            "user_domain_id": env.get("OS_USER_DOMAIN_ID"),
            "project_domain_name": env.get("OS_PROJECT_DOMAIN_NAME"),
            "project_domain_id": env.get("OS_PROJECT_DOMAIN_ID"),
            "region_name": env.get("OS_REGION_NAME"),
            "interface": env.get("OS_INTERFACE"),
        }.items() if v},
    )


def snapshot_time(name):
    match = re.search(r"_(\d{8}T\d{6})Z", name)
    return datetime.strptime(match.group(1), "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc) if match else None


def local(tag):
    return tag.rsplit("}", 1)[-1]


def text(element, name):
    for child in element:
        if local(child.tag) == name:
            return (child.text or "").strip() or None
    return None


def first_text(element, name):
    """First matching element anywhere below `element`."""
    for child in element.iter():
        if local(child.tag) == name:
            return (child.text or "").strip() or None
    return None


def flag(value):
    return None if value is None else value == "true"


def num(value):
    return None if value is None else float(value)


class TableWriter:
    """Collects rows and writes them to one Parquet file in batches."""

    def __init__(self, path, schema):
        self.path, self.schema, self.rows, self.count = path, schema, [], 0
        self.writer = pq.ParquetWriter(path, schema, compression="zstd")

    def add(self, row):
        self.rows.append(row)
        if len(self.rows) >= BATCH_ROWS:
            self.flush()

    def flush(self):
        if self.rows:
            self.writer.write_table(pa.Table.from_pylist(
                self.rows, schema=self.schema))
            self.count += len(self.rows)
            self.rows = []

    def close(self):
        self.flush()
        self.writer.close()


# ------------------------------------------------------------------- parsers
# Each parser gets the raw bytes of one file and yields table rows (dicts).

def parse_vehicle_positions(payload, base):
    for v in json.loads(payload).get("data", {}).get("vehicles") or []:
        line, loc = v.get("line") or {}, v.get("location") or {}
        yield {**base,
               "vehicle_id": v.get("vehicleId"), "last_updated": v.get("lastUpdated"),
               "line_ref": line.get("lineRef"), "line_name": line.get("lineName"),
               "public_code": line.get("publicCode"),
               "latitude": num(loc.get("latitude")), "longitude": num(loc.get("longitude")),
               "bearing": num(v.get("bearing")), "speed": num(v.get("speed")),
               "delay": num(v.get("delay")), "occupancy_status": v.get("occupancyStatus")}


def parse_service_alerts(payload, base):
    for _, elem in ET.iterparse(io.BytesIO(payload)):
        if local(elem.tag) != "PtSituationElement":
            continue
        alert = {**base,
                 "situation_number": text(elem, "SituationNumber"),
                 "creation_time": text(elem, "CreationTime"),
                 "version_time": text(elem, "VersionedAtTime"),
                 "progress": text(elem, "Progress"), "severity": text(elem, "Severity"),
                 "report_type": text(elem, "ReportType"), "planned": text(elem, "Planned"),
                 "summary": text(elem, "Summary"),
                 "valid_start": first_text(elem, "StartTime"), "valid_end": first_text(elem, "EndTime")}
        empty = {"affect_type": None, "line_ref": None, "journey_ref": None,
                 "operator_ref": None, "origin_aimed_departure": None}
        affected = []
        for part in elem.iter():
            if local(part.tag) == "AffectedVehicleJourney":
                affected.append({**empty, "affect_type": "journey",
                                 "line_ref": first_text(part, "LineRef"),
                                 "journey_ref": first_text(part, "VehicleJourneyRef"),
                                 "operator_ref": first_text(part, "OperatorRef"),
                                 "origin_aimed_departure": first_text(part, "OriginAimedDepartureTime")})
            elif local(part.tag) == "AffectedNetwork":
                operator = first_text(part, "OperatorRef")
                lines = [first_text(l, "LineRef") for l in part.iter() if local(
                    l.tag) == "AffectedLine"]
                for line_ref in lines or [None]:
                    affected.append({**empty, "affect_type": "line", "line_ref": line_ref,
                                     "operator_ref": operator})
        for a in affected or [empty]:   # an alert without affects still gets one row
            yield {**alert, **a}
        elem.clear()


def parse_estimated_timetable(payload, base):
    for _, elem in ET.iterparse(io.BytesIO(payload)):
        if local(elem.tag) != "EstimatedVehicleJourney":
            continue
        journey = {**base,
                   "recorded_at": text(elem, "RecordedAtTime"),
                   "journey_ref": first_text(elem, "DatedVehicleJourneyRef"),
                   "data_frame_ref": first_text(elem, "DataFrameRef"),
                   "line_ref": text(elem, "LineRef"), "direction_ref": text(elem, "DirectionRef"),
                   "vehicle_mode": text(elem, "VehicleMode"), "operator_ref": text(elem, "OperatorRef"),
                   "monitored": flag(text(elem, "Monitored")),
                   "journey_cancelled": flag(text(elem, "Cancellation"))}
        for call in elem.iter():
            kind = local(call.tag)
            if kind not in ("RecordedCall", "EstimatedCall"):
                continue
            order = text(call, "Order")
            yield {**journey,
                   "call_type": "recorded" if kind == "RecordedCall" else "estimated",
                   "stop_order": int(order) if order else None,
                   "stop_ref": text(call, "StopPointRef"), "stop_name": text(call, "StopPointName"),
                   "destination": text(call, "DestinationDisplay"),
                   "aimed_arrival": text(call, "AimedArrivalTime"),
                   "expected_arrival": text(call, "ExpectedArrivalTime"),
                   "actual_arrival": text(call, "ActualArrivalTime"),
                   "aimed_departure": text(call, "AimedDepartureTime"),
                   "expected_departure": text(call, "ExpectedDepartureTime"),
                   "actual_departure": text(call, "ActualDepartureTime"),
                   "arrival_status": text(call, "ArrivalStatus"),
                   "departure_status": text(call, "DepartureStatus"),
                   "call_cancelled": flag(text(call, "Cancellation")),
                   "prediction_inaccurate": flag(text(call, "PredictionInaccurate"))}
        elem.clear()


FEED_TABLE = {"vehicle_positions": ("vehicle_positions", parse_vehicle_positions),
              "service_alerts": ("service_alerts", parse_service_alerts),
              "estimated_timetable": ("stop_visits", parse_estimated_timetable)}


# ---------------------------------------------------------------------- jobs
def list_files(conn, feed, day, max_files):
    _, objects = conn.get_container(
        RAW_CONTAINER, prefix=f"{feed}/date={day}/", full_listing=True)
    names = sorted(o["name"] for o in objects)
    return names[:max_files] if max_files else names


def download(conn, name):
    _, data = conn.get_object(RAW_CONTAINER, name)
    return gzip.decompress(data) if name.endswith(".gz") else data


def upload(conn, local_path, object_name):
    with open(local_path, "rb") as f:
        conn.put_object(BRONZE_CONTAINER, object_name, contents=f,
                        content_length=os.path.getsize(local_path),
                        content_type="application/vnd.apache.parquet")
    print(f"    uploaded -> {BRONZE_CONTAINER}/{object_name}")


_local = threading.local()


def thread_conn():
    """One Swift connection per download thread (connections are not thread-safe)."""
    if not hasattr(_local, "conn"):
        _local.conn = swift_connection()
    return _local.conn


def fetch(name):
    try:
        return name, download(thread_conn(), name), None
    except Exception as error:
        return name, None, error


def build_realtime(conn, feed, day, workdir, max_files):
    table, parser = FEED_TABLE[feed]
    names = list_files(conn, feed, day, max_files)
    print(f"{feed}: {len(names)} files")
    if not names:
        return
    out = workdir / f"{table}.parquet"
    writer = TableWriter(out, SCHEMAS[table])
    failed = 0
    for i, name in enumerate(names, 1):
        base = {"snapshot_time": snapshot_time(name), "source_file": name}
        try:
            for row in parser(download(conn, name), base):
                writer.add(row)
        except Exception as error:          # one bad file must not stop the day
            failed += 1
            print(f"    ERROR {name}: {error}")
        if i % 100 == 0:
            print(
                f"    {i}/{len(names)} files, {writer.count + len(writer.rows):,} rows")
    writer.close()
    print(f"    {writer.count:,} rows from {len(names) - failed} files ({failed} failed), "
          f"{out.stat().st_size / 1e6:.1f} MB")
    upload(conn, out, f"{table}/date={day}/part-0.parquet")
    out.unlink()


def build_gtfs(conn, day, workdir, max_files):
    names = [n for n in list_files(
        conn, "static_timetable", day, None) if ".zip" in n]
    print(f"static_timetable: {len(names)} files")
    if not names:
        return
    # the day's latest timetable is enough
    name = names[-1]
    with zipfile.ZipFile(io.BytesIO(download(conn, name))) as zf:
        for member in sorted(n for n in zf.namelist() if n.endswith(".txt")):
            data = zf.read(member)
            columns = data.decode("utf-8-sig").split("\n",
                                                     1)[0].strip().split(",")
            columns = [c.strip().strip('"') for c in columns]
            # Read everything as text: GTFS times can be past 24:00 (e.g. 25:10:00)
            tbl = pacsv.read_csv(
                io.BytesIO(data),
                convert_options=pacsv.ConvertOptions(column_types={c: pa.string() for c in columns}))
            tbl = tbl.append_column("snapshot_time", pa.array(
                [snapshot_time(name)] * tbl.num_rows, UTC_TS))
            tbl = tbl.append_column(
                "source_file", pa.array([name] * tbl.num_rows, S))
            table = "gtfs_" + member[:-4]
            out = workdir / f"{table}.parquet"
            pq.write_table(tbl, out, compression="zstd")
            print(f"  {table}: {tbl.num_rows:,} rows")
            upload(conn, out, f"{table}/date={day}/part-0.parquet")
            out.unlink()


def main():
    yesterday = (datetime.now(timezone.utc) -
                 timedelta(days=1)).strftime("%Y-%m-%d")
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", default=yesterday,
                        help="UTC day to process (default: yesterday)")
    parser.add_argument("--feeds", nargs="+", default=FEEDS, choices=FEEDS)
    parser.add_argument("--max-files", type=int,
                        help="only the first N files per feed (for testing)")
    args = parser.parse_args()

    conn = swift_connection()
    conn.put_container(BRONZE_CONTAINER)    # no-op if it exists
    print(
        f"Building bronze tables for {args.date}  ({RAW_CONTAINER} -> {BRONZE_CONTAINER})")
    with tempfile.TemporaryDirectory() as tmp:
        for feed in args.feeds:
            if feed == "static_timetable":
                build_gtfs(conn, args.date, Path(tmp), args.max_files)
            else:
                build_realtime(conn, feed, args.date,
                               Path(tmp), args.max_files)
    print("Done.")


if __name__ == "__main__":
    main()
