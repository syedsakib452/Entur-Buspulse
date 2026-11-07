"""Bronze tables: turn one day of raw files in Swift into Parquet tables.

Reads   buspulse-raw/<feed>/date=YYYY-MM-DD/*.{json,xml,zip}[.gz]
Writes  <repo>/tables/bronze-layer/YYYY-MM-DD/<table>.parquet   (local disk, change with --out-dir)
Audit   <repo>/tables/bronze-layer/_audit/YYYY-MM-DD/<feed>_files.csv   (one row per raw file)

Values are copied as they are (timestamps stay as the original text);
cleaning happens later, in silver. Every row gets snapshot_time (from the
file name) and source_file, so it can be traced back to its raw file.
Running it again for the same day overwrites matching output objects.

Speed: each worker PROCESS downloads (several threads) + decompresses + parses
a small batch of files and returns an Arrow table. The main process only appends
tables to the Parquet file. XML/JSON parsing is CPU bound, so processes (not
threads) are what scale.

Audit: every file ends up as ok / empty / failed (with stage + error). A file
that fails halfway contributes NO rows (all-or-nothing per file).

Usage (OpenStack login loaded into the environment first):
    python analysis/bronze-layer/build_bronze_tables.py                          # yesterday (UTC), all feeds
    python analysis/bronze-layer/build_bronze_tables.py --date 2026-10-30
    python analysis/bronze-layer/build_bronze_tables.py --date 2026-10-30 --feeds vehicle_positions --max-files 10

Tuning (env vars):
    PROCS=8            worker processes (default: min(cpu_count, 8))
    DL_THREADS=4       download threads inside each process
    FILES_PER_TASK=25  files handled per task
    SWIFT_TIMEOUT=30   seconds before a hung Swift request is retried
    ROW_GROUP_ROWS=250000
Total Swift connections = PROCS * DL_THREADS; lower them if Swift returns 400/429.
"""
import argparse
import csv
import gzip
import io
import json
import multiprocessing
import os
import re
import shutil
import sys
import tempfile
import threading
import time
import zipfile
from collections import deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import xml.etree.ElementTree as ET

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from swiftclient.client import Connection
from swiftclient.exceptions import ClientException

try:                      # optional, much faster JSON parsing
    import orjson
    json_loads = orjson.loads
except ImportError:
    json_loads = json.loads

RAW_CONTAINER = os.getenv("RAW_CONTAINER", "buspulse-raw")
PROCS = int(os.getenv("PROCS", str(min(os.cpu_count() or 2, 8))))
DL_THREADS = int(os.getenv("DL_THREADS", "4"))
FILES_PER_TASK = int(os.getenv("FILES_PER_TASK", "25"))
SWIFT_TIMEOUT = int(os.getenv("SWIFT_TIMEOUT", "30"))
ROW_GROUP_ROWS = int(os.getenv("ROW_GROUP_ROWS", "250000"))
EVERY = 1   # use only every Nth file per feed; set with --every
# <repo>/tables/bronze-layer, independent of the working directory
OUT_DIR = Path(os.getenv("OUT_DIR", str(Path(__file__).resolve().parents[2] / "tables" / "bronze-layer")))
AUDIT_FIELDS = ["file", "status", "stage", "rows", "bytes", "seconds", "error", "note"]
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
        ("situation_number", S), ("creation_time", S), ("version_time", S), ("progress", S),
        ("severity", S), ("report_type", S), ("planned", S), ("summary", S),
        ("valid_start", S), ("valid_end", S), ("affect_type", S), ("line_ref", S),
        ("journey_ref", S), ("operator_ref", S), ("origin_aimed_departure", S)),
    "stop_visits": schema(
        ("recorded_at", S), ("journey_ref", S), ("data_frame_ref", S), ("line_ref", S),
        ("direction_ref", S), ("vehicle_mode", S), ("operator_ref", S), ("monitored", B),
        ("journey_cancelled", B), ("call_type", S), ("stop_order", I), ("stop_ref", S),
        ("stop_name", S), ("destination", S),
        ("aimed_arrival", S), ("expected_arrival", S), ("actual_arrival", S),
        ("aimed_departure", S), ("expected_departure", S), ("actual_departure", S),
        ("arrival_status", S), ("departure_status", S), ("call_cancelled", B),
        ("prediction_inaccurate", B)),
}


# --------------------------------------------------------------------- helpers
def swift_connection():
    """Same OS_* variables as openstack.env: a password login (VM) or an
    application credential (Kubernetes Secret)."""
    env = os.environ
    return Connection(
        authurl=env["OS_AUTH_URL"], user=env.get("OS_USERNAME"), key=env.get("OS_PASSWORD"),
        auth_version="3",
        timeout=SWIFT_TIMEOUT,   # give up on a hung request and retry it (env SWIFT_TIMEOUT, seconds)
        retries=5,               # and try it again, up to 5 times
        os_options={k: v for k, v in {
            "auth_type": env.get("OS_AUTH_TYPE"),
            "application_credential_id": env.get("OS_APPLICATION_CREDENTIAL_ID"),
            "application_credential_secret": env.get("OS_APPLICATION_CREDENTIAL_SECRET"),
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


def short(err):
    """One-line error text, safe for logs and CSV."""
    return f"{type(err).__name__}: {err}".replace("\n", " ").replace("\r", " ")[:300]


class TableWriter:
    """Buffers Arrow tables and writes them to one Parquet file in big row groups."""

    def __init__(self, path, schema):
        self.buf, self.buffered, self.count = [], 0, 0
        self.writer = pq.ParquetWriter(path, schema, compression="zstd")

    def add(self, table):
        self.buf.append(table)
        self.buffered += table.num_rows
        if self.buffered >= ROW_GROUP_ROWS:
            self.flush()

    def flush(self):
        if self.buf:
            table = pa.concat_tables(self.buf)
            self.writer.write_table(table, row_group_size=ROW_GROUP_ROWS)
            self.count += table.num_rows
            self.buf, self.buffered = [], 0

    def close(self):
        self.flush()
        self.writer.close()


# ------------------------------------------------------------------- parsers
# Each parser gets the raw bytes of one file and yields table rows (dicts).

def parse_vehicle_positions(payload, base):
    for v in json_loads(payload).get("data", {}).get("vehicles") or []:
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
                lines = [first_text(l, "LineRef") for l in part.iter()
                         if local(l.tag) == "AffectedLine"]
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


# ------------------------------------------------------------------ swift I/O
def list_files(conn, feed, day, max_files, every=1):
    _, objects = conn.get_container(
        RAW_CONTAINER, prefix=f"{feed}/date={day}/", full_listing=True)
    names = sorted(o["name"] for o in objects)[::every]
    return names[:max_files] if max_files else names


def download(conn, name, attempts=3):
    """GET with a few extra retries (on top of swiftclient's own), then gunzip."""
    for attempt in range(1, attempts + 1):
        try:
            _, data = conn.get_object(RAW_CONTAINER, name)
            break
        except Exception:
            if attempt == attempts:
                raise
            time.sleep(2 ** attempt)
    return gzip.decompress(data) if name.endswith(".gz") else data


def upload(conn, local_path, object_name, content_type=None):
    """Save a finished file under OUT_DIR (name kept so callers stay the same).
    Copies to a .tmp file and renames, so a crash never leaves a half-written table."""
    dest = OUT_DIR / object_name
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".tmp")
    shutil.copyfile(local_path, tmp)
    os.replace(tmp, dest)
    print(f"    saved -> {dest}", flush=True)


_local = threading.local()


def thread_conn():
    """One Swift connection per thread (connections are not thread-safe)."""
    if not hasattr(_local, "conn"):
        _local.conn = swift_connection()
    return _local.conn


# ------------------------------------------------------------- worker process
def process_batch(feed, names):
    """Runs in a worker process. Returns (audit_entries, arrow_table_or_None)."""
    table_name, parser = FEED_TABLE[feed]
    schema_ = SCHEMAS[table_name]

    def fetch(name):
        t0 = time.perf_counter()
        try:
            return name, download(thread_conn(), name), None, time.perf_counter() - t0
        except Exception as error:
            return name, None, error, time.perf_counter() - t0

    entries, good = [], []          # good: (entry, rows)
    with ThreadPoolExecutor(DL_THREADS) as pool:
        for name, payload, error, dl_secs in pool.map(fetch, names):
            entry = {"file": name, "status": "ok", "stage": "", "rows": 0,
                     "bytes": len(payload) if payload else 0, "seconds": 0.0,
                     "error": "", "note": ""}
            base = {"snapshot_time": snapshot_time(name), "source_file": name}
            stage, t0 = "download", time.perf_counter()
            try:
                if error:
                    raise error
                stage = "parse"
                if not payload:
                    raise ValueError("empty payload")
                rows = list(parser(payload, base))   # all-or-nothing per file
                entry["rows"] = len(rows)
                if not rows:
                    entry["status"] = "empty"
                if base["snapshot_time"] is None:
                    entry["note"] = "no snapshot_time in file name"
                good.append((entry, rows))
            except Exception as err:
                entry.update(status="failed", stage=stage, error=short(err))
            entry["seconds"] = round(dl_secs + time.perf_counter() - t0, 2)
            entries.append(entry)

    table = None
    if good:
        try:
            table = pa.Table.from_pylist([r for _, rows in good for r in rows], schema=schema_)
        except Exception:
            # find which file(s) break the Arrow conversion instead of losing the batch
            tables = []
            for entry, rows in good:
                try:
                    tables.append(pa.Table.from_pylist(rows, schema=schema_))
                except Exception as err:
                    entry.update(status="failed", stage="arrow", rows=0, error=short(err))
            table = pa.concat_tables(tables) if tables else None
    return entries, table


# -------------------------------------------------------------------- audit
def write_audit(conn, day, feed, entries):
    path = OUT_DIR / "_audit" / day / f"{feed}_files.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=AUDIT_FIELDS)
        w.writeheader()
        w.writerows(sorted(entries, key=lambda e: e["file"]))
    print(f"    audit -> {path}", flush=True)


# ---------------------------------------------------------------------- jobs
def build_realtime(conn, feed, day, workdir, max_files):
    """Returns the number of failed raw files."""
    table, _ = FEED_TABLE[feed]
    names = list_files(conn, feed, day, max_files, EVERY)
    print(f"{feed}: {len(names)} files ({PROCS} processes x {DL_THREADS} download threads)",
          flush=True)
    if not names:
        print(f"    WARN no raw files found for {feed} on {day}", flush=True)
        return 0

    out = workdir / f"{table}.parquet"
    writer = TableWriter(out, SCHEMAS[table])
    audit, state = [], {"last_report": 0}
    started = time.perf_counter()

    def consume(batch, future):
        try:
            entries, tbl = future.result()
        except Exception as err:     # worker crashed (e.g. out of memory)
            entries = [{"file": n, "status": "failed", "stage": "worker", "rows": 0,
                        "bytes": 0, "seconds": 0, "error": short(err), "note": ""}
                       for n in batch]
            tbl = None
        for e in entries:
            if e["status"] == "failed":
                print(f"    ERROR {e['file']}: [{e['stage']}] {e['error']}", flush=True)
        audit.extend(entries)
        if tbl is not None:
            writer.add(tbl)
        if len(audit) - state["last_report"] >= 100:
            state["last_report"] = len(audit)
            rate = len(audit) / (time.perf_counter() - started)
            print(f"    {len(audit)}/{len(names)} files ({rate:.1f} files/s), "
                  f"{writer.count + writer.buffered:,} rows", flush=True)

    batches = [names[i:i + FILES_PER_TASK] for i in range(0, len(names), FILES_PER_TASK)]
    pending = deque()
    ctx = multiprocessing.get_context("spawn")      # safe with pyarrow threads
    with ProcessPoolExecutor(PROCS, mp_context=ctx) as pool:
        for batch in batches:
            pending.append((batch, pool.submit(process_batch, feed, batch)))
            while len(pending) >= PROCS * 2:        # bounded: limits memory
                consume(*pending.popleft())
        while pending:
            consume(*pending.popleft())
    writer.close()

    failed = sum(e["status"] == "failed" for e in audit)
    empty = sum(e["status"] == "empty" for e in audit)
    expected = sum(e["rows"] for e in audit if e["status"] != "failed")
    size_mb = out.stat().st_size / 1e6
    print(f"    {writer.count:,} rows from {len(names) - failed} files ({failed} failed), "
          f"{size_mb:.1f} MB", flush=True)
    if empty:
        print(f"    WARN {empty} files parsed fine but contained no rows", flush=True)
    written = pq.ParquetFile(out).metadata.num_rows
    if written != expected:
        print(f"    VERIFY MISMATCH: parquet has {written:,} rows, audit expected {expected:,}",
              flush=True)

    if writer.count == 0 and failed:
        # do not overwrite yesterday's good output with an empty file
        print(f"    SKIPPED upload of {table}: no rows written and {failed} files failed",
              flush=True)
    else:
        upload(conn, out, f"{day}/{table}.parquet")
    out.unlink()
    write_audit(conn, day, feed, audit)
    return failed


def gtfs_member(blob, member, name, day, workdir):
    """Convert one GTFS .txt to Parquet and upload it (runs in a thread)."""
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        data = zf.read(member)
    columns = data.decode("utf-8-sig").split("\n", 1)[0].strip().split(",")
    columns = [c.strip().strip('"') for c in columns]
    # Read everything as text: GTFS times can be past 24:00 (e.g. 25:10:00)
    tbl = pacsv.read_csv(
        io.BytesIO(data),
        convert_options=pacsv.ConvertOptions(column_types={c: pa.string() for c in columns}))
    tbl = tbl.append_column("snapshot_time", pa.array(
        [snapshot_time(name)] * tbl.num_rows, UTC_TS))
    tbl = tbl.append_column("source_file", pa.array([name] * tbl.num_rows, S))
    table = "gtfs_" + member[:-4]
    out = workdir / f"{table}.parquet"
    pq.write_table(tbl, out, compression="zstd")
    upload(thread_conn(), out, f"{day}/{table}.parquet")
    out.unlink()
    return table, tbl.num_rows


def build_gtfs(conn, day, workdir, max_files):
    """Returns the number of failed GTFS tables."""
    names = [n for n in list_files(conn, "static_timetable", day, None) if ".zip" in n]
    print(f"static_timetable: {len(names)} files", flush=True)
    if not names:
        print(f"    WARN no static_timetable zip found on {day}", flush=True)
        return 0
    name = names[-1]               # the day's latest timetable is enough
    audit = []
    try:
        blob = download(conn, name)
        with zipfile.ZipFile(io.BytesIO(blob)) as zf:
            members = sorted(n for n in zf.namelist() if n.endswith(".txt"))
    except Exception as err:
        print(f"    ERROR {name}: [download/unzip] {short(err)}", flush=True)
        audit.append({"file": name, "status": "failed", "stage": "download", "rows": 0,
                      "bytes": 0, "seconds": 0, "error": short(err), "note": ""})
        write_audit(conn, day, "static_timetable", audit)
        return 1

    def run(member):
        t0 = time.perf_counter()
        entry = {"file": f"{name}::{member}", "status": "ok", "stage": "", "rows": 0,
                 "bytes": 0, "seconds": 0, "error": "", "note": ""}
        try:
            table, rows = gtfs_member(blob, member, name, day, workdir)
            entry["rows"] = rows
            print(f"  {table}: {rows:,} rows", flush=True)
            if rows == 0:
                entry["status"] = "empty"
        except Exception as err:
            entry.update(status="failed", stage="convert/upload", error=short(err))
            print(f"    ERROR {entry['file']}: [convert/upload] {entry['error']}", flush=True)
        entry["seconds"] = round(time.perf_counter() - t0, 2)
        return entry

    with ThreadPoolExecutor(min(4, max(1, len(members)))) as pool:
        audit = list(pool.map(run, members))
    failed = sum(e["status"] == "failed" for e in audit)
    total = sum(e["rows"] for e in audit)
    print(f"    {total:,} rows from {len(members) - failed} files ({failed} failed), "
          f"zip {len(blob) / 1e6:.1f} MB", flush=True)
    write_audit(conn, day, "static_timetable", audit)
    return failed


def main():
    global EVERY, OUT_DIR
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", default=yesterday, help="UTC day to process (default: yesterday)")
    parser.add_argument("--feeds", nargs="+", default=FEEDS, choices=FEEDS)
    parser.add_argument("--max-files", type=int, help="only the first N files per feed (for testing)")
    parser.add_argument("--every", type=int, default=1,
                        help="use only every Nth file per feed, e.g. 10")
    parser.add_argument("--out-dir", default=str(OUT_DIR),
                        help="where tables are written: <out-dir>/<date>/<table>.parquet")
    parser.add_argument("--strict", action="store_true",
                        help="exit with code 1 if any raw file failed")
    args = parser.parse_args()
    EVERY = args.every
    OUT_DIR = Path(args.out_dir).expanduser()

    conn = swift_connection()
    print(f"Building bronze tables for {args.date}  ({RAW_CONTAINER} -> {OUT_DIR / args.date})"
          + (f", every {EVERY}th file" if EVERY > 1 else ""), flush=True)
    total_failed = 0
    started = time.perf_counter()
    with tempfile.TemporaryDirectory() as tmp:
        for feed in args.feeds:
            if feed == "static_timetable":
                total_failed += build_gtfs(conn, args.date, Path(tmp), args.max_files)
            else:
                total_failed += build_realtime(conn, feed, args.date, Path(tmp), args.max_files)
    print(f"Done in {time.perf_counter() - started:.0f}s. Failed files/tables: {total_failed}",
          flush=True)
    return 1 if (args.strict and total_failed) else 0


if __name__ == "__main__":
    sys.exit(main())