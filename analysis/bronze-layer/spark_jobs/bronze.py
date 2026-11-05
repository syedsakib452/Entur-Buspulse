"""Bronze: one UTC day of raw files -> Parquet tables, with Spark.

Reads   <raw>/<feed>/date=D/*.json.gz | *.xml.gz | *.zip     (as the collectors upload them)
Writes  <bronze>/<table>/date=D/part-*.parquet               (same columns as before)

Replaces the swiftclient-based tables/build_bronze_tables.py:
- Spark lists and reads the files through the S3 API, with time limits and retries
  on every request, so one hanging download cannot block the job.
- Files are parsed in parallel worker processes; each worker holds one file at a time
  and streams its rows on, so memory does not grow with the size of the day.

Usage:  python -m spark_jobs.bronze --date 2026-10-30 [--feeds estimated_timetable ...]
"""
import argparse
import gzip
import io
import os
import tempfile
import zipfile

from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, StringType, StructField,
                               StructType, TimestampType)

from spark_jobs import common
from spark_jobs.parsers import FEEDS, snapshot_time

S, D, I, B = StringType(), DoubleType(), IntegerType(), BooleanType()


def _schema(*fields):
    return StructType([StructField("snapshot_time", TimestampType())]
                      + [StructField(n, t) for n, t in fields]
                      + [StructField("source_file", S)])


SCHEMAS = {
    "vehicle_positions": _schema(
        ("vehicle_id", S), ("last_updated", S), ("line_ref", S), ("line_name", S),
        ("public_code", S), ("latitude", D), ("longitude", D), ("bearing", D),
        ("speed", D), ("delay", D), ("occupancy_status", S)),
    "service_alerts": _schema(
        ("situation_number", S), ("creation_time", S), ("version_time", S), ("progress", S),
        ("severity", S), ("report_type", S), ("planned", S), ("summary", S),
        ("valid_start", S), ("valid_end", S), ("affect_type", S), ("line_ref", S),
        ("journey_ref", S), ("operator_ref", S), ("origin_aimed_departure", S)),
    "stop_visits": _schema(
        ("recorded_at", S), ("journey_ref", S), ("data_frame_ref", S), ("line_ref", S),
        ("direction_ref", S), ("vehicle_mode", S), ("operator_ref", S), ("monitored", B),
        ("journey_cancelled", B), ("call_type", S), ("stop_order", I), ("stop_ref", S),
        ("stop_name", S), ("destination", S),
        ("aimed_arrival", S), ("expected_arrival", S), ("actual_arrival", S),
        ("aimed_departure", S), ("expected_departure", S), ("actual_departure", S),
        ("arrival_status", S), ("departure_status", S), ("call_cancelled", B),
        ("prediction_inaccurate", B)),
}


def _object_name(uri):
    """s3a://buspulse-raw/vehicle_positions/date=.../x.json.gz -> vehicle_positions/date=.../x.json.gz"""
    return uri.split("://", 1)[-1].split("/", 1)[-1] if "://" in uri else uri


def _rows(files, parser, columns, bad):
    """Runs in a worker: one raw file at a time -> tuples in schema order."""
    for f in files:
        name = _object_name(f.path)
        try:
            data = bytes(f.content)
            payload = gzip.decompress(data) if name.endswith(".gz") else data
            base = {"snapshot_time": snapshot_time(name), "source_file": name}
            for row in parser(payload, base):
                yield tuple(row.get(c) for c in columns)
        except Exception:            # one broken file must not stop the day; counted, not hidden
            bad.add(1)


def build_feed(spark, feed, day):
    table, parser = FEEDS[feed]
    src = common.path(common.RAW, feed, f"date={day}")
    n_files = len(common.list_files(spark, src))
    if not n_files:
        print(f"{feed}: no raw files for {day}, skipped")
        return 0
    schema = SCHEMAS[table]
    columns = schema.fieldNames()
    bad = spark.sparkContext.accumulator(0)
    files = (spark.read.format("binaryFile").option("pathGlobFilter", "*.*")
             .load(src).select("path", "content"))
    rdd = files.rdd.mapPartitions(lambda it: _rows(it, parser, columns, bad))
    df = spark.createDataFrame(rdd, schema)
    out = common.write(df, common.BRONZE, table, "date", day, files=4)
    n = spark.read.parquet(out).count()
    print(f"{feed}: {n_files} files -> {n:,} rows ({bad.value} unreadable files) -> {out}")
    return bad.value


def build_gtfs(spark, day):
    """The day's newest GTFS zip -> one bronze table per GTFS file, every column as text
    (GTFS times can be past 24:00, so nothing is converted here)."""
    src = common.path(common.RAW, "static_timetable", f"date={day}")
    zips = [p for p in common.list_files(spark, src) if ".zip" in p]
    if not zips:
        print(f"static_timetable: no zip for {day}, skipped (later steps use a neighbouring day's)")
        return
    newest = zips[-1]
    name = _object_name(newest)
    content = (spark.read.format("binaryFile").load(newest).select("content").first()[0])
    data = bytes(content)
    if name.endswith(".gz"):
        data = gzip.decompress(data)
    with tempfile.TemporaryDirectory(dir=os.getenv("SPARK_LOCAL_DIR")) as tmp:
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            members = sorted(m for m in zf.namelist() if m.endswith(".txt"))
            zf.extractall(tmp, members)
        for member in members:
            csv = (spark.read.option("header", True).option("inferSchema", False)
                   .option("encoding", "UTF-8").csv(f"file://{tmp}/{member}"))
            # strip a byte-order mark and quotes from the header
            csv = csv.toDF(*[c.replace("\ufeff", "").strip().strip('"') for c in csv.columns])
            csv = csv.withColumns({"snapshot_time": F.lit(snapshot_time(name)).cast("timestamp"),
                                   "source_file": F.lit(name)})
            table = "gtfs_" + member[:-4]
            out = common.write(csv, common.BRONZE, table, "date", day, files=1)
            print(f"  {table}: {spark.read.parquet(out).count():,} rows")


def run(spark, day, feeds=None):
    feeds = feeds or [*FEEDS, "static_timetable"]
    print(f"=== bronze {day}")
    bad = 0
    for feed in feeds:
        if feed == "static_timetable":
            build_gtfs(spark, day)
        else:
            bad += build_feed(spark, feed, day)
    return bad


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", required=True, help="UTC day of the raw files, e.g. 2026-10-30")
    p.add_argument("--feeds", nargs="+", choices=[*FEEDS, "static_timetable"])
    a = p.parse_args()
    spark = common.spark_session("buspulse-bronze")
    run(spark, a.date, a.feeds)
    common.mark_done(spark, "bronze", a.date)


if __name__ == "__main__":
    main()
