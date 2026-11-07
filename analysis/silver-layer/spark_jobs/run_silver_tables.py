"""Build silver tables from local date-first bronze Parquet folders.

Bronze input: TABLES_DIR/bronze/<date>/<table>.parquet
Silver output: TABLES_DIR/silver/<date>/<table>.parquet
"""
import argparse
from datetime import date, timedelta
import os
from pathlib import Path
import traceback


def _date_dirs(root, start):
    dates = []
    for path in root.iterdir():
        if not path.is_dir():
            continue
        try:
            day = date.fromisoformat(path.name)
        except ValueError:
            continue
        if day.isoformat() >= start:
            dates.append(day.isoformat())
    return sorted(dates)


def _requested_days(root, start, end):
    first = date.fromisoformat(start)
    if end:
        last = date.fromisoformat(end)
        if first > last:
            raise ValueError("--start must be on or before --end")
        count = (last - first).days
        return [(first + timedelta(days=offset)).isoformat() for offset in range(count + 1)]
    return _date_dirs(root, start)


def _has_bronze(root, table, day):
    return (root / "bronze" / day / f"{table}.parquet").is_file()


def main():
    os.environ.setdefault("TABLES_DIR", "tables")
    os.environ.setdefault("DATA_ROOT", "/tmp/buspulse-silver-runtime")
    os.environ.setdefault(
        "SILVER_LOCAL_DIR", str(Path(os.environ["TABLES_DIR"]) / "silver")
    )
    from spark_jobs import common, silver, stop_analysis

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default=os.getenv("START_DATE") or "2026-10-20")
    parser.add_argument("--end", default=os.getenv("END_DATE") or "")
    parser.add_argument(
        "--redo", action="store_true",
        default=os.getenv("REDO", "").lower() in ("1", "true", "yes"),
        help="rebuild silver even when local completion markers exist",
    )
    args = parser.parse_args()
    date.fromisoformat(args.start)
    if args.end:
        date.fromisoformat(args.end)

    tables = common.tables_dir()
    bronze_root = tables / "bronze-layer"
    if not bronze_root.is_dir():
        parser.error(f"bronze tables directory does not exist: {bronze_root}")

    requested = _requested_days(bronze_root, args.start, args.end)
    if not requested:
        print(f"No bronze date folders found from {args.start} onward under {bronze_root}.")
        return 0

    spark = common.spark_session("buspulse-silver-local")
    failures = []
    try:
        print(f"=== local silver tables: {args.start} .. {args.end or 'available dates'}")
        for day in requested:
            marker = tables / "silver" / day / "_done_clean"
            stop_marker = tables / "silver" / day / "_done_stop_analysis"
            if not args.redo and marker.is_file() and stop_marker.is_file():
                print(f"--- silver {day}: already done, skipped")
                continue
            missing = []
            if not _has_bronze(tables, "stop_visits", day):
                missing.append(f"bronze/{day}/stop_visits.parquet")
            next_day = common.shift(day, 1)
            if not _has_bronze(tables, "stop_visits", next_day):
                missing.append(f"bronze/{next_day}/stop_visits.parquet")
            try:
                silver._gtfs_dir(day)
            except FileNotFoundError:
                missing.append(f"GTFS tables under {bronze_root}/<date>")
            if missing:
                print(f"--- silver {day}: waiting for {', '.join(missing)}")
                continue

            if args.redo:
                marker.unlink(missing_ok=True)
                stop_marker.unlink(missing_ok=True)
            try:
                if args.redo or not marker.is_file():
                    silver.run(spark, day)
                    common.mark_done(spark, "clean", day)
                if args.redo or not stop_marker.is_file():
                    stop_analysis.run(spark, day)
                    common.mark_done(spark, "stop_analysis", day)
                print(f"--- silver {day}: done")
            except Exception as error:
                failures.append(day)
                print(f"--- silver {day}: FAILED: {error}", flush=True)
                traceback.print_exc()
    finally:
        spark.stop()

    print(f"=== silver finished; failed dates: {failures or 'none'}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
