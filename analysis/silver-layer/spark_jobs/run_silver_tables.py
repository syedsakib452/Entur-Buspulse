"""Build silver tables from local date-first bronze Parquet folders.

Bronze input: TABLES_DIR/bronze-layer/<date>/<table>.parquet
Silver output: TABLES_DIR/silver-layer/<date>/<table>.parquet
"""
import argparse
from datetime import date, timedelta
import hashlib
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
    return (root / "bronze-layer" / day / f"{table}.parquet").is_file()


def _code_signature():
    """Fingerprint the Silver transformations and pinned runtime dependencies."""
    layer_root = Path(__file__).resolve().parents[1]
    files = [
        layer_root / "requirements.txt",
        layer_root / "spark_jobs" / "common.py",
        layer_root / "spark_jobs" / "run_silver_tables.py",
        layer_root / "spark_jobs" / "silver.py",
        layer_root / "spark_jobs" / "stop_analysis.py",
        layer_root / "spark_jobs" / "stop_geometry.py",
    ]
    digest = hashlib.sha256()
    for path in files:
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def main():
    os.environ.setdefault("TABLES_DIR", "tables")
    os.environ.setdefault("DATA_ROOT", "/tmp/buspulse-silver-runtime")
    os.environ.setdefault(
        "SILVER_LOCAL_DIR", str(Path(os.environ["TABLES_DIR"]) / "silver-layer")
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

    code_signature = _code_signature()
    spark = common.spark_session("buspulse-silver-local")
    failures = []
    try:
        print(f"=== local silver tables: {args.start} .. {args.end or 'available dates'}")
        for day in requested:
            marker = tables / "silver-layer" / day / "_done_clean"
            stop_marker = tables / "silver-layer" / day / "_done_stop_analysis"
            signature_path = marker.parent / "_silver_code_signature"
            previous_signature = (signature_path.read_text(encoding="utf-8").strip()
                                  if signature_path.is_file() else "")
            code_changed = previous_signature != code_signature
            if not args.redo and not code_changed and marker.is_file() and stop_marker.is_file():
                print(f"--- silver {day}: already done with current code, skipped")
                continue
            missing = []
            if not _has_bronze(tables, "stop_visits", day):
                missing.append(f"bronze-layer/{day}/stop_visits.parquet")
            next_day = common.shift(day, 1)
            if not _has_bronze(tables, "stop_visits", next_day):
                missing.append(f"bronze-layer/{next_day}/stop_visits.parquet")
            try:
                silver._gtfs_dir(day)
            except FileNotFoundError:
                missing.append(f"GTFS tables under {bronze_root}/<date>")
            if missing:
                print(f"--- silver {day}: waiting for {', '.join(missing)}")
                continue

            if args.redo or code_changed:
                marker.unlink(missing_ok=True)
                stop_marker.unlink(missing_ok=True)
                signature_path.unlink(missing_ok=True)
                if code_changed and previous_signature:
                    print(f"--- silver {day}: processing code changed; rebuilding both Silver stages")
            try:
                clean_needed = args.redo or code_changed or not marker.is_file()
                stop_needed = args.redo or code_changed or clean_needed or not stop_marker.is_file()
                if clean_needed:
                    silver.run(spark, day)
                    common.mark_done(spark, "clean", day)
                if stop_needed:
                    stop_analysis.run(spark, day)
                    common.mark_done(spark, "stop_analysis", day)
                if marker.is_file() and stop_marker.is_file():
                    temp_signature = signature_path.with_name(signature_path.name + ".tmp")
                    temp_signature.write_text(code_signature + "\n", encoding="utf-8")
                    os.replace(temp_signature, signature_path)
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
