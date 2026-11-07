"""Build Gold locally from complete folders in tables/silver-layer.

Input:  tables/silver-layer/<date>/<table>.parquet
Output: tables/gold-layer/<gold-table>.parquet and gold.duckdb
Only dates with both _done_clean and _done_stop_analysis markers are included.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import duckdb

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parents[1]
SILVER_DIR = Path(os.getenv("SILVER_DIR", PROJECT_ROOT / "tables" / "silver-layer"))
GOLD_DIR = Path(os.getenv("GOLD_DIR", PROJECT_ROOT / "tables" / "gold-layer"))
WORK = Path(os.getenv("GOLD_WORK_DIR", "/work/gold"))
SILVER_TABLES = [
    "clean_stop_visits", "stop_event_status", "expected_per_stop",
    "stop_gps_status", "trip_vehicle", "per_stop_evidence", "time_lost_null",
    "stop_headways", "trip_segments", "removal_log",
]


def completed_days(start=None, end=None):
    """Find date folders with completed clean + stop-analysis outputs."""
    if not SILVER_DIR.is_dir():
        raise FileNotFoundError(f"Silver directory does not exist: {SILVER_DIR}")
    days = []
    waiting = []
    for folder in sorted(SILVER_DIR.iterdir()):
        if not folder.is_dir():
            continue
        day = folder.name
        try:
            from datetime import date
            date.fromisoformat(day)
        except ValueError:
            continue
        if start and day < start or end and day > end:
            continue
        ready = (folder / "_done_clean").is_file() and (folder / "_done_stop_analysis").is_file()
        if ready:
            days.append(day)
        elif any(folder.glob("*.parquet")):
            waiting.append(day)
    if waiting:
        print(f"Silver dates not yet complete (skipped): {waiting}")
    return days


def _code_signature():
    """Fingerprint the Gold builder, runner and pinned DuckDB version."""
    files = [HERE / "requirements.txt", HERE / "build_gold.py", HERE / "run_gold.py"]
    digest = hashlib.sha256()
    for path in files:
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _signature(days):
    result = {}
    for day in days:
        folder = SILVER_DIR / day
        result[day] = {}
        for item in sorted(folder.iterdir()):
            if item.is_file() and item.suffix == ".parquet":
                stat = item.stat()
                result[day][item.name] = [stat.st_size, stat.st_mtime_ns]
    return result


def combine_silver(days, destination):
    """Merge each table across complete service dates into the flat files Gold expects."""
    destination.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        for table in SILVER_TABLES:
            sources = [SILVER_DIR / day / f"{table}.parquet" for day in days]
            sources = [p for p in sources if p.is_file()]
            if not sources:
                print(f"  {table}: not present, skipped")
                continue
            out = destination / f"{table}.parquet"
            paths_sql = "[" + ",".join("'" + p.as_posix().replace("'", "''") + "'" for p in sources) + "]"
            sql = (f"COPY (SELECT * FROM read_parquet({paths_sql}, union_by_name=true)) "
                   f"TO '{out.as_posix().replace(chr(39), chr(39)*2)}' "
                   "(FORMAT PARQUET, COMPRESSION ZSTD)")
            con.execute(sql)
            rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{out.as_posix()}')").fetchone()[0]
            print(f"  {table}: {len(sources)} date files -> {rows:,} rows")
    finally:
        con.close()


def _load_state():
    path = GOLD_DIR / "_gold_state.json"
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _atomic_copy(source, destination):
    temp = destination.with_name(destination.name + ".tmp")
    shutil.copy2(source, temp)
    os.replace(temp, destination)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", default=os.getenv("START_DATE"), help="first service date (inclusive)")
    parser.add_argument("--end", default=os.getenv("END_DATE"), help="last service date (inclusive)")
    parser.add_argument("--redo", action="store_true", default=os.getenv("REDO", "").lower() in ("1", "true", "yes"),
                        help="force a rebuild even when the Silver inputs have not changed")
    args = parser.parse_args()
    days = completed_days(args.start, args.end)
    print(f"=== Gold input: {SILVER_DIR}; complete dates: {days}", flush=True)
    if not days:
        print("=== no complete Silver dates; nothing to build")
        return 0

    signature = {"silver_inputs": _signature(days), "gold_code": _code_signature()}
    previous = _load_state()
    if not args.redo and previous and previous.get("input_signature") == signature:
        print("=== Gold is already up to date; no rebuild needed")
        return 0

    shutil.rmtree(WORK, ignore_errors=True)
    staged_silver = WORK / "silver"
    staged_gold = WORK / "gold"
    staged_silver.mkdir(parents=True, exist_ok=True)
    staged_gold.mkdir(parents=True, exist_ok=True)
    combine_silver(days, staged_silver)

    env = dict(os.environ, SILVER_DIR=str(staged_silver), GOLD_DIR=str(staged_gold))
    command = [sys.executable, str(HERE / "build_gold.py")]
    print("=== running DuckDB Gold build", flush=True)
    result = subprocess.run(command, cwd=HERE, env=env)
    if result.returncode:
        print(f"=== Gold build failed (exit {result.returncode}); existing Gold output was not replaced")
        return result.returncode

    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    generated = sorted(p for p in staged_gold.iterdir() if p.is_file())
    new_names = {p.name for p in generated}
    old_names = (previous or {}).get("files", [])
    for source in generated:
        _atomic_copy(source, GOLD_DIR / source.name)
    for old in old_names:
        if old not in new_names:
            (GOLD_DIR / old).unlink(missing_ok=True)

    state = {"service_dates": days, "input_signature": signature,
             "files": sorted(new_names)}
    state_path = GOLD_DIR / "_gold_state.json"
    temp_state = state_path.with_suffix(".json.tmp")
    temp_state.write_text(json.dumps(state, indent=2), encoding="utf-8")
    os.replace(temp_state, state_path)
    shutil.rmtree(WORK, ignore_errors=True)
    print(f"=== Gold complete: {len(generated)} files written to {GOLD_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
