"""Run the Swift bronze builder over a date range with bounded parallelism.

Examples:
    python run_bronze_range.py --start 2026-10-22 --end 2026-11-05 --workers 4
    python run_bronze_range.py --catch-up --since 2026-10-20     # every missing day up to yesterday (UTC)
"""
import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
import os
from pathlib import Path
import re
import subprocess
import sys
import threading

HERE = Path(__file__).resolve().parent
FEED_START = re.compile(r"^([a-z_]+): (\d+) files(?: \([^)]*\))?(?: ->.*)?$")
FEED_SUMMARY = re.compile(
    r"^\s+([\d,]+) rows from ([\d,]+) files \((\d+) failed\)(?:,|$)"
)
FILE_ERROR = re.compile(r"^\s+ERROR (.+?): (.*)$")
OUTPUT_LOCK = threading.Lock()


def date_range(start, end):
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    if first > last:
        raise ValueError("--start must be on or before --end")
    current = first
    while current <= last:
        yield current.isoformat()
        current += timedelta(days=1)


def default_out_dir():
    value = os.getenv("OUT_DIR")
    return Path(value) if value else HERE.parent / "tables" / "bronze-layer"


def is_complete(out_dir, day):
    """A day is done if bronze wrote _SUCCESS, or all 3 realtime tables already exist."""
    folder = out_dir / day
    return (folder / "_SUCCESS").exists() or all(
        (folder / f"{table}.parquet").exists()
        for table in ("vehicle_positions", "service_alerts", "stop_visits")
    )


def missing_days(out_dir, since, until):
    if date.fromisoformat(since) > date.fromisoformat(until):
        return []
    return [day for day in date_range(since, until) if not is_complete(out_dir, day)]


def run_day(day, log_dir):
    log_path = log_dir / f"{day}.log"
    env = os.environ.copy()
    command = [sys.executable, "-u", str(HERE / "build_bronze_tables.py"),
               "--date", day, "--strict"]
    feed_counts = {}
    errors = []
    current_feed = ""

    with log_path.open("w", encoding="utf-8") as log:
        process = subprocess.Popen(
            command, cwd=HERE.parent.parent, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, bufsize=1, env=env,
        )
        assert process.stdout is not None
        for line in process.stdout:
            log.write(line)
            log.flush()
            with OUTPUT_LOCK:
                print(f"[{day}] {line}", end="", flush=True)

            match = FEED_START.match(line.rstrip("\n"))
            if match:
                current_feed = match.group(1)
                feed_counts[current_feed] = {
                    "listed": int(match.group(2)),
                    "processed": 0,
                    "failed": 0,
                }
                continue

            match = FILE_ERROR.match(line.rstrip("\n"))
            if match:
                errors.append({
                    "date": day,
                    "feed": current_feed,
                    "file": match.group(1),
                    "error": match.group(2),
                })
                continue

            match = FEED_SUMMARY.match(line.rstrip("\n"))
            if match and current_feed in feed_counts:
                feed_counts[current_feed]["processed"] = int(match.group(2).replace(",", ""))
                feed_counts[current_feed]["failed"] = int(match.group(3))
                continue


        return_code = process.wait()

    return {
        "date": day,
        "return_code": return_code,
        "log": str(log_path),
        "feeds": feed_counts,
        "errors": errors,
    }


def write_audit(results, log_dir):
    with (log_dir / "summary.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "feed", "listed_files", "processed_files", "failed_files", "exit_code", "log"])
        for result in sorted(results, key=lambda item: item["date"]):
            feeds = result["feeds"] or {"": {"listed": 0, "processed": 0, "failed": 0}}
            for feed, counts in feeds.items():
                writer.writerow([
                    result["date"], feed, counts["listed"], counts["processed"],
                    counts["failed"], result["return_code"], result["log"],
                ])

    with (log_dir / "failed_files.csv").open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "feed", "file", "error"])
        writer.writeheader()
        for result in sorted(results, key=lambda item: item["date"]):
            writer.writerows(result["errors"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", help="first UTC date, YYYY-MM-DD")
    parser.add_argument("--end", help="last UTC date, YYYY-MM-DD")
    parser.add_argument("--catch-up", action="store_true",
                        help="process every day from --since up to yesterday (UTC) that is not complete yet")
    parser.add_argument("--since", default=os.getenv("BRONZE_SINCE"),
                        help="oldest day to fetch in --catch-up mode, YYYY-MM-DD")
    parser.add_argument("--out-dir", default=str(default_out_dir()),
                        help="where bronze tables are written (default: $OUT_DIR)")
    parser.add_argument("--workers", type=int, default=4, help="maximum dates running at once")
    parser.add_argument(
        "--log-dir", default=str(Path.home() / "buspulse-bronze-audit"),
        help="directory for per-day logs and audit CSV files",
    )
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be at least 1")

    out_dir = Path(args.out_dir).expanduser()
    os.environ["OUT_DIR"] = str(out_dir)   # build_bronze_tables.py (child process) reads this

    # ---- decide which days to run
    if args.catch_up:
        if not args.since:
            parser.error("--catch-up needs --since (or the BRONZE_SINCE env var)")
        try:
            date.fromisoformat(args.since)
        except ValueError:
            parser.error("--since must be YYYY-MM-DD")
        yesterday = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
        days = missing_days(out_dir, args.since, yesterday)
        if not days:
            print(f"Nothing to do: all days {args.since}..{yesterday} are complete.", flush=True)
            return 0
    else:
        if not (args.start and args.end):
            parser.error("give --start and --end, or use --catch-up")
        try:
            days = list(date_range(args.start, args.end))
        except ValueError as error:
            parser.error(str(error))

    log_dir = Path(args.log_dir).expanduser()
    log_dir.mkdir(parents=True, exist_ok=True)
    results = []
    print(
        f"Running {len(days)} dates ({days[0]} through {days[-1]}) "
        f"with at most {args.workers} in parallel "
        f"(standalone Bronze). Logs/audit: {log_dir}",
        flush=True,
    )

    # ---- run them
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_day, day, log_dir): day for day in days}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            failures = sum(feed["failed"] for feed in result["feeds"].values())
            print(
                f"=== {result['date']} finished: exit={result['return_code']}, "
                f"failed files={failures}, log={result['log']}",
                flush=True,
            )

    # ---- mark fully successful days so the next run skips them
    for result in results:
        if result["return_code"] == 0:
            day_dir = out_dir / result["date"]
            day_dir.mkdir(parents=True, exist_ok=True)
            (day_dir / "_SUCCESS").write_text(datetime.now(timezone.utc).isoformat() + "\n")

    write_audit(results, log_dir)
    failed_files = sum(
        feed["failed"]
        for result in results
        for feed in result["feeds"].values()
    )
    failed_runs = sum(result["return_code"] != 0 for result in results)
    print(f"Audit summary: {log_dir / 'summary.csv'}", flush=True)
    print(f"Failed-file details: {log_dir / 'failed_files.csv'}", flush=True)
    if failed_files or failed_runs:
        print(
            f"Completed with {failed_files} failed input files and "
            f"{failed_runs} failed date-runs.",
            flush=True,
        )
        return 1
    print("All dates completed without reported file failures.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
