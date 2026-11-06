"""Run the Swift bronze builder over a date range with bounded parallelism.

Example:
    python run_bronze_range.py --start 2026-10-22 --end 2026-11-05 --workers 4
"""
import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
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
SPARK_FEED_SUMMARY = re.compile(
    r"^([a-z_]+): ([\d,]+) files -> [\d,]+ rows \((\d+) unreadable files\)"
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


def run_day(day, log_dir, spark):
    log_path = log_dir / f"{day}.log"
    env = os.environ.copy()
    if spark:
        paths = (str(HERE), str(HERE.parent / "silver-layer"))
        env["PYTHONPATH"] = os.pathsep.join(paths + ((env["PYTHONPATH"],) if env.get("PYTHONPATH") else ()))
        env.setdefault("SPARK_LOCAL_DIR", f"/tmp/buspulse-spark/{day}")
        command = [sys.executable, "-u", "-m", "spark_jobs.bronze", "--date", day]
    else:
        command = [sys.executable, "-u", str(HERE / "build_bronze_tables.py"), "--date", day]
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

            match = SPARK_FEED_SUMMARY.match(line.rstrip("\n"))
            if match:
                feed = match.group(1)
                feed_counts.setdefault(feed, {"listed": int(match.group(2).replace(",", ""))})
                feed_counts[feed]["processed"] = (
                    feed_counts[feed]["listed"] - int(match.group(3))
                )
                feed_counts[feed]["failed"] = int(match.group(3))

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
    parser.add_argument("--start", required=True, help="first UTC date, YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="last UTC date, YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=4, help="maximum dates running at once")
    parser.add_argument(
        "--spark", action="store_true",
        help="run spark_jobs.bronze instead of the standalone Swift bronze script",
    )
    parser.add_argument(
        "--log-dir", default=str(Path.home() / "buspulse-bronze-audit"),
        help="directory for per-day logs and audit CSV files",
    )
    args = parser.parse_args()
    if args.workers < 1:
        parser.error("--workers must be at least 1")

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
        f"({'Spark bronze' if args.spark else 'standalone bronze'}). Logs/audit: {log_dir}",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_day, day, log_dir, args.spark): day for day in days}
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            failures = sum(feed["failed"] for feed in result["feeds"].values())
            print(
                f"=== {result['date']} finished: exit={result['return_code']}, "
                f"failed files={failures}, log={result['log']}",
                flush=True,
            )

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
