"""Bronze + silver for every finished day that is not in buspulse-silver yet.

First bronze for every day to do, then silver (so a day without its own timetable
can use a neighbouring day's). A failed day does not stop the others; it is retried
on the next run (finished days are skipped).
Used by the Kubernetes CronJob in k8s-tables/. Settings (environment variables
or arguments):
    START_DATE / --start   first day to process            (default 2026-10-20)
    END_DATE   / --end     last day; empty = yesterday (UTC)

Run by hand (OpenStack login loaded first):
    python tables/run_days.py --start 2026-10-20 --end 2026-10-20
"""
import argparse
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from swiftclient.exceptions import ClientException

from build_bronze_tables import swift_connection

HERE = Path(__file__).resolve().parent
SILVER_CONTAINER = os.getenv("SILVER_CONTAINER", "buspulse-silver")


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def silver_done(conn, day):
    try:
        _, objects = conn.get_container(SILVER_CONTAINER, prefix=f"stop_visits/date={day}/", limit=1)
    except ClientException as error:
        if error.http_status == 404:        # container does not exist yet
            return False
        raise
    return bool(objects)


def run(script, day):
    return subprocess.run([sys.executable, "-u", str(HERE / script), "--date", day]).returncode == 0


def main():
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", default=os.getenv("START_DATE") or "2026-10-20")
    parser.add_argument("--end", default=os.getenv("END_DATE") or yesterday)
    args = parser.parse_args()

    conn = swift_connection()
    day, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
    todo = []
    while day <= end:
        d = day.isoformat()
        if silver_done(conn, d):
            print(f"=== {d}: already in {SILVER_CONTAINER}, skipping", flush=True)
        else:
            todo.append(d)
        day += timedelta(days=1)
    print(f"=== to do: {todo or 'nothing'}", flush=True)

    # 1. bronze for all days first, so silver can borrow a neighbouring day's timetable
    failed, bronze_ok = [], []
    for d in todo:
        print(f"=== {d}: bronze ({now()})", flush=True)
        if run("build_bronze_tables.py", d):
            bronze_ok.append(d)
        else:
            failed.append(d)
            print(f"=== {d}: bronze FAILED ({now()})", flush=True)

    # 2. silver
    for d in bronze_ok:
        print(f"=== {d}: silver ({now()})", flush=True)
        ok = run("build_silver_tables.py", d)
        print(f"=== {d}: {'done' if ok else 'silver FAILED'} ({now()})", flush=True)
        if not ok:
            failed.append(d)

    print(f"=== finished, failed days: {sorted(failed) or 'none'}", flush=True)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
