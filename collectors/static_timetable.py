"""Static timetable (GTFS): the planned schedule — stops, routes, trips, stop times.
Needed to give names and coordinates to the IDs in the real-time feeds.

Saves the raw GTFS zip as
static_timetable/date=YYYY-MM-DD/static_timetable_<UTC time>.zip
(locally under /data or in Swift, see common/storage.py)

Two ways to run:
  default         download at startup, then every INTERVAL (VM + Docker Compose)
  RUN_ONCE=true   download once and exit, 0 = ok / 1 = failed (Kubernetes CronJob;
                  Kubernetes retries a failed run, so there is no fallback buffer here)

Run: python -m collectors.static_timetable
"""
import os
import signal
import sys
import time

from common import config
from common.entur_client import EnturClient
from common.logger import get_logger
from common.storage import Storage

NAME = "static_timetable"
CLIENT_NAME = "uis-buspulse-static-timetable"
INTERVAL = 24 * 60 * 60  # once a day
RUN_ONCE = os.getenv("RUN_ONCE", "false") == "true"

log = get_logger(NAME)
storage = Storage(log)


def collect(client: EnturClient, fallback: bool = True):
    payload = client.download(config.GTFS_URL)
    storage.save(NAME, "zip", payload, fallback=fallback)


def run_once(client: EnturClient) -> int:
    log.info("started")
    try:
        collect(client, fallback=False)
        return 0
    except Exception:
        log.exception("collect failed")
        return 1
    finally:
        log.info("stopped")


def main():
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # docker stop -> "stopped" gets logged
    client = EnturClient(CLIENT_NAME)
    if RUN_ONCE:
        sys.exit(run_once(client))

    storage.start_flusher()  # swift mode: retries the fallback buffer in the background
    log.info("started")
    try:
        while True:
            try:
                collect(client)
            except Exception:
                log.exception("collect failed")
            time.sleep(INTERVAL)
    finally:
        log.info("stopped")


if __name__ == "__main__":
    main()
