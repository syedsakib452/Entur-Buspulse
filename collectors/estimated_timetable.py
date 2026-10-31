"""Estimated timetable (SIRI ET): when each bus arrives/departs at each stop,
planned vs. actual/expected times.

Saves the raw XML response every INTERVAL seconds as
estimated_timetable/date=YYYY-MM-DD/estimated_timetable_<UTC time>.xml(.gz)
(locally under /data or in Swift, see common/storage.py)

Run: python -m collectors.estimated_timetable
"""
import re
import signal
import sys
import time

from common import config, health
from common.entur_client import EnturClient
from common.logger import get_logger
from common.storage import Storage

NAME = "estimated_timetable"
CLIENT_NAME = "uis-buspulse-estimated-timetable"
INTERVAL = 60  # seconds
UPLOAD_TIMEOUT = 45  # seconds; less than INTERVAL so a slow Swift never delays the next fetch

log = get_logger(NAME)
storage = Storage(log, upload_timeout=UPLOAD_TIMEOUT)


def collect(client: EnturClient):
    payload = client.get_siri(config.ET_URL)
    storage.save(NAME, "xml", payload)
    if re.search(rb"MoreData>\s*true\s*<", payload):
        log.error("snapshot was cut off: raise SIRI_MAX_SIZE in common/config.py")


def main():
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # docker stop -> "stopped" gets logged
    client = EnturClient(CLIENT_NAME)
    storage.start_flusher()  # swift mode: retries the fallback buffer in the background
    log.info("started")
    health.beat(NAME)
    next_run = time.monotonic()
    try:
        while True:
            try:
                collect(client)
            except Exception:
                log.exception("collect failed")
            health.beat(NAME)  # liveness probe: the loop is still going
            # Fixed rhythm: sleep only what is left of INTERVAL, so a slow fetch or upload
            # doesn't push every later fetch back. If a round overran, start the next at once.
            next_run = max(next_run + INTERVAL, time.monotonic())
            time.sleep(next_run - time.monotonic())
    finally:
        log.info("stopped")


if __name__ == "__main__":
    main()
