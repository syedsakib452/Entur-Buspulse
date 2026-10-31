"""Vehicle positions: where every Kolumbus bus is right now (Entur Vehicles GraphQL).

Saves the raw JSON response every INTERVAL seconds as
vehicle_positions/date=YYYY-MM-DD/vehicle_positions_<UTC time>.json(.gz)
(locally under /data or in Swift, see common/storage.py)

Run: python -m collectors.vehicle_positions
"""
import signal
import sys
import time

from common import config, health
from common.entur_client import EnturClient
from common.logger import get_logger
from common.storage import Storage

NAME = "vehicle_positions"
CLIENT_NAME = "uis-buspulse-vehicle-positions"
INTERVAL = 30  # seconds

QUERY = """
{
  vehicles(codespaceId: "%s") {
    vehicleId lastUpdated
    line { lineRef lineName publicCode }
    location { latitude longitude }
    bearing speed delay occupancyStatus
  }
}
""" % config.CODESPACE

log = get_logger(NAME)
storage = Storage(log)


def collect(client: EnturClient):
    payload = client.post_graphql(config.VEHICLES_URL, QUERY)
    storage.save(NAME, "json", payload)


def main():
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # docker stop -> "stopped" gets logged
    client = EnturClient(CLIENT_NAME)
    storage.start_flusher()  # swift mode: retries the fallback buffer in the background
    log.info("started")
    health.beat(NAME)
    try:
        while True:
            try:
                collect(client)
            except Exception:
                log.exception("collect failed")
            health.beat(NAME)  # liveness probe: the loop is still going
            time.sleep(INTERVAL)
    finally:
        log.info("stopped")


if __name__ == "__main__":
    main()
