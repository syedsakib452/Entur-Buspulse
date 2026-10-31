"""Service alerts (SIRI SX): disruptions and notices published by Kolumbus.

Alerts only change when someone publishes or edits one, so the feed is checked
every INTERVAL seconds but a file is saved ONLY when the alerts changed
(and once at startup), as
service_alerts/date=YYYY-MM-DD/service_alerts_<UTC time>.xml(.gz)
(locally under /data or in Swift, see common/storage.py)

Run: python -m collectors.service_alerts
"""
import hashlib
import re
import signal
import sys
import time

from common import config, health
from common.entur_client import EnturClient
from common.logger import get_logger
from common.storage import Storage

NAME = "service_alerts"
CLIENT_NAME = "uis-buspulse-service-alerts"
INTERVAL = 60  # seconds

log = get_logger(NAME)
storage = Storage(log)


def fingerprint(payload: bytes) -> str:
    """Hash of the alerts only. The rest of the response (timestamps etc.)
    changes on every call, so it is ignored when comparing."""
    alerts = re.findall(rb"<(?:\w+:)?PtSituationElement\b.*?</(?:\w+:)?PtSituationElement>",
                        payload, re.S)
    return hashlib.sha256(b"".join(sorted(alerts))).hexdigest()


def collect(client: EnturClient, last_fingerprint: str | None) -> str:
    payload = client.get_siri(config.SX_URL)
    current = fingerprint(payload)
    if current != last_fingerprint:
        storage.save(NAME, "xml", payload)
    return current


def main():
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))  # docker stop -> "stopped" gets logged
    client = EnturClient(CLIENT_NAME)
    last_fingerprint = None
    storage.start_flusher()  # swift mode: retries the fallback buffer in the background
    log.info("started")
    health.beat(NAME)
    try:
        while True:
            try:
                last_fingerprint = collect(client, last_fingerprint)
            except Exception:
                log.exception("collect failed")
            health.beat(NAME)  # liveness probe: the loop is still going
            time.sleep(INTERVAL)
    finally:
        log.info("stopped")


if __name__ == "__main__":
    main()
