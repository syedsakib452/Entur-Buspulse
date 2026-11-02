"""Numbers the collectors report to Prometheus, at http://<pod>:METRICS_PORT/metrics.

    buspulse_stored_total{feed, destination}          snapshots stored: swift / buffer / local / skipped
    buspulse_last_stored_timestamp_seconds{feed}      when data last reached Swift (or local disk)
    buspulse_buffer_files{feed}                       files waiting in the fallback buffer
    buspulse_buffer_uploaded_total{feed}              buffered files that reached Swift later
    buspulse_swift_down{feed}                         1 while uploads fail and new data is buffered
    buspulse_rounds_total{feed, result}               collector rounds: ok / error (fetch or save failed)

Alerts and the Grafana dashboard (monitoring/) are built on these.
METRICS_PORT=0 turns the endpoint off (e.g. for run-once jobs, which exit right away).
"""
import os
import time

from prometheus_client import Counter, Gauge, start_http_server

STORED = Counter("buspulse_stored", "Snapshots stored, by where they went",
                 ["feed", "destination"])
LAST_STORED = Gauge("buspulse_last_stored_timestamp_seconds",
                    "Unix time when data last reached Swift (or local disk)", ["feed"])
BUFFER_FILES = Gauge("buspulse_buffer_files", "Files waiting in the fallback buffer", ["feed"])
BUFFER_UPLOADED = Counter("buspulse_buffer_uploaded",
                          "Buffered files that reached Swift later", ["feed"])
SWIFT_DOWN = Gauge("buspulse_swift_down", "1 while uploads fail and new data is buffered", ["feed"])
ROUNDS = Counter("buspulse_rounds", "Collector rounds, by result (ok / error)", ["feed", "result"])


def start(feed: str) -> None:
    """Start the /metrics endpoint and set starting values for this collector."""
    # Count from the start, so "time since last stored" doesn't look years old right after
    # a restart (the first snapshot arrives within one interval).
    LAST_STORED.labels(feed).set(time.time())
    BUFFER_FILES.labels(feed).set(0)
    SWIFT_DOWN.labels(feed).set(0)
    for destination in ("swift", "buffer", "local", "skipped"):
        STORED.labels(feed, destination)  # show all series as 0 from the start
    for result in ("ok", "error"):
        ROUNDS.labels(feed, result)
    port = int(os.getenv("METRICS_PORT", "8000"))
    if port:
        start_http_server(port)


def stored(feed: str, destination: str) -> None:
    STORED.labels(feed, destination).inc()
    if destination in ("swift", "local"):
        LAST_STORED.labels(feed).set(time.time())


def buffer_uploaded(feed: str, count: int) -> None:
    BUFFER_UPLOADED.labels(feed).inc(count)
    LAST_STORED.labels(feed).set(time.time())


def buffer_files(feed: str, count: int) -> None:
    BUFFER_FILES.labels(feed).set(count)


def swift_down(feed: str, down: bool) -> None:
    SWIFT_DOWN.labels(feed).set(1 if down else 0)


def round_done(feed: str, ok: bool) -> None:
    ROUNDS.labels(feed, "ok" if ok else "error").inc()
