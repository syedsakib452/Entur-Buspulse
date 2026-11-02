"""The collectors' Prometheus numbers follow what Storage does. Run: pytest"""
import logging
import time
from datetime import datetime, timedelta, timezone

import pytest
from prometheus_client import REGISTRY

from common import config, metrics
from common.storage import Storage

LOG = logging.getLogger("test")
NOW = datetime(2026, 11, 2, 12, 0, 0, tzinfo=timezone.utc)
FEED = "vehicle_positions"


class FakeSwift:
    def __init__(self, down=False):
        self.down, self.objects = down, {}

    def put(self, name, data):
        if self.down:
            raise ConnectionError("Swift unreachable")
        self.objects[name] = data


def value(name, **labels):
    return REGISTRY.get_sample_value(name, labels) or 0.0


@pytest.fixture(autouse=True)
def settings(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "BUFFER_DIR", tmp_path / "buffer")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(config, "STORAGE_MODE", "swift")
    monkeypatch.setenv("METRICS_PORT", "0")  # no HTTP server in tests
    metrics.start(FEED)


def test_direct_upload_counts_and_updates_last_stored():
    before = value("buspulse_stored_total", feed=FEED, destination="swift")
    t0 = time.time()
    Storage(LOG, target=FakeSwift()).save(FEED, "json", b"1", now=NOW)
    assert value("buspulse_stored_total", feed=FEED, destination="swift") == before + 1
    assert value("buspulse_last_stored_timestamp_seconds", feed=FEED) >= t0


def test_outage_and_recovery_are_visible():
    swift = FakeSwift(down=True)
    store = Storage(LOG, target=swift)
    buffered_before = value("buspulse_stored_total", feed=FEED, destination="buffer")
    uploaded_before = value("buspulse_buffer_uploaded_total", feed=FEED)

    store.save(FEED, "json", b"1", now=NOW)
    store.save(FEED, "json", b"2", now=NOW + timedelta(seconds=30))
    assert value("buspulse_swift_down", feed=FEED) == 1
    assert value("buspulse_buffer_files", feed=FEED) == 2
    assert value("buspulse_stored_total", feed=FEED, destination="buffer") == buffered_before + 2

    swift.down = False
    store.flush_buffer()
    assert value("buspulse_swift_down", feed=FEED) == 0
    assert value("buspulse_buffer_files", feed=FEED) == 0
    assert value("buspulse_buffer_uploaded_total", feed=FEED) == uploaded_before + 2


def test_skipped_snapshots_are_counted():
    before = value("buspulse_stored_total", feed=FEED, destination="skipped")
    store = Storage(LOG, target=FakeSwift())
    store.save(FEED, "json", b"1", now=NOW)
    store.save(FEED, "json", b"2", now=NOW)  # same second
    assert value("buspulse_stored_total", feed=FEED, destination="skipped") == before + 1


def test_rounds():
    ok, err = value("buspulse_rounds_total", feed=FEED, result="ok"), value("buspulse_rounds_total", feed=FEED, result="error")
    metrics.round_done(FEED, ok=True)
    metrics.round_done(FEED, ok=False)
    assert value("buspulse_rounds_total", feed=FEED, result="ok") == ok + 1
    assert value("buspulse_rounds_total", feed=FEED, result="error") == err + 1


def test_metrics_endpoint_serves_the_numbers(monkeypatch):
    """The HTTP endpoint Prometheus scrapes actually answers with our metrics."""
    import socket
    import urllib.request

    s = socket.socket(); s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]; s.close()
    monkeypatch.setenv("METRICS_PORT", str(port))
    metrics.start(FEED)
    body = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
    assert 'buspulse_last_stored_timestamp_seconds{feed="vehicle_positions"}' in body
    assert "buspulse_buffer_files" in body
