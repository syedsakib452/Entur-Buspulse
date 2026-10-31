"""Tests for common/storage.py and common/health.py. Run: pytest"""
import gzip
import logging
import os
import time
from datetime import datetime, timezone

import pytest

from common import config, health, storage as storage_module
from common.storage import Storage

NOW = datetime(2026, 10, 31, 10, 5, 30, tzinfo=timezone.utc)
LOG = logging.getLogger("test")


class FakeSwift:
    """Stands in for Swift. fail_times: how many puts fail before they start working."""

    def __init__(self, fail_times=0):
        self.objects = {}
        self.fail_times = fail_times
        self.calls = 0

    def put(self, name, data):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise ConnectionError("Swift unreachable")
        self.objects[name] = data


@pytest.fixture(autouse=True)
def settings(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(config, "BUFFER_DIR", tmp_path / "buffer")
    monkeypatch.setattr(config, "HEARTBEAT_DIR", tmp_path)
    monkeypatch.setattr(config, "UPLOAD_ATTEMPTS", 3)
    monkeypatch.setattr(storage_module.time, "sleep", lambda s: None)  # no waiting between retries


def buffered_files():
    return sorted(p.relative_to(config.BUFFER_DIR).as_posix()
                  for p in config.BUFFER_DIR.rglob("*") if p.is_file())


def test_object_name_layout():
    assert config.object_name("vehicle_positions", "json.gz", NOW) == \
        "vehicle_positions/date=2026-10-31/vehicle_positions_20261031T100530Z.json.gz"


def test_local_mode_writes_raw_file_as_before(monkeypatch):
    monkeypatch.setattr(config, "STORAGE_MODE", "local")
    assert Storage(LOG).save("estimated_timetable", "xml", b"<Siri/>", now=NOW) == "local"
    path = config.DATA_DIR / "estimated_timetable/date=2026-10-31/estimated_timetable_20261031T100530Z.xml"
    assert path.read_bytes() == b"<Siri/>"  # uncompressed, same as before


def test_swift_mode_uploads_gzipped_with_same_name(monkeypatch):
    monkeypatch.setattr(config, "STORAGE_MODE", "swift")
    swift = FakeSwift()
    assert Storage(LOG, target=swift).save("vehicle_positions", "json", b'{"a":1}', now=NOW) == "swift"
    name = "vehicle_positions/date=2026-10-31/vehicle_positions_20261031T100530Z.json.gz"
    assert gzip.decompress(swift.objects[name]) == b'{"a":1}'
    assert buffered_files() == []


def test_zip_is_not_gzipped_again(monkeypatch):
    monkeypatch.setattr(config, "STORAGE_MODE", "swift")
    swift = FakeSwift()
    Storage(LOG, target=swift).save("static_timetable", "zip", b"PK..", now=NOW)
    assert swift.objects == {"static_timetable/date=2026-10-31/static_timetable_20261031T100530Z.zip": b"PK.."}


def test_retry_succeeds_without_buffer(monkeypatch):
    monkeypatch.setattr(config, "STORAGE_MODE", "swift")
    swift = FakeSwift(fail_times=2)  # third attempt works
    assert Storage(LOG, target=swift).save("service_alerts", "xml", b"<x/>", now=NOW) == "swift"
    assert buffered_files() == []


def test_failed_upload_goes_to_buffer_then_flushes(monkeypatch):
    monkeypatch.setattr(config, "STORAGE_MODE", "swift")
    swift = FakeSwift(fail_times=3)  # all 3 attempts fail
    store = Storage(LOG, target=swift)
    assert store.save("vehicle_positions", "json", b'{"a":1}', now=NOW) == "buffer"
    name = "vehicle_positions/date=2026-10-31/vehicle_positions_20261031T100530Z.json.gz"
    assert buffered_files() == [name]  # same name as it would have had in Swift

    assert store.flush_buffer() == 1  # Swift is back
    assert gzip.decompress(swift.objects[name]) == b'{"a":1}'
    assert buffered_files() == []


def test_flush_stops_while_swift_is_down(monkeypatch):
    monkeypatch.setattr(config, "STORAGE_MODE", "swift")
    swift = FakeSwift(fail_times=100)
    store = Storage(LOG, target=swift)
    store.save("vehicle_positions", "json", b"1", now=NOW)
    assert store.flush_buffer() == 0
    assert len(buffered_files()) == 1  # still waiting, nothing lost


def test_no_fallback_raises(monkeypatch):
    monkeypatch.setattr(config, "STORAGE_MODE", "swift")
    with pytest.raises(RuntimeError):
        Storage(LOG, target=FakeSwift(fail_times=3)).save("static_timetable", "zip", b"PK", fallback=False)
    assert buffered_files() == []


def test_unknown_mode_is_rejected(monkeypatch):
    monkeypatch.setattr(config, "STORAGE_MODE", "s3")
    with pytest.raises(ValueError):
        Storage(LOG)


def test_heartbeat():
    assert not health.is_alive("vehicle_positions", 60)  # no heartbeat yet
    health.beat("vehicle_positions")
    assert health.is_alive("vehicle_positions", 60)
    old = time.time() - 600
    os.utime(health.heartbeat_file("vehicle_positions"), (old, old))
    assert not health.is_alive("vehicle_positions", 60)  # stuck for 10 min
