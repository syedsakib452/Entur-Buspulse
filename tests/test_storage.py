"""Tests for common/storage.py and common/health.py. Run: pytest"""
import gzip
import logging
import os
import socket
import threading
import time
from datetime import datetime, timedelta, timezone

import pytest

from common import config, health
from common.storage import Storage, SwiftTarget

NOW = datetime(2026, 10, 31, 10, 5, 30, tzinfo=timezone.utc)
LOG = logging.getLogger("test")


class FakeSwift:
    """Stands in for Swift. Set .down = True to make every upload fail."""

    def __init__(self, down=False):
        self.objects = {}
        self.down = down
        self.calls = 0

    def put(self, name, data):
        self.calls += 1
        if self.down:
            raise ConnectionError("Swift unreachable")
        self.objects[name] = data


@pytest.fixture(autouse=True)
def settings(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(config, "BUFFER_DIR", tmp_path / "buffer")
    monkeypatch.setattr(config, "HEARTBEAT_DIR", tmp_path)
    monkeypatch.setattr(config, "STORAGE_MODE", "swift")


def buffered_files():
    if not config.BUFFER_DIR.exists():
        return []
    return sorted(p.relative_to(config.BUFFER_DIR).as_posix()
                  for p in config.BUFFER_DIR.rglob("*") if p.is_file())


def name(feed, ext, when=NOW):
    return config.object_name(feed, ext, when)


def test_object_name_layout():
    assert name("vehicle_positions", "json.gz") == \
        "vehicle_positions/date=2026-10-31/vehicle_positions_20261031T100530Z.json.gz"


def test_local_mode_writes_raw_file_as_before(monkeypatch):
    monkeypatch.setattr(config, "STORAGE_MODE", "local")
    assert Storage(LOG).save("estimated_timetable", "xml", b"<Siri/>", now=NOW) == "local"
    path = config.DATA_DIR / "estimated_timetable/date=2026-10-31/estimated_timetable_20261031T100530Z.xml"
    assert path.read_bytes() == b"<Siri/>"  # uncompressed, same as before


def test_uploads_gzipped_with_same_name():
    swift = FakeSwift()
    assert Storage(LOG, target=swift).save("vehicle_positions", "json", b'{"a":1}', now=NOW) == "swift"
    assert gzip.decompress(swift.objects[name("vehicle_positions", "json.gz")]) == b'{"a":1}'
    assert buffered_files() == []


def test_zip_is_not_gzipped_again():
    swift = FakeSwift()
    Storage(LOG, target=swift).save("static_timetable", "zip", b"PK..", now=NOW)
    assert swift.objects == {name("static_timetable", "zip"): b"PK.."}


def test_one_try_then_buffer():
    swift = FakeSwift(down=True)
    assert Storage(LOG, target=swift).save("vehicle_positions", "json", b"1", now=NOW) == "buffer"
    assert swift.calls == 1  # no repeated waiting for a Swift that is down
    assert buffered_files() == [name("vehicle_positions", "json.gz")]


def test_while_buffer_has_files_new_data_skips_swift():
    swift = FakeSwift(down=True)
    store = Storage(LOG, target=swift)
    store.save("vehicle_positions", "json", b"1", now=NOW)
    swift.calls = 0
    store.save("vehicle_positions", "json", b"2", now=NOW + timedelta(seconds=30))
    assert swift.calls == 0  # went straight to the buffer, no timeout to wait for
    assert len(buffered_files()) == 2


def test_flush_uploads_oldest_first_then_direct_resumes():
    swift = FakeSwift(down=True)
    store = Storage(LOG, target=swift)
    t1, t2 = NOW, NOW + timedelta(seconds=30)
    store.save("vehicle_positions", "json", b"1", now=t1)
    store.save("vehicle_positions", "json", b"2", now=t2)

    swift.down = False  # Swift is back
    uploaded = []
    swift.put = lambda n, d: (uploaded.append(n), swift.objects.__setitem__(n, d))
    assert store.flush_buffer() == 2
    assert uploaded == [name("vehicle_positions", "json.gz", t1), name("vehicle_positions", "json.gz", t2)]
    assert buffered_files() == []

    t3 = NOW + timedelta(seconds=60)
    assert store.save("vehicle_positions", "json", b"3", now=t3) == "swift"  # direct again


def test_flush_keeps_files_while_swift_is_down():
    store = Storage(LOG, target=FakeSwift(down=True))
    store.save("vehicle_positions", "json", b"1", now=NOW)
    assert store.flush_buffer() == 0
    assert len(buffered_files()) == 1  # still waiting, nothing lost


def test_no_fallback_raises_and_ignores_buffer():
    with pytest.raises(RuntimeError):
        Storage(LOG, target=FakeSwift(down=True)).save("static_timetable", "zip", b"PK", fallback=False)
    assert buffered_files() == []


def test_unknown_mode_is_rejected(monkeypatch):
    monkeypatch.setattr(config, "STORAGE_MODE", "s3")
    with pytest.raises(ValueError):
        Storage(LOG)


def test_stalled_swift_times_out_instead_of_hanging():
    """A server that accepts the connection but never answers (what a stalled
    upload looks like): the upload must give up after the timeout."""
    import openstack

    srv = socket.socket()
    srv.bind(("127.0.0.1", 0))
    srv.listen(5)
    port = srv.getsockname()[1]
    open_conns = []
    threading.Thread(target=lambda: [open_conns.append(srv.accept()) for _ in range(5)],
                     daemon=True).start()

    target = SwiftTarget("buspulse-raw-test", timeout=1)
    target._conn = openstack.connect(  # real openstacksdk connection, test server instead of Keystone
        auth_type="admin_token", api_timeout=1,
        auth={"endpoint": f"http://127.0.0.1:{port}/v1/AUTH_test", "token": "test"})
    store = Storage(LOG, target=target)

    start = time.monotonic()
    assert store.save("vehicle_positions", "json", b"1", now=NOW) == "buffer"
    assert time.monotonic() - start < 10
    assert len(buffered_files()) == 1
    srv.close()


class SwiftHTTP:
    """Tiny HTTP server playing Swift: answers every upload with `status`."""

    def __init__(self, status, etag=None):
        import http.server

        outer = self
        self.status, self.etag, self.received = status, etag, []

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_PUT(self):
                body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                outer.received.append((self.path, body))
                self.send_response(outer.status)
                if outer.etag == "md5-of-body":  # like real Swift: fingerprint of what it received
                    import hashlib
                    self.send_header("Etag", hashlib.md5(body).hexdigest())
                elif outer.etag:
                    self.send_header("Etag", outer.etag)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args):
                pass

        self.server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def target(self):
        import openstack

        target = SwiftTarget("buspulse-raw-test", timeout=5)
        target._conn = openstack.connect(  # real openstacksdk connection, test server instead of Keystone
            auth_type="admin_token", api_timeout=5,
            auth={"endpoint": f"http://127.0.0.1:{self.server.server_port}/v1/AUTH_test", "token": "test"})
        return target

    def close(self):
        self.server.shutdown()


@pytest.mark.parametrize("status", [400, 401, 403, 404, 500, 503])
def test_http_error_from_swift_goes_to_buffer(status):
    """Swift rejecting an upload (e.g. 404 missing container, 400 from the proxy)
    must NOT count as success: the snapshot has to end up in the buffer."""
    swift = SwiftHTTP(status)
    try:
        store = Storage(LOG, target=swift.target())
        assert store.save("vehicle_positions", "json", b"1", now=NOW) == "buffer"
        assert len(swift.received) == 1
        assert buffered_files() == [name("vehicle_positions", "json.gz")]
    finally:
        swift.close()


def test_http_201_with_matching_etag_is_success():
    data = b'{"a":1}'
    swift = SwiftHTTP(201, etag="md5-of-body")
    try:
        assert Storage(LOG, target=swift.target()).save("vehicle_positions", "json", data, now=NOW) == "swift"
        path, body = swift.received[0]
        assert path.endswith("/buspulse-raw-test/vehicle_positions/date%3D2026-10-31/"
                             "vehicle_positions_20261031T100530Z.json.gz")
        assert gzip.decompress(body) == data
        assert buffered_files() == []
    finally:
        swift.close()


def test_http_201_with_wrong_etag_goes_to_buffer():
    swift = SwiftHTTP(201, etag="0" * 32)  # Swift stored something else than we sent
    try:
        assert Storage(LOG, target=swift.target()).save("vehicle_positions", "json", b"1", now=NOW) == "buffer"
    finally:
        swift.close()


def test_heartbeat():
    assert not health.is_alive("vehicle_positions", 60)  # no heartbeat yet
    health.beat("vehicle_positions")
    assert health.is_alive("vehicle_positions", 60)
    old = time.time() - 600
    os.utime(health.heartbeat_file("vehicle_positions"), (old, old))
    assert not health.is_alive("vehicle_positions", 60)  # stuck for 10 min
