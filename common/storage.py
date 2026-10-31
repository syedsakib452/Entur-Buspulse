"""Where collected data goes. Every collector calls Storage.save() instead of writing files itself.

STORAGE_MODE=local (default, VM + Docker Compose)
    Saves the raw response as a file under DATA_DIR, exactly as before.
    The Go uploader moves it to Swift at night.

STORAGE_MODE=swift (Kubernetes)
    gzips the response (zip files are kept as they are) and uploads it straight to Swift,
    with the same object name the uploader uses, e.g.
        vehicle_positions/date=2026-10-31/vehicle_positions_20261031T100530Z.json.gz
    Each upload is tried UPLOAD_ATTEMPTS times. Swift checks the MD5 we send, so a
    successful upload is a byte-for-byte copy.
    If all attempts fail, the file is saved in BUFFER_DIR (the fallback folder) and a
    background thread retries everything in it every BUFFER_RETRY_SECONDS. Buffered
    files keep their original name, so they land in the right day folder in Swift.
"""
import gzip
import hashlib
import logging
import threading
import time
from datetime import datetime
from urllib.parse import quote

from common import config


class SwiftTarget:
    """Uploads objects to one Swift container. Logs in with the usual OS_* environment
    variables (password or application credential), the same as openstack.env."""

    def __init__(self, container: str):
        self.container = container
        self._conn = None
        self._lock = threading.Lock()  # the collector and the buffer thread share one connection

    def _connection(self):
        if self._conn is None:
            import openstack  # only needed in swift mode

            self._conn = openstack.connect()
        return self._conn

    def put(self, name: str, data: bytes) -> None:
        md5 = hashlib.md5(data).hexdigest()
        content_type = "application/zip" if name.endswith(".zip") else "application/gzip"
        with self._lock:
            try:
                resp = self._connection().object_store.put(
                    f"{quote(self.container)}/{quote(name, safe='/')}",
                    data=data,
                    headers={"ETag": md5, "Content-Type": content_type},
                )
            except Exception:
                self._conn = None  # log in again next time (expired token, network change, ...)
                raise
        etag = resp.headers.get("Etag", "").strip('"')
        if etag and etag != md5:
            raise RuntimeError(f"checksum mismatch for {name}: sent {md5}, Swift has {etag}")


class Storage:
    def __init__(self, log: logging.Logger, target=None):
        self.log = log
        self.mode = config.STORAGE_MODE
        if self.mode not in ("local", "swift"):
            raise ValueError(f"STORAGE_MODE must be 'local' or 'swift', not {self.mode!r}")
        self.target = target or (SwiftTarget(config.SWIFT_CONTAINER) if self.mode == "swift" else None)

    def save(self, feed: str, ext: str, payload: bytes,
             fallback: bool = True, now: datetime | None = None) -> str:
        """Store one raw response. Returns where it went: "local", "swift" or "buffer".
        With fallback=False a failed upload raises instead of going to the buffer
        (used by run-once jobs, whose buffer would disappear when they exit)."""
        if self.mode == "local":
            config.bronze_path(feed, ext, now).write_bytes(payload)
            return "local"

        if ext == "zip":  # already compressed
            data = payload
        else:
            data, ext = gzip.compress(payload), ext + ".gz"
        name = config.object_name(feed, ext, now)

        error = None
        for attempt in range(1, config.UPLOAD_ATTEMPTS + 1):
            try:
                self.target.put(name, data)
                return "swift"
            except Exception as e:  # noqa: BLE001 - any failure means: try again / buffer
                error = e
                if attempt < config.UPLOAD_ATTEMPTS:
                    time.sleep(2 * attempt)  # 2 s, 4 s, ...

        if not fallback:
            raise RuntimeError(f"upload of {name} failed: {error}") from error
        self._to_buffer(name, data)
        self.log.error("upload of %s failed (%s), saved to buffer", name, error)
        return "buffer"

    def _to_buffer(self, name: str, data: bytes) -> None:
        path = config.BUFFER_DIR / name
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")  # write fully, then rename: never half a file
        tmp.write_bytes(data)
        tmp.replace(path)

    def flush_buffer(self) -> int:
        """Upload everything waiting in the buffer, oldest first. Stops at the first
        failure (Swift is probably still unreachable). Returns how many were uploaded."""
        if self.mode != "swift" or not config.BUFFER_DIR.exists():
            return 0
        files = sorted(p for p in config.BUFFER_DIR.rglob("*")
                       if p.is_file() and not p.name.endswith(".tmp"))
        sent = 0
        for path in files:
            name = path.relative_to(config.BUFFER_DIR).as_posix()
            try:
                self.target.put(name, path.read_bytes())
            except Exception as e:  # noqa: BLE001
                self.log.error("buffer: %d file(s) still waiting, upload failed (%s)",
                               len(files) - sent, e)
                break
            path.unlink()
            sent += 1
        if sent:
            self.log.info("buffer: uploaded %d file(s)", sent)
        return sent

    def start_flusher(self) -> None:
        """Retry the buffer at startup and then every BUFFER_RETRY_SECONDS, in the background."""
        if self.mode != "swift":
            return

        def loop():
            while True:
                try:
                    self.flush_buffer()
                except Exception:  # noqa: BLE001 - never let the retry thread die
                    self.log.exception("buffer flush failed")
                time.sleep(config.BUFFER_RETRY_SECONDS)

        threading.Thread(target=loop, name="buffer-flusher", daemon=True).start()
