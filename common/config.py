"""Shared settings for all collectors. Collector-specific settings
(client name, interval) live at the top of each collector file."""
import os
from datetime import datetime, timezone
from pathlib import Path

# Folders (mounted as volumes in compose.yaml). Override with env vars when running locally.
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/logs"))

CODESPACE = "KOL"  # Kolumbus (Rogaland)

# Entur open APIs: no key, only the ET-Client-Name header is required
ET_URL = "https://api.entur.io/realtime/v1/rest/et"
SX_URL = "https://api.entur.io/realtime/v1/rest/sx"
VEHICLES_URL = "https://api.entur.io/realtime/v2/vehicles/graphql"
GTFS_URL = "https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_kol-aggregated-gtfs.zip"

REQUEST_TIMEOUT = 60    # seconds
DOWNLOAD_TIMEOUT = 300  # seconds, for big files (GTFS zip)
SIRI_MAX_SIZE = 10000   # max elements per SIRI response

# Where collected data goes (see common/storage.py)
#   local: files under DATA_DIR (VM + Docker Compose; the Go uploader moves them to Swift)
#   swift: straight to Swift (Kubernetes), with BUFFER_DIR as fallback when an upload fails
STORAGE_MODE = os.getenv("STORAGE_MODE", "local")
SWIFT_CONTAINER = os.getenv("SWIFT_CONTAINER", "buspulse-raw")
BUFFER_DIR = Path(os.getenv("BUFFER_DIR", "/buffer"))
UPLOAD_ATTEMPTS = int(os.getenv("UPLOAD_ATTEMPTS", "3"))
BUFFER_RETRY_SECONDS = int(os.getenv("BUFFER_RETRY_SECONDS", "60"))

# Heartbeat files for the Kubernetes liveness probe (see common/health.py)
HEARTBEAT_DIR = Path(os.getenv("HEARTBEAT_DIR", "/tmp"))


def object_name(feed: str, ext: str, now: datetime | None = None) -> str:
    """Naming convention for raw data, the same locally and in Swift (UTC):
    <feed>/date=YYYY-MM-DD/<feed>_YYYYMMDDTHHMMSSZ.<ext>
    The Go uploader and the processing step depend on this layout."""
    now = now or datetime.now(timezone.utc)
    return f"{feed}/date={now:%Y-%m-%d}/{feed}_{now:%Y%m%dT%H%M%SZ}.{ext}"


def bronze_path(feed: str, ext: str, now: datetime | None = None) -> Path:
    """Local file for raw data: DATA_DIR/<object_name>. Creates the day folder."""
    path = DATA_DIR / object_name(feed, ext, now)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path