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


def bronze_path(feed: str, ext: str) -> Path:
    """Naming convention for raw files:
    /data/<feed>/date=YYYY-MM-DD/<feed>_YYYYMMDDTHHMMSSZ.<ext>   (UTC)"""
    now = datetime.now(timezone.utc)
    folder = DATA_DIR / feed / f"date={now:%Y-%m-%d}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{feed}_{now:%Y%m%dT%H%M%SZ}.{ext}"