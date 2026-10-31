"""Heartbeat for the Kubernetes liveness probe.

Each collector calls beat(NAME) at startup and after every loop round, whether the
fetch succeeded or failed (failures are already logged). If the file stops being
updated, the collector is stuck (e.g. hanging forever on a network call), and
Kubernetes restarts it. Entur or Swift being down does NOT trigger restarts.

Probe command (exits 0 = healthy, 1 = restart me):
    python -m common.health <name> <max_age_seconds>
"""
import sys
import time
from pathlib import Path

from common import config


def heartbeat_file(name: str) -> Path:
    return config.HEARTBEAT_DIR / f"heartbeat-{name}"


def beat(name: str) -> None:
    heartbeat_file(name).touch()


def is_alive(name: str, max_age: float) -> bool:
    try:
        age = time.time() - heartbeat_file(name).stat().st_mtime
    except FileNotFoundError:
        return False
    return age <= max_age


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: python -m common.health <name> <max_age_seconds>")
    sys.exit(0 if is_alive(sys.argv[1], float(sys.argv[2])) else 1)
