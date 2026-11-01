"""Fixed rhythm for the collector loops.

Each round should start INTERVAL seconds after the previous one, no matter how long
the fetch and upload took. So after a round, sleep only for what is left of INTERVAL.
If a round took longer than INTERVAL, start the next one at once (no catch-up burst).

Usage in a collector loop:
    next_run = time.monotonic()
    while True:
        ...one round...
        next_run = rhythm.wait_for_next_round(next_run, INTERVAL)
"""
import time


def wait_for_next_round(previous_start: float, interval: float) -> float:
    """Sleep until the next round is due and return its planned start time."""
    next_start = max(previous_start + interval, time.monotonic())
    # Time keeps moving between the two clock readings, so the remaining time can
    # come out a tiny bit negative after an overrun; sleep() refuses negative values.
    time.sleep(max(0.0, next_start - time.monotonic()))
    return next_start
