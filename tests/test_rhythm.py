"""Tests for common/rhythm.py. Run: pytest"""
import pytest

from common import rhythm


class MovingClock:
    """Like the real clock: every reading is a little later than the previous one,
    and sleep() moves it forward. (A frozen fake clock hid the overnight crash.)"""

    def __init__(self, start=1000.0, tick=0.001):
        self.now, self.tick, self.sleeps = start, tick, []

    def monotonic(self):
        self.now += self.tick
        return self.now

    def sleep(self, seconds):
        if seconds < 0:
            raise ValueError("sleep length must be non-negative")  # what time.sleep does
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch):
    c = MovingClock()
    monkeypatch.setattr(rhythm.time, "monotonic", c.monotonic)
    monkeypatch.setattr(rhythm.time, "sleep", c.sleep)
    return c


def test_sleeps_only_what_is_left(clock):
    start = clock.monotonic()
    clock.now += 5  # the round took 5 s
    next_start = rhythm.wait_for_next_round(start, 30)
    assert next_start == pytest.approx(start + 30)
    assert clock.sleeps[-1] == pytest.approx(25, abs=0.01)


def test_overrun_starts_next_round_at_once_without_crashing(clock):
    """The bug that restarted every collector 9 times overnight."""
    start = clock.monotonic()
    clock.now += 45  # the round took longer than the 30 s interval
    next_start = rhythm.wait_for_next_round(start, 30)
    assert clock.sleeps[-1] == 0.0  # no ValueError, no waiting
    assert next_start == pytest.approx(start + 45, abs=0.01)  # rhythm continues from now


def test_many_rounds_keep_a_steady_rhythm(clock):
    starts = [clock.monotonic()]
    for work in [1, 21, 1, 45, 1, 1]:
        clock.now += work
        starts.append(rhythm.wait_for_next_round(starts[-1], 30))
    gaps = [round(b - a) for a, b in zip(starts, starts[1:])]
    assert gaps == [30, 30, 30, 45, 30, 30]
