import datetime

import pytest

from quantide.core.enums import FrameType
from quantide.core.runtime.clock_bridge import BacktestClockAdapter, SystemClockAdapter


def test_backtest_clock_set_now_and_now():
    clock = BacktestClockAdapter()
    tm = datetime.datetime(2024, 1, 2, 9, 30, 0)
    clock.set_now(tm)
    assert clock.now() == tm


def test_backtest_clock_iter_frames_returns_iterable():
    clock = BacktestClockAdapter()
    frames = clock.iter_frames(
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 5),
        FrameType.DAY,
    )
    assert hasattr(frames, "__iter__")


def test_system_clock_set_now_raises():
    clock = SystemClockAdapter()
    with pytest.raises(RuntimeError):
        clock.set_now(datetime.datetime.now())
