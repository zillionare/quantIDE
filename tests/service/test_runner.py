import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pyqmt.core.enums import FrameType
from pyqmt.core.strategy import BaseStrategy
from pyqmt.data.sqlite import db
from pyqmt.service.runner import BacktestRunner


class SimpleStrategy(BaseStrategy):
    async def init(self):
        pass
    async def on_start(self):
        pass
    async def on_stop(self):
        pass
    async def on_day_open(self, tm):
        pass
    async def on_day_close(self, tm):
        pass
    async def on_bar(self, tm, quote, frame_type):
        pass

@pytest.fixture(autouse=True)
def setup_db():
    db.init(":memory:")
    yield
    # No explicit close method in SQLiteDB, but it's fine for memory db

@pytest.mark.asyncio
async def test_run_daily():
    start_date = datetime.date(2024, 1, 1)
    end_date = datetime.date(2024, 1, 2)

    # Mock dependencies
    with patch("pyqmt.service.runner.calendar") as mock_calendar, \
         patch("pyqmt.service.runner.BacktestBroker") as MockBroker, \
         patch("pyqmt.service.runner.daily_bars") as mock_daily_bars, \
         patch("pyqmt.service.runner.db") as mock_db, \
         patch("pyqmt.service.runner.metrics") as mock_metrics:

        # Setup calendar mock
        mock_calendar.get_frames.return_value = [start_date, end_date]
        mock_calendar.replace_time.side_effect = lambda d, h, m: datetime.datetime(d.year, d.month, d.day, h, m)

        # Setup broker mock
        mock_broker_instance = MockBroker.return_value
        mock_broker_instance.stop_backtest = AsyncMock()
        mock_broker_instance.positions = {}

        # Setup strategy mock (spy)
        strategy = SimpleStrategy(mock_broker_instance, {})
        strategy.on_day_open = AsyncMock()
        strategy.on_day_close = AsyncMock()
        strategy.on_bar = AsyncMock()
        strategy.init = AsyncMock()
        strategy.on_start = AsyncMock()
        strategy.on_stop = AsyncMock()

        # Run runner
        runner = BacktestRunner()

        # Mock strategy_cls
        MockStrategyCls = MagicMock(return_value=strategy)
        MockStrategyCls.__name__ = "SimpleStrategy"

        await runner.run(MockStrategyCls, {}, start_date, end_date, frame_type=FrameType.DAY)

        # Verify calls
        # 2 days -> 2 open, 2 bars, 2 closes
        assert strategy.on_day_open.call_count == 2
        assert strategy.on_bar.call_count == 2
        assert strategy.on_day_close.call_count == 2

        # Verify arguments
        # Day 1
        open_tm1 = datetime.datetime(2024, 1, 1, 9, 30)
        bar_tm1 = datetime.datetime(2024, 1, 1, 15, 0)
        close_tm1 = datetime.datetime(2024, 1, 1, 15, 30)

        strategy.on_day_open.assert_any_call(open_tm1)
        # Note: quote is empty dict because daily_bars.get_bars_in_range mocked implicitly (returns MagicMock which is not empty, wait)
        # Actually daily_bars.get_bars_in_range returns a MagicMock by default.
        # In runner: if not df.is_empty(): ...
        # So we need to ensure df.is_empty() returns True to avoid iteration on mock, or properly mock it.
        # If we let it return True (default for bool(mock)), it might enter iteration.
        # Let's mock get_bars_in_range to return empty df-like object.
        mock_df = MagicMock()
        mock_df.is_empty.return_value = True
        mock_daily_bars.get_bars_in_range.return_value = mock_df

        strategy.on_bar.assert_any_call(bar_tm1, {}, FrameType.DAY)
        strategy.on_day_close.assert_any_call(close_tm1)

@pytest.mark.asyncio
async def test_run_minute():
    start_date = datetime.date(2024, 1, 1)
    end_date = datetime.date(2024, 1, 1) # 1 day

    # Mock dependencies
    with patch("pyqmt.service.runner.calendar") as mock_calendar, \
         patch("pyqmt.service.runner.BacktestBroker") as MockBroker, \
         patch("pyqmt.service.runner.daily_bars") as mock_daily_bars, \
         patch("pyqmt.service.runner.db") as mock_db, \
         patch("pyqmt.service.runner.metrics") as mock_metrics:

        # Setup calendar mock
        # Minute frames: 9:31, 9:32 (just 2 frames for test)
        tm1 = datetime.datetime(2024, 1, 1, 9, 31)
        tm2 = datetime.datetime(2024, 1, 1, 9, 32)
        mock_calendar.get_frames.return_value = [tm1, tm2]

        mock_calendar.first_min_frame.return_value = datetime.datetime(2024, 1, 1, 9, 31)
        mock_calendar.last_min_frame.return_value = datetime.datetime(2024, 1, 1, 15, 0)

        mock_calendar.replace_time.side_effect = lambda d, h, m: datetime.datetime(d.year, d.month, d.day, h, m)

        # Setup broker mock
        mock_broker_instance = MockBroker.return_value
        mock_broker_instance.stop_backtest = AsyncMock()
        mock_broker_instance.positions = {}

        # Setup strategy mock
        strategy = SimpleStrategy(mock_broker_instance, {})
        strategy.on_day_open = AsyncMock()
        strategy.on_day_close = AsyncMock()
        strategy.on_bar = AsyncMock()
        strategy.init = AsyncMock()
        strategy.on_start = AsyncMock()
        strategy.on_stop = AsyncMock()

        MockStrategyCls = MagicMock(return_value=strategy)
        MockStrategyCls.__name__ = "SimpleStrategy"

        runner = BacktestRunner()
        await runner.run(MockStrategyCls, {}, start_date, end_date, frame_type=FrameType.MIN1)

        # Verify calls
        # 1 day -> 1 open, 2 bars, 1 close
        assert strategy.on_day_open.call_count == 1
        assert strategy.on_bar.call_count == 2
        assert strategy.on_day_close.call_count == 1

        # Verify arguments
        open_tm = datetime.datetime(2024, 1, 1, 9, 30)
        close_tm = datetime.datetime(2024, 1, 1, 15, 30)

        strategy.on_day_open.assert_called_once_with(open_tm)
        strategy.on_day_close.assert_called_once_with(close_tm)

        strategy.on_bar.assert_any_call(tm1, {}, FrameType.MIN1)
        strategy.on_bar.assert_any_call(tm2, {}, FrameType.MIN1)
