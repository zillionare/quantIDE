import datetime

import polars as pl
import pytest

from pyqmt.core.enums import FrameType, OrderSide
from pyqmt.data.sqlite import db
from pyqmt.service import backtest_broker as backtest_broker_module
from pyqmt.service import runner as runner_module
from pyqmt.service.runner import BacktestRunner
from pyqmt.strategies.example.dual_ma import DualMAStrategy


class StaticDailyFeed:
    def __init__(self, rows: list[dict]):
        self._bars = pl.DataFrame(rows).sort("date")

    def _filter(self, start=None, end=None, assets=None) -> pl.DataFrame:
        df = self._bars
        if assets:
            df = df.filter(pl.col("asset").is_in(assets))
        if start is not None:
            start_date = start.date() if isinstance(start, datetime.datetime) else start
            df = df.filter(pl.col("date") >= start_date)
        if end is not None:
            end_date = end.date() if isinstance(end, datetime.datetime) else end
            df = df.filter(pl.col("date") <= end_date)
        return df.sort("date")

    def get_bars_in_range(
        self,
        start: datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime | None = None,
        assets: list[str] | None = None,
        adjust: str | None = "qfq",
        eager_mode: bool = True,
    ) -> pl.DataFrame:
        _ = adjust
        _ = eager_mode
        return self._filter(start=start, end=end, assets=assets)

    def get_bars(
        self,
        n: int,
        end: datetime.date | datetime.datetime | None = None,
        assets: list[str] | None = None,
        adjust: str | None = "qfq",
        eager_mode: bool = True,
    ) -> pl.DataFrame:
        _ = adjust
        _ = eager_mode
        return self._filter(end=end, assets=assets).tail(n)

    def get_price_for_match(self, asset: str, tm: datetime.datetime) -> pl.DataFrame:
        return self._filter(start=tm.date(), end=tm.date(), assets=[asset])

    def get_trade_price_limits(self, asset: str, dt: datetime.date) -> tuple[float, float]:
        df = self._filter(start=dt, end=dt, assets=[asset])
        if df.is_empty():
            return 0.0, 0.0
        row = df.row(0, named=True)
        return float(row["down_limit"]), float(row["up_limit"])

    def get_close_adjust_factor(
        self,
        assets: list[str],
        start: datetime.date,
        end: datetime.date,
    ) -> pl.DataFrame:
        return self._filter(start=start, end=end, assets=assets).select(
            pl.col("date"),
            pl.col("asset"),
            pl.col("close"),
            pl.col("adjust"),
        )


@pytest.mark.asyncio
async def test_dual_ma_strategy_executes_buy_and_sell_in_backtest(calendar, monkeypatch):
    db.init(":memory:")
    feed = StaticDailyFeed(
        [
            {
                "date": datetime.date(2024, 1, 2),
                "asset": "000001.SZ",
                "open": 10.0,
                "close": 10.0,
                "up_limit": 11.0,
                "down_limit": 9.0,
                "volume": 10000.0,
                "adjust": 1.0,
            },
            {
                "date": datetime.date(2024, 1, 3),
                "asset": "000001.SZ",
                "open": 10.0,
                "close": 10.0,
                "up_limit": 11.0,
                "down_limit": 9.0,
                "volume": 10000.0,
                "adjust": 1.0,
            },
            {
                "date": datetime.date(2024, 1, 4),
                "asset": "000001.SZ",
                "open": 10.0,
                "close": 10.0,
                "up_limit": 11.0,
                "down_limit": 9.0,
                "volume": 10000.0,
                "adjust": 1.0,
            },
            {
                "date": datetime.date(2024, 1, 5),
                "asset": "000001.SZ",
                "open": 12.0,
                "close": 12.0,
                "up_limit": 13.0,
                "down_limit": 11.0,
                "volume": 10000.0,
                "adjust": 1.0,
            },
            {
                "date": datetime.date(2024, 1, 8),
                "asset": "000001.SZ",
                "open": 8.0,
                "close": 8.0,
                "up_limit": 9.0,
                "down_limit": 7.0,
                "volume": 10000.0,
                "adjust": 1.0,
            },
        ]
    )
    monkeypatch.setattr(runner_module, "daily_bars", feed)
    monkeypatch.setattr(backtest_broker_module, "daily_bars", feed)

    runner = BacktestRunner()
    result = await runner.run(
        DualMAStrategy,
        {
            "symbol": "000001.SZ",
            "fast": 1,
            "slow": 2,
            "invest": 100000,
            "universe": ["000001.SZ"],
        },
        start_date=datetime.date(2024, 1, 5),
        end_date=datetime.date(2024, 1, 8),
        frame_type=FrameType.DAY,
        initial_cash=200000,
        portfolio_id="dual-ma-example",
    )

    assert result["portfolio_id"] == "dual-ma-example"

    trades = db.trades_all(portfolio_id="dual-ma-example")
    assert trades is not None
    assert trades.height == 2
    assert trades["asset"].to_list() == ["000001.SZ", "000001.SZ"]
    assert trades["side"].to_list() == [OrderSide.BUY.value, OrderSide.SELL.value]

    orders = db.orders_all(portfolio_id="dual-ma-example")
    assert orders.height == 2