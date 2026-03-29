import asyncio
import datetime
from unittest.mock import patch

import pytest

from quantide.core.domain import QuoteSnapshot
from quantide.core.enums import OrderStatus
from quantide.data.sqlite import db
from quantide.service.metrics import metrics
from quantide.service.sim_broker import PaperBroker, SimulationBroker


class DummyMarketData:
    def __init__(self):
        self._quotes: dict[str, QuoteSnapshot] = {}

    def set_quote(self, symbol: str, price: float, volume: float | None = None):
        self._quotes[symbol] = QuoteSnapshot(
            symbol=symbol,
            price=price,
            open=price,
            high=price,
            low=price,
            volume=volume,
            amount=(price * volume) if volume is not None else None,
            ts=datetime.datetime.now(),
        )

    def snapshot(self, symbols: list[str]) -> dict[str, QuoteSnapshot]:
        return {
            symbol: self._quotes[symbol]
            for symbol in symbols
            if symbol in self._quotes
        }


async def publish_market(
    brokers: list[PaperBroker],
    asset: str,
    price: float,
    *,
    volume: float | None = None,
    up_limit: float | None = None,
    down_limit: float | None = None,
):
    for broker in brokers:
        market_data = broker._market_data
        if isinstance(market_data, DummyMarketData):
            market_data.set_quote(asset, price, volume)
    quote = {asset: {"lastPrice": price, "amount": 1_000_000.0}}
    if volume is not None:
        quote[asset]["volume"] = volume
    for broker in brokers:
        if up_limit is not None or down_limit is not None:
            broker._on_limit_update(
                {
                    asset: {
                        "up": float(up_limit or 0),
                        "down": float(down_limit or 0),
                    }
                }
            )
        broker._on_quote_update(quote)
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_paper_broker_persistence_and_recovery():
    db.init(":memory:")
    portfolio_id = "test_persist_paper"
    asset = "000001.SZ"

    with patch.object(SimulationBroker, "_get_today", return_value=datetime.date(2024, 1, 2)):
        broker1 = SimulationBroker(
            portfolio_id,
            principal=100000,
            market_data=DummyMarketData(),
        )

    await publish_market([broker1], asset, 10.0, volume=1000, up_limit=11.0, down_limit=9.0)
    task = asyncio.create_task(broker1.buy(asset, 100, price=10.0, timeout=1.0))
    await asyncio.sleep(0.05)
    await publish_market([broker1], asset, 10.0, volume=1000)
    result = await task

    assert len(result.trades or []) == 1
    trades_df = db.query_trade(qtoid=result.qt_oid)
    assert len(trades_df) == 1

    with patch.object(SimulationBroker, "_get_today", return_value=datetime.date(2024, 1, 2)):
        broker2 = SimulationBroker.load(portfolio_id, market_data=DummyMarketData())

    assert asset in broker2._positions
    assert broker2._positions[asset].shares == 100


@pytest.mark.asyncio
async def test_paper_broker_concurrent_accounts_share_quote_feed():
    db.init(":memory:")
    broker1 = SimulationBroker("paper-p1", principal=100000, market_data=DummyMarketData())
    broker2 = SimulationBroker("paper-p2", principal=100000, market_data=DummyMarketData())

    asset = "000001.SZ"
    await publish_market([broker1, broker2], asset, 10.0, up_limit=11.0, down_limit=9.0)

    task1 = asyncio.create_task(broker1.buy(asset, 100, price=10.0, timeout=1.0))
    task2 = asyncio.create_task(broker2.buy(asset, 200, price=10.0, timeout=1.0))
    await asyncio.sleep(0.05)
    await publish_market([broker1, broker2], asset, 10.0, volume=10000)

    result1 = await task1
    result2 = await task2
    assert len(result1.trades or []) == 1
    assert len(result2.trades or []) == 1
    assert broker1._positions[asset].shares == 100
    assert broker2._positions[asset].shares == 200


@pytest.mark.asyncio
async def test_paper_broker_day_close_cancels_pending_orders():
    db.init(":memory:")
    broker = SimulationBroker("paper-close", principal=100000, market_data=DummyMarketData())
    asset = "000001.SZ"
    await publish_market([broker], asset, 10.0, up_limit=11.0, down_limit=9.0)

    unfilled_task = asyncio.create_task(broker.buy(asset, 1000, price=9.0, timeout=1.0))
    await asyncio.sleep(0.05)
    order1 = broker._active_orders[asset][0]
    await broker.on_day_close()
    result1 = await unfilled_task

    assert len(result1.trades or []) == 0
    assert db.get_order(order1.qtoid).status == OrderStatus.CANCELED

    partial_task = asyncio.create_task(broker.buy(asset, 1000, price=10.0, timeout=1.0))
    await asyncio.sleep(0.05)
    await publish_market([broker], asset, 10.0, volume=5)
    await asyncio.sleep(0.05)
    order2 = broker._active_orders[asset][0]
    assert order2.filled == 500

    await broker.on_day_close()
    result2 = await partial_task
    assert len(result2.trades or []) == 1
    assert result2.trades[0].shares == 500
    assert db.get_order(order2.qtoid).status == OrderStatus.PARTSUCC_CANCEL


@pytest.mark.asyncio
async def test_paper_broker_full_lifecycle_metrics():
    db.init(":memory:")
    portfolio_id = "paper-metrics"
    asset = "000001.SZ"

    with patch.object(SimulationBroker, "_get_today", return_value=datetime.date(2022, 12, 31)):
        broker = SimulationBroker.create(
            portfolio_id=portfolio_id,
            principal=100000,
            market_data=DummyMarketData(),
        )

    with patch.object(SimulationBroker, "_get_today", return_value=datetime.date(2023, 1, 1)):
        await publish_market([broker], asset, 10.0, up_limit=11.0, down_limit=9.0)
        buy_task = asyncio.create_task(broker.buy(asset, 1000, price=10.0, timeout=1.0))
        await asyncio.sleep(0.05)
        await publish_market([broker], asset, 10.0, volume=2000)
        buy_result = await buy_task
        assert len(buy_result.trades or []) == 1
        await broker.on_day_close(close_prices={asset: 10.5})

    with patch.object(SimulationBroker, "_get_today", return_value=datetime.date(2023, 1, 1)):
        broker = SimulationBroker.load(portfolio_id, market_data=DummyMarketData())

    with patch.object(SimulationBroker, "_get_today", return_value=datetime.date(2023, 1, 2)):
        await publish_market([broker], asset, 11.0, up_limit=12.0, down_limit=10.0)
        sell_task = asyncio.create_task(broker.sell(asset, 500, price=11.0, timeout=1.0))
        await asyncio.sleep(0.05)
        await publish_market([broker], asset, 11.0, volume=1000)
        sell_result = await sell_task
        assert len(sell_result.trades or []) == 1
        await broker.on_day_close(close_prices={asset: 11.0})

    with patch.object(SimulationBroker, "_get_today", return_value=datetime.date(2023, 1, 3)):
        await broker.on_day_close(close_prices={asset: 10.0})

    stats = metrics(portfolio_id)
    assert stats is not None
    assert not stats.empty
    assert "Sharpe Ratio" in stats.index
