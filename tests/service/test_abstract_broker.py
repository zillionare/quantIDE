import datetime
import tempfile
from pathlib import Path

import polars as pl
import pytest

from quantide.core.enums import BidType, OrderSide
from quantide.data.sqlite import Asset, Order, Position, Trade, db
from quantide.service.abstract_broker import AbstractBroker


@pytest.fixture(scope="function")
def setup_db():
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test_bills.db"
        db.init(db_path)
        yield db
        db.close()


from quantide.core.enums import BrokerKind
from quantide.service.local_broker import LocalBroker


class MockBroker(LocalBroker):
    async def buy(self, *args, **kwargs):
        pass

    async def buy_percent(self, *args, **kwargs):
        pass

    async def buy_amount(self, *args, **kwargs):
        return []

    async def sell(self, *args, **kwargs):
        return []

    async def sell_percent(self, *args, **kwargs):
        return []

    async def sell_amount(self, *args, **kwargs):
        return []

    async def cancel_order(self, *args, **kwargs):
        pass

    async def cancel_all_orders(self, *args, **kwargs):
        pass

    async def trade_target_pct(self, *args, **kwargs):
        pass


def test_abstract_broker_bills(setup_db):
    portfolio_id = "test_p"
    broker = MockBroker(kind=BrokerKind.BACKTEST, portfolio_id=portfolio_id)

    # 1. Prepare data for this portfolio
    tm = datetime.datetime.now()
    dt = tm.date()

    # Order
    order = Order(
        portfolio_id, "000001.SZ", OrderSide.BUY, 100, BidType.MARKET, tm=tm, qtoid="o1"
    )
    db.insert_order(order)

    # Trade
    trade = Trade(
        portfolio_id,
        "t1",
        "o1",
        "f1",
        "000001.SZ",
        100,
        10.0,
        1000.0,
        tm,
        OrderSide.BUY,
        "c1",
    )
    db.insert_trades(trade)

    # Position
    pos = Position(portfolio_id, dt, "000001.SZ", 100, 100, 10.0, 0.0, 1000.0)
    db.upsert_positions(pos)

    # Asset
    asset = Asset(portfolio_id, dt, 1000000.0, 1000000.0, 0.0, 0.0, 1000000.0)
    db.upsert_asset(asset)

    # 2. Prepare data for another portfolio
    other_p = "other_p"
    db.insert_order(
        Order(
            other_p, "000002.SZ", OrderSide.BUY, 200, BidType.MARKET, tm=tm, qtoid="o2"
        )
    )
    db.insert_trades(
        Trade(
            other_p,
            "t2",
            "o2",
            "f2",
            "000002.SZ",
            200,
            20.0,
            4000.0,
            tm,
            OrderSide.BUY,
            "c2",
        )
    )
    db.upsert_positions(Position(other_p, dt, "000002.SZ", 200, 200, 20.0, 0.0, 4000.0))
    db.upsert_asset(Asset(other_p, dt, 2000000.0, 2000000.0, 0.0, 0.0, 2000000.0))

    # 3. Call bills
    res = broker.bills()

    # 4. Verify results
    assert isinstance(res, dict)
    assert set(res.keys()) == {"orders", "trades", "positions", "assets"}

    # Verify orders (only o1)
    assert len(res["orders"]) == 1
    assert res["orders"]["qtoid"][0] == "o1"
    assert res["orders"]["portfolio_id"][0] == portfolio_id

    # Verify trades (only t1)
    assert len(res["trades"]) == 1
    assert res["trades"]["tid"][0] == "t1"
    assert res["trades"]["portfolio_id"][0] == portfolio_id

    # Verify positions
    assert len(res["positions"]) == 1
    assert res["positions"]["asset"][0] == "000001.SZ"
    assert res["positions"]["portfolio_id"][0] == portfolio_id

    # Verify assets
    assert len(res["assets"]) == 1
    assert res["assets"]["total"][0] == 1000000.0
    assert res["assets"]["portfolio_id"][0] == portfolio_id


def test_abstract_broker_bills_empty(setup_db):
    broker = MockBroker(kind=BrokerKind.BACKTEST, portfolio_id="empty_p")
    res = broker.bills()

    assert len(res["orders"]) == 0
    assert len(res["trades"]) == 0
    assert len(res["positions"]) == 0
    assert len(res["assets"]) == 0
