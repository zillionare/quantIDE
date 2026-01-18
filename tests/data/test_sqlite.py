import copy
import datetime
import sqlite3
import tempfile
import uuid
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from typing import List, Optional, Union

import polars as pl
import pytest

from pyqmt.core.enums import BidType, BrokerKind, OrderSide, OrderStatus
from pyqmt.data.sqlite import Asset, Entity, Order, Portfolio, Position, Trade, db


@pytest.fixture(scope="function")
def setup():
    with tempfile.TemporaryDirectory() as temp_dir:
        db_path = Path(temp_dir) / "test.db"
        db.init(db_path)
        yield db
        try:
            db.close()
            db_path.unlink()
        except Exception:
            pass


def test_table_creation(setup):
    """Test that all model tables are created during initialization"""
    # Check that tables exist
    tables = db.table_names()
    assert "orders" in tables
    assert "trades" in tables
    assert "positions" in tables
    assert "assets" in tables

    # Verify table schemas
    orders_table = db["orders"]
    trades_table = db["trades"]

    # Check that primary keys are set correctly
    # 注意：sqlite-utils 中 pks 是一个列表
    assert "qtoid" in orders_table.pks
    assert "tid" in trades_table.pks

    # Check foreign key constraints
    foreign_keys = list(trades_table.foreign_keys)
    assert len(foreign_keys) > 0
    # Check that there's a foreign key from trades.qtoid to orders.qtoid
    qtoid_fk = [
        fk
        for fk in foreign_keys
        if fk.column == "qtoid"
        and fk.other_table == "orders"
        and fk.other_column == "qtoid"
    ]
    assert len(qtoid_fk) == 1


def test_portfolio_crud(setup):
    """Test portfolio CRUD"""
    # 01 Test saving portfolio
    portfolio_id = "test_p"
    start_date = datetime.date(2024, 1, 1)
    portfolio = Portfolio(
        portfolio_id=portfolio_id,
        kind=BrokerKind.BACKTEST,
        start=start_date,
        name="Test Portfolio",
        info="Testing...",
        status=True
    )
    db.insert_portfolio(portfolio)

    # Verify it was saved
    saved = db.get_portfolio(portfolio_id)
    assert saved is not None
    assert saved.portfolio_id == portfolio_id
    assert saved.kind == BrokerKind.BACKTEST
    assert saved.name == "Test Portfolio"
    assert saved.start == start_date

    # 02 Test update_portfolio with variable fields
    db.update_portfolio(
        portfolio_id,
        name="Updated Name",
        info="Updated Info",
        status=False
    )

    # Verify update
    updated = db.get_portfolio(portfolio_id)
    assert updated is not None
    assert updated.name == "Updated Name"
    assert updated.info == "Updated Info"
    assert updated.status is False
    # Ensure other fields are intact
    assert updated.kind == BrokerKind.BACKTEST
    assert updated.start == start_date

def test_orders_crud(setup):
    """Test order CRUD"""


    # 01 Test saving order
    tm = datetime.datetime.now()
    assert db.query_order_by_date(tm) is None
    order = Order(
        portfolio_id="test_portfolio",
        asset="000001.SZ",
        price=10.5,
        shares=100,
        side=OrderSide.BUY,
        bid_type=BidType.MARKET,
        tm=tm,
    )
    qtoid = db.insert_order(order)

    # Verify it was saved
    saved_order = db.get_order(qtoid)
    assert saved_order is not None
    assert saved_order.qtoid == qtoid
    assert saved_order.asset == "000001.SZ"
    assert saved_order.foid is None
    assert saved_order.cid is None
    assert saved_order.status_msg == ""

    # 02 Test updating order
    db.update_order(
        qtoid,
        status=OrderStatus.REPORTED_CANCEL,
        status_msg="Canceled by user",
        foid=str(1234),
        cid="567",
    )

    # 03 Verify update
    updated_order = db.get_order(qtoid)
    assert updated_order.status == OrderStatus.REPORTED_CANCEL
    assert updated_order.status_msg == "Canceled by user"
    assert updated_order.foid == str(1234)
    assert updated_order.cid == "567"
    assert isinstance(updated_order.tm, datetime.datetime)

    # 04 fetch by foid
    db_order = db.get_order_by_foid(str(1234))
    assert db_order is not None
    assert db_order.qtoid == qtoid
    assert db_order.foid == str(1234)

    # 05 返回所有的订单
    orders = db.orders_all()
    assert len(orders) == 1
    assert orders["tm"][0] == tm

    # 06 Test query_order_by_date
    orders_by_date = db.query_order_by_date(tm)
    assert len(orders_by_date) ==1 # type: ignore

    order_qtoids = orders_by_date["qtoid"].to_list() #type: ignore
    assert qtoid in order_qtoids

    # Create an order for a different date
    yesterday = tm.date() - datetime.timedelta(days=1)
    order2 = copy.deepcopy(order)
    order2.qtoid = "order2"
    order2.asset = "000002.SZ"
    order2.tm = datetime.datetime.combine(yesterday, datetime.time(9, 0))
    order2_qtoid = db.insert_order(order2)

    # Query orders by yesterday - should only return the second order
    yesterday_orders = db.query_order_by_date(yesterday)
    assert len(yesterday_orders) == 1
    assert yesterday_orders["qtoid"][0] == order2_qtoid
    assert yesterday_orders["asset"][0] == "000002.SZ"


def test_get_order_by_foid(setup):
    """Test getting order by external foid"""


    # Save the order
    tm = datetime.datetime.now()
    order = Order(
        portfolio_id="test_portfolio",
        qtoid="internal_1",
        asset="000001.SZ",
        price=10.5,
        shares=100,
        side=OrderSide.BUY,
        bid_type=BidType.MARKET,
        tm=tm,
        foid="123"
    )
    db.insert_order(order)

    # Retrieve by foid
    retrieved_order = db.get_order_by_foid("123")
    assert retrieved_order is not None
    assert retrieved_order.qtoid == "internal_1"
    assert retrieved_order.foid == "123"


def test_trades_crud(setup):
    """Test trades CRUD operations with self-contained workflow"""
    # First create an order that we'll reference in the trade
    tm = datetime.datetime.now()

    # 测试空表查询（或者不命中）,返回None
    assert db.get_order_by_foid("Not Exist") is None
    assert db.trades_all() is None

    order = Order(
        portfolio_id="test_portfolio",
        asset="000001.SZ",
        price=10.5,
        shares=100,
        side=OrderSide.BUY,
        bid_type=BidType.MARKET,
        tm=tm,
        foid="foid1"
    )
    qtoid = db.insert_order(order)

    # Create and save multiple trade records
    trade1 = Trade(
        portfolio_id="test_portfolio",
        tid="trade1",
        qtoid=qtoid,  # Reference to the existing order
        foid="foid1",
        asset="000001.SZ",
        shares=100,
        price=10.5,
        amount=1050.0,
        tm=datetime.datetime.now(),
        side=OrderSide.BUY,
        cid="cid1",
    )

    trade2 = Trade(
        portfolio_id="test_portfolio",
        tid="trade2",
        qtoid=qtoid,  # Reference to the same order
        foid="foid1",
        asset="000002.SZ",
        shares=200,
        price=20.5,
        amount=4100.0,
        tm=datetime.datetime.now(),
        side=OrderSide.SELL,
        cid="cid2",
    )

    # Test save_trades with multiple trades (batch insert)
    db.insert_trades([trade1, trade2])

    # Test get_trade to retrieve a single trade by tid
    retrieved_trade = db.get_trade("trade1")
    assert retrieved_trade is not None
    assert retrieved_trade.tid == "trade1"
    assert retrieved_trade.qtoid == qtoid
    assert retrieved_trade.asset == "000001.SZ"
    assert retrieved_trade.shares == 100
    assert retrieved_trade.price == 10.5
    assert retrieved_trade.amount == 1050.0
    assert retrieved_trade.side == OrderSide.BUY

    # Test query_trade to retrieve trades by qtoid
    trades_by_qtoid = db.query_trade(qtoid=qtoid)
    assert len(trades_by_qtoid) == 2  # Should return trade1 and trade2
    trade_tids = trades_by_qtoid["tid"]
    assert "trade1" in trade_tids
    assert "trade2" in trade_tids

    # Test query_trade to retrieve trades by foid
    trades_by_foid = db.query_trade(foid="foid1")
    assert len(trades_by_foid) == 2  # Should return trade1 and trade2
    assert trades_by_foid["tid"][0] == "trade1"
    assert isinstance(trades_by_foid["tm"][0], datetime.datetime)

    # Test query_trade with no parameters to get all trades
    all_trades = db.query_trade()
    assert len(all_trades) == 2
    assert "trade1" in all_trades["tid"]
    assert "trade2" in all_trades["tid"]

    # Test saving a single trade record
    trade3 = Trade(
        portfolio_id="test_portfolio",
        tid="trade3",
        qtoid=qtoid,  # Reference to the same order
        foid="foid1",
        asset="000004.SZ",
        shares=300,
        price=30.5,
        amount=9150.0,
        tm=datetime.datetime.now(),
        side=OrderSide.BUY,
        cid="cid3",
    )

    # Save single trade
    db.insert_trades(trade3)

    # Verify single trade was added
    all_trades = db.query_trade()
    assert len(all_trades) == 3
    assert "trade3" in all_trades["tid"]

    # Test get_trade for the new single trade
    retrieved_trade3 = db.get_trade("trade3")
    assert retrieved_trade3 is not None
    assert retrieved_trade3.tid == "trade3"
    assert retrieved_trade3.asset == "000004.SZ"
    assert retrieved_trade3.shares == 300

    # 返回所有的 trades
    df = db.trades_all()
    assert len(df) == 3


def test_foreign_key_constraint(setup):
    """Test foreign key constraint enforcement"""



    # Create an order first
    order = Order(
        portfolio_id="test_portfolio",
        asset="000001.SZ",
        price=10.5,
        shares=100,
        side=OrderSide.BUY,
        bid_type=BidType.MARKET,
        tm=datetime.datetime.now(),
    )
    qtoid = db.insert_order(order)

    # Create a trade that references the order - this should succeed
    valid_trade = Trade(
        portfolio_id="test_portfolio",
        tid="valid_trade",
        qtoid=qtoid,  # Valid reference to existing order
        foid="foid1",
        asset="000001.SZ",
        shares=100,
        price=10.5,
        amount=1050.0,
        tm=datetime.datetime.now(),
        side=OrderSide.BUY,
        cid="cid1",
    )

    # This should succeed since qtoid references an existing order
    db.insert_trades(valid_trade)

    # Verify the trade was saved
    retrieved_trade = db.get_trade("valid_trade")
    assert retrieved_trade is not None
    assert retrieved_trade.qtoid == qtoid

    # Verify foreign key constraint is defined
    foreign_keys = list(db["trades"].foreign_keys)
    qtoid_fk = [
        fk
        for fk in foreign_keys
        if fk.column == "qtoid"
        and fk.other_table == "orders"
        and fk.other_column == "qtoid"
    ]
    assert len(qtoid_fk) == 1

    # Now test what happens when we try to insert a trade with a non-existent qtoid
    # This should fail due to foreign key constraint
    invalid_trade = Trade(
        portfolio_id="test_portfolio",
        tid="invalid_trade",
        qtoid="non_existent_qtoid",  # Invalid reference to non-existent order
        foid="foid2",
        asset="000002.SZ",
        shares=200,
        price=20.5,
        amount=4100.0,
        tm=datetime.datetime.now(),
        side=OrderSide.SELL,
        cid="cid2",
    )

    # Attempt to save the invalid trade - this should raise an exception due to foreign key constraint
    with pytest.raises(sqlite3.IntegrityError):
        db.insert_trades(invalid_trade)


def test_positions_crud(setup):
    """Test get_positions method to cover that code path"""


    # 01 Insert single position record directly
    pos01 = Position(
        portfolio_id="test_portfolio",
        dt=datetime.date.today(),
        asset="000001.SZ",
        shares=1000,
        avail=800,
        price=10.5,
        mv=10500.0,
        profit=1000.0,
    )

    db.upsert_positions(pos01)

    # Test get_positions
    positions = db.get_positions(datetime.date.today(), portfolio_id="test_portfolio")
    assert len(positions) == 1
    assert positions["asset"][0] == "000001.SZ"
    assert positions["shares"][0] == 1000
    assert isinstance(positions["dt"][0], datetime.date)

    # 02 update single position record
    pos02 = copy.deepcopy(pos01)
    pos02.asset = "000002.SZ"
    db.upsert_positions(pos02)

    # Test get_positions for the new asset
    positions = db.get_positions(datetime.date.today(), portfolio_id="test_portfolio")
    assert len(positions) == 2
    assert "000001.SZ" in positions["asset"]
    assert "000002.SZ" in positions["asset"]

    # 03 get all positions
    posistions = db.positions_all(portfolio_id="test_portfolio")
    assert len(posistions) == 2

    # 04 batch upsert
    db.upsert_positions([pos01, pos02])
    positions = db.positions_all(portfolio_id="test_portfolio")
    assert len(positions) == 2
    assert "000001.SZ" in positions["asset"]
    assert "000002.SZ" in positions["asset"]
    assert positions["dt"][0] == datetime.date.today()

    # 05 Test get_positions with dt=None (fetch latest date)
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    pos_tomorrow_1 = Position(
        portfolio_id="test_portfolio",
        dt=tomorrow,
        asset="000003.SZ",
        shares=3000,
        avail=3000,
        price=30.0,
        mv=90000.0,
        profit=0.0,
    )
    pos_tomorrow_2 = Position(
        portfolio_id="test_portfolio",
        dt=tomorrow,
        asset="000004.SZ",
        shares=4000,
        avail=4000,
        price=40.0,
        mv=160000.0,
        profit=0.0,
    )
    db.upsert_positions([pos_tomorrow_1, pos_tomorrow_2])

    # Calling get_positions with dt=None should return BOTH positions from tomorrow
    latest_positions = db.get_positions(dt=None, portfolio_id="test_portfolio")
    assert len(latest_positions) == 2
    assert all(latest_positions["dt"] == tomorrow)
    assert set(latest_positions["asset"]) == {"000003.SZ", "000004.SZ"}
    assert set(latest_positions["shares"]) == {3000, 4000}

    # Test get_positions with dt=None for empty portfolio
    assert len(db.get_positions(dt=None, portfolio_id="non_existent")) == 0

def test_proxy_methods(setup):
    """Test __getitem__ and __getattr__ proxy methods"""



    # Test __getitem__ proxy
    orders_table = db["orders"]
    assert orders_table is not None

    # Test __getattr__ proxy by calling a method from the underlying db
    table_names = db.table_names()
    assert "orders" in table_names
    assert "trades" in table_names
    assert "positions" in table_names
    assert "assets" in table_names


def test_foreign_key_constraint_in_init_tables(setup):
    """Test that foreign keys are properly created in _init_tables"""



    # Check that foreign key constraint exists on trades table
    trades_table = db["trades"]
    foreign_keys = list(trades_table.foreign_keys)

    # Find the foreign key from trades.qtoid to orders.qtoid
    qtoid_fk = [
        fk
        for fk in foreign_keys
        if fk.column == "qtoid"
        and fk.other_table == "orders"
        and fk.other_column == "qtoid"
    ]
    assert (
        len(qtoid_fk) == 1
    ), "Foreign key constraint from trades.qtoid to orders.qtoid should exist"


def test_assets(setup):
    assert len(db.assets_all()) == 0
    # 01 save/query
    dt = datetime.datetime.today().date()
    asset = Asset(
        portfolio_id="test_portfolio",
        dt=dt,
        principal=1_000_000,
        cash=1_000_000,
        frozen_cash=0,
        market_value=0,
        total=1_000_000
    )
    db.upsert_asset(asset)
    asset_from_db = db.get_asset(dt, portfolio_id="test_portfolio")
    assert asset_from_db is not None
    assert asset_from_db == asset

    # 02 update
    new_principal = 5_000_000
    asset.principal = new_principal
    db.update_asset(dt, portfolio_id="test_portfolio", principal=new_principal)
    asset_from_db = db.get_asset(dt, portfolio_id="test_portfolio")
    assert asset_from_db is not None
    assert asset_from_db == asset

    # 03 dt 为 datetime.date
    dt = datetime.date.today() - datetime.timedelta(days=1)
    asset = Asset(
        portfolio_id="test_portfolio",
        dt=dt,
        principal=new_principal,
        cash=1_000_000,
        frozen_cash=0,
        market_value=0,
        total=1_000_000
    )
    db.upsert_asset(asset)
    asset_from_db = db.get_asset(dt, portfolio_id="test_portfolio")
    assert asset_from_db is not None
    assert asset_from_db == asset

    # 04 assets_all
    assets = db.assets_all(portfolio_id="test_portfolio")
    assert len(assets) == 2

    # 05 批量 upsert
    dt3 = datetime.date.today() + datetime.timedelta(days=1)
    dt4 = datetime.date.today() + datetime.timedelta(days=2)
    assets_to_batch = [
        Asset("test_portfolio", dt3, 1000, 1000, 0, 0, 1000),
        Asset("test_portfolio", dt4, 2000, 2000, 0, 0, 2000),
    ]
    db.upsert_asset(assets_to_batch)

    batch_from_db = db.query_assets(portfolio_id="test_portfolio", start=dt3, end=dt4)
    assert len(batch_from_db) == 2
    assert dt3 in batch_from_db["dt"].to_list()
    assert dt4 in batch_from_db["dt"].to_list()


def test_query_assets_combinations(setup):
    db = setup
    portfolio_1 = "p1"
    portfolio_2 = "p2"
    d1 = datetime.date(2024, 1, 1)
    d2 = datetime.date(2024, 1, 2)
    d3 = datetime.date(2024, 1, 3)

    assets = [
        Asset(portfolio_1, d1, 100, 100, 0, 0, 100),
        Asset(portfolio_1, d2, 110, 110, 0, 0, 110),
        Asset(portfolio_1, d3, 120, 120, 0, 0, 120),
        Asset(portfolio_2, d2, 200, 200, 0, 0, 200),
    ]
    for a in assets:
        db.upsert_asset(a)

    # 1. Query by portfolio_id
    df = db.query_assets(portfolio_id=portfolio_1)
    assert len(df) == 3
    assert all(df["portfolio_id"] == portfolio_1)

    # 2. Query by date range
    df = db.query_assets(start=d2, end=d2)
    assert len(df) == 2
    assert set(df["portfolio_id"]) == {portfolio_1, portfolio_2}

    # 3. Query by portfolio and date range
    df = db.query_assets(portfolio_id=portfolio_1, start=d2)
    assert len(df) == 2
    assert d1 not in df["dt"].to_list()

    # 4. Query all
    df = db.query_assets()
    assert len(df) == 4


def test_query_positions_combinations(setup):
    db = setup
    portfolio_1 = "p1"
    portfolio_2 = "p2"
    d1 = datetime.date(2024, 1, 1)
    d2 = datetime.date(2024, 1, 2)

    positions = [
        Position(portfolio_1, d1, "000001.SZ", 100, 100, 10, 0, 1000),
        Position(portfolio_1, d2, "000001.SZ", 110, 110, 11, 0, 1210),
        Position(portfolio_2, d1, "000002.SZ", 200, 200, 20, 0, 4000),
    ]
    db.upsert_positions(positions)

    # 1. Query by portfolio
    df = db.query_positions(portfolio_id=portfolio_1)
    assert len(df) == 2

    # 2. Query by date
    df = db.query_positions(start=d1, end=d1)
    assert len(df) == 2

    # 3. Query all
    df = db.query_positions()
    assert len(df) == 3


def test_get_asset_edge_cases(setup):
    db = setup
    p1 = "p1"
    p2 = "p2"
    d1 = datetime.date(2024, 1, 1)
    d2 = datetime.date(2024, 1, 2)

    db.upsert_asset(Asset(p1, d1, 100, 100, 0, 0, 100))
    db.upsert_asset(Asset(p2, d2, 200, 200, 0, 0, 200))

    # Get latest across all portfolios
    latest = db.get_asset()
    assert latest.portfolio_id == p2
    assert latest.dt == d2

    # Get latest for specific portfolio
    latest_p1 = db.get_asset(portfolio_id=p1)
    assert latest_p1.portfolio_id == p1
    assert latest_p1.dt == d1

    # Get specific date across all portfolios (returns first match)
    asset_d1 = db.get_asset(dt=d1)
    assert asset_d1.dt == d1
    assert asset_d1.portfolio_id == p1

    # Non-existent
    assert db.get_asset(dt=datetime.date(2000, 1, 1)) is None


def test_sqlite_reinit(setup):
    # setup already initialized db
    db = setup
    # old_path = db.db_path # We don't really need to compare with old path here

    with tempfile.TemporaryDirectory() as temp_dir:
        new_path = Path(temp_dir) / "new_test.db"
        db.init(new_path)
        assert str(db.db_path) == str(new_path)
        assert "orders" in db.table_names()

        # Verify it's a different database
        db.upsert_asset(Asset("p1", datetime.date.today(), 100, 100, 0, 0, 100))
        assert len(db.query_assets()) == 1

        # Close and check file exists
        db.close()
        assert new_path.exists()


def test_entity_to_db_schema_coverage():
    # Test internal Entity.to_db_schema with various types
    class MyEnum(IntEnum):
        A = 1
        B = 2

    @dataclass
    class TestEntity(Entity):
        __table_name__ = "test"
        __pk__ = "id"
        __indexes__ = None

        id: int
        name: str
        price: float
        active: bool
        status: MyEnum
        tags: List[str]
        optional_val: Optional[int]
        union_val: Union[int, str]

    schema = TestEntity.to_db_schema()
    assert schema["id"] == int
    assert schema["name"] == str
    assert schema["price"] == float
    assert schema["active"] == bool
    assert schema["status"] == int
    assert schema["tags"] == str  # Non-standard types fall back to str
    assert schema["optional_val"] == int
    assert schema["union_val"] == int  # Takes first non-None type


def test_upsert_list_vs_single(setup):
    db = setup
    p = "p1"
    d = datetime.date.today()

    # Position upsert single
    pos1 = Position(p, d, "000001.SZ", 100, 100, 10, 0, 1000)
    db.upsert_positions(pos1)
    assert len(db.query_positions()) == 1

    # Position upsert list
    pos2 = Position(p, d, "000002.SZ", 200, 200, 20, 0, 4000)
    db.upsert_positions([pos2])
    assert len(db.query_positions()) == 2

    # Trade insert single
    order = Order(
        p, "000001.SZ", OrderSide.BUY, 100, BidType.MARKET, tm=datetime.datetime.now()
    )
    qtoid = db.insert_order(order)

    trade1 = Trade(
        p,
        "t1",
        qtoid,
        "f1",
        "000001.SZ",
        100,
        10,
        1000,
        datetime.datetime.now(),
        OrderSide.BUY,
        "c1",
    )
    db.insert_trades(trade1)
    assert len(db.query_trade()) == 1

    # Trade insert list
    trade2 = Trade(
        p,
        "t2",
        qtoid,
        "f1",
        "000001.SZ",
        100,
        10,
        1000,
        datetime.datetime.now(),
        OrderSide.BUY,
        "c1",
    )
    db.insert_trades([trade2])
    assert len(db.query_trade()) == 2


def test_entity_post_init_conversions():
    # Test date/datetime conversions in __post_init__

    # 1. Order
    o = Order("p", "A", OrderSide.BUY, 100, BidType.MARKET, tm="2024-01-01T10:00:00")
    assert isinstance(o.tm, datetime.datetime)
    assert o.tm.hour == 10

    # 2. Trade
    t = Trade(
        "p",
        "t1",
        "q1",
        "f1",
        "A",
        100,
        10,
        1000,
        "2024-01-01T10:00:00",
        OrderSide.BUY,
        "c1",
    )
    assert isinstance(t.tm, datetime.datetime)

    # 3. Position
    p_iso = Position("p", "2024-01-01T10:00:00", "A", 100, 100, 10, 0, 1000)
    assert p_iso.dt == datetime.date(2024, 1, 1)

    p_str = Position("p", "2024-01-02", "A", 100, 100, 10, 0, 1000)
    assert p_str.dt == datetime.date(2024, 1, 2)

    p_dt = Position("p", datetime.datetime(2024, 1, 3, 10, 0), "A", 100, 100, 10, 0, 1000)
    assert p_dt.dt == datetime.date(2024, 1, 3)

    # 4. Asset
    a_iso = Asset("p", "2024-01-01T10:00:00", 100, 100, 0, 0, 100)
    assert a_iso.dt == datetime.date(2024, 1, 1)

    a_str = Asset("p", "2024-01-02", 100, 100, 0, 0, 100)
    assert a_str.dt == datetime.date(2024, 1, 2)

    a_dt = Asset("p", datetime.datetime(2024, 1, 3, 10, 0), 100, 100, 0, 0, 100)
    assert a_dt.dt == datetime.date(2024, 1, 3)


def test_query_methods_with_datetime(setup):
    db = setup
    p = "p1"
    dt_obj = datetime.datetime(2024, 1, 1, 10, 0)

    # 1. query_order_by_date with datetime
    db.insert_order(Order(p, "A", OrderSide.BUY, 100, BidType.MARKET, tm=dt_obj))
    res = db.query_order_by_date(dt_obj)
    assert len(res) == 1

    # 2. update_asset with datetime
    db.upsert_asset(Asset(p, dt_obj.date(), 100, 100, 0, 0, 100))
    db.update_asset(dt_obj, p, principal=200)
    assert db.get_asset(dt_obj.date(), p).principal == 200


def test_query_trade_multiple_filters(setup):
    db = setup
    p = "p1"
    q1, q2 = "q1", "q2"
    f1, f2 = "f1", "f2"

    # Insert orders first to satisfy foreign key constraint
    db.insert_order(
        Order(
            p,
            "A",
            OrderSide.BUY,
            100,
            BidType.MARKET,
            tm=datetime.datetime.now(),
            qtoid=q1,
        )
    )
    db.insert_order(
        Order(
            p,
            "B",
            OrderSide.BUY,
            200,
            BidType.MARKET,
            tm=datetime.datetime.now(),
            qtoid=q2,
        )
    )

    db.insert_trades(
        [
            Trade(
                p,
                "t1",
                q1,
                f1,
                "A",
                100,
                10,
                1000,
                datetime.datetime.now(),
                OrderSide.BUY,
                "c1",
            ),
            Trade(
                p,
                "t2",
                q2,
                f2,
                "B",
                200,
                20,
                4000,
                datetime.datetime.now(),
                OrderSide.BUY,
                "c2",
            ),
        ]
    )

    # Query with qtoid and foid (OR logic)
    res = db.query_trade(qtoid=q1, foid=f2)
    assert len(res) == 2

    # Query with only qtoid
    res = db.query_trade(qtoid=q1)
    assert len(res) == 1
    assert res["tid"][0] == "t1"


def test_update_asset_single_field(setup):
    db = setup
    p = "p1"
    d = datetime.date.today()

    asset = Asset(p, d, 1000, 1000, 0, 0, 1000)
    db.upsert_asset(asset)

    # Update only cash
    db.update_asset(d, p, cash=500)
    updated = db.get_asset(d, p)
    assert updated.cash == 500
    assert updated.principal == 1000  # Unchanged

    # Update market_value and total
    db.update_asset(d, p, market_value=600, total=1100)
    updated = db.get_asset(d, p)
    assert updated.market_value == 600
    assert updated.total == 1100
    assert updated.cash == 500  # Unchanged from previous update


def test_query_order_various_filters(setup):
    db = setup
    p = "p1"
    tm1 = datetime.datetime(2024, 1, 1, 10, 0)
    tm2 = datetime.datetime(2024, 1, 2, 10, 0)

    o1 = Order(p, "A", OrderSide.BUY, 100, BidType.MARKET, tm=tm1, foid="f1")
    o2 = Order(
        p, "B", OrderSide.BUY, 200, BidType.MARKET, tm=tm2, foid="123"
    )  # Numeric foid as string

    db.insert_order(o1)
    db.insert_order(o2)

    # 1. query_order_by_date
    res = db.query_order_by_date(tm1.date())
    assert len(res) == 1
    assert res["qtoid"][0] == o1.qtoid

    # 2. get_order_by_foid with string
    res_f1 = db.get_order_by_foid("f1")
    assert res_f1.qtoid == o1.qtoid

    # 3. get_order_by_foid with int
    res_f2 = db.get_order_by_foid(123)
    assert res_f2.qtoid == o2.qtoid

    # 4. update_order
    db.update_order(o1.qtoid, status=OrderStatus.SUCCEEDED, filled=100)
    updated = db.get_order(o1.qtoid)
    assert updated.status == OrderStatus.SUCCEEDED
    assert updated.filled == 100

    # 5. orders_all
    assert len(db.orders_all()) == 2


def test_query_trade_variations(setup):
    db = setup
    p = "p1"
    q1 = "q1"

    # trades_all when empty
    assert db.trades_all() is None

    db.insert_order(
        Order(
            p,
            "A",
            OrderSide.BUY,
            100,
            BidType.MARKET,
            tm=datetime.datetime.now(),
            qtoid=q1,
        )
    )
    db.insert_trades(
        Trade(
            p,
            "t1",
            q1,
            "f1",
            "A",
            100,
            10,
            1000,
            datetime.datetime.now(),
            OrderSide.BUY,
            "c1",
        )
    )

    # query_trade with no filters (should return all)
    res = db.query_trade()
    assert len(res) == 1

    # trades_all when not empty
    assert len(db.trades_all()) == 1


def test_get_positions_variations(setup):
    db = setup
    p1 = "p1"
    p2 = "p2"
    d1 = datetime.date(2024, 1, 1)
    d2 = datetime.date(2024, 1, 2)

    positions = [
        Position(p1, d1, "A", 100, 100, 10, 0, 1000),
        Position(p2, d2, "B", 200, 200, 20, 0, 4000),
        Position(p1, d2, "C", 300, 300, 30, 0, 9000),
    ]
    db.upsert_positions(positions)

    # 1. get_positions(dt=None, portfolio_id=None) -> returns latest date's positions across all portfolios (d2 has p1:C and p2:B)
    latest_all = db.get_positions()
    assert len(latest_all) == 2
    assert all(latest_all["dt"] == d2)
    assert set(latest_all["asset"]) == {"B", "C"}

    # 1.1 get_positions(dt=None, portfolio_id=p1) -> returns latest date for p1 (d2 has p1:C)
    latest_p1 = db.get_positions(portfolio_id=p1)
    assert len(latest_p1) == 1
    assert latest_p1["dt"][0] == d2
    assert latest_p1["asset"][0] == "C"

    # 1.2 get_positions(dt=None, portfolio_id=p2) -> returns latest date for p2 (d2 has p2:B)
    latest_p2 = db.get_positions(portfolio_id=p2)
    assert len(latest_p2) == 1
    assert latest_p2["dt"][0] == d2
    assert latest_p2["asset"][0] == "B"

    # 2. get_positions(dt=d1)
    assert len(db.get_positions(dt=d1)) == 1

    # 3. positions_all(portfolio_id=p1) -> returns all historical positions for p1 (d1 and d2)
    assert len(db.positions_all(portfolio_id=p1)) == 2


def test_entity_to_db_schema_uuid_and_unions():
    import uuid
    from typing import Union

    @dataclass
    class AdvancedEntity(Entity):
        __table_name__ = "advanced"
        __pk__ = "uid"
        __indexes__ = None

        uid: uuid.UUID
        val: Union[float, None]
        status: Union[int, str]

    schema = AdvancedEntity.to_db_schema()
    assert schema["uid"] == str
    assert schema["val"] == float
    assert schema["status"] == int  # Takes first non-None type
