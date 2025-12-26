import pytest
import tempfile
from pathlib import Path
from pyqmt.models import OrderModel, TradeModel, PositionModel, AssetModel
from pyqmt.core.enums import OrderSide, BidType, OrderStatus
import datetime
import sqlite3
import copy


@pytest.fixture(scope="function")
def temp_db():
    from pyqmt.dal.tradedb import db
    _, temp_file_path = tempfile.mkstemp(suffix=".db")
    db._initialized = False
    db.init(temp_file_path)
    yield db
    try:
        Path(temp_file_path).unlink()
    except Exception:
        pass


def test_table_creation(temp_db):
    """Test that all model tables are created during initialization"""
    # Check that tables exist
    tables = temp_db.table_names()
    assert "orders" in tables
    assert "trades" in tables
    assert "positions" in tables
    assert "assets" in tables

    # Verify table schemas
    orders_table = temp_db["orders"]
    trades_table = temp_db["trades"]

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


def test_orders_crud(temp_db):
    """Test order CRUD"""
    

    # 01 Test saving order
    tm = datetime.datetime.now()
    assert temp_db.query_order_by_date(tm) is None
    order = OrderModel(
        asset="000001.SZ",
        price=10.5,
        shares=100,
        side=OrderSide.BUY,
        bid_type=BidType.MARKET,
        tm=tm,
    )
    qtoid = temp_db.insert_order(order)

    # Verify it was saved
    saved_order = temp_db.get_order(qtoid)
    assert saved_order is not None
    assert saved_order.qtoid == qtoid
    assert saved_order.asset == "000001.SZ"
    assert saved_order.foid is None
    assert saved_order.cid is None
    assert saved_order.status_msg == ""

    # 02 Test updating order
    temp_db.update_order(
        qtoid,
        status=OrderStatus.REPORTED_CANCEL,
        status_msg="Canceled by user",
        foid=str(1234),
        cid="567",
    )

    # 03 Verify update
    updated_order = temp_db.get_order(qtoid)
    assert updated_order.status == OrderStatus.REPORTED_CANCEL
    assert updated_order.status_msg == "Canceled by user"
    assert updated_order.foid == str(1234)
    assert updated_order.cid == "567"
    assert isinstance(updated_order.tm, datetime.datetime)

    # 04 fetch by foid
    db_order = temp_db.get_order_by_foid(str(1234))
    assert db_order is not None
    assert db_order.qtoid == qtoid
    assert db_order.foid == str(1234)

    # 05 返回所有的订单
    orders = temp_db.orders_all()
    assert len(orders) == 1
    assert orders["tm"][0] == tm

    # 06 Test query_order_by_date
    orders_by_date = temp_db.query_order_by_date(tm)
    assert len(orders_by_date) ==1 # type: ignore
    
    order_qtoids = orders_by_date["qtoid"].to_list() #type: ignore
    assert qtoid in order_qtoids

    # Create an order for a different date
    yesterday = tm.date() - datetime.timedelta(days=1)
    order2 = copy.deepcopy(order)
    order2.qtoid = "order2"
    order2.asset = "000002.SZ"
    order2.tm = datetime.datetime.combine(yesterday, datetime.time(9, 0))
    order2_qtoid = temp_db.insert_order(order2)

    # Query orders by yesterday - should only return the second order
    yesterday_orders = temp_db.query_order_by_date(yesterday)
    assert len(yesterday_orders) == 1
    assert yesterday_orders["qtoid"][0] == order2_qtoid
    assert yesterday_orders["asset"][0] == "000002.SZ"


def test_get_order_by_foid(temp_db):
    """Test getting order by external foid"""
    

    # Save the order
    tm = datetime.datetime.now()
    order = OrderModel(
        qtoid="internal_1",
        asset="000001.SZ",
        price=10.5,
        shares=100,
        side=OrderSide.BUY,
        bid_type=BidType.MARKET,
        tm=tm,
        foid="123"
    )
    temp_db.insert_order(order)

    # Retrieve by foid
    retrieved_order = temp_db.get_order_by_foid("123")
    assert retrieved_order is not None
    assert retrieved_order.qtoid == "internal_1"
    assert retrieved_order.foid == "123"


def test_trades_crud(temp_db):
    """Test trades CRUD operations with self-contained workflow"""
    # First create an order that we'll reference in the trade
    tm = datetime.datetime.now()

    # 测试空表查询（或者不命中）,返回None
    assert temp_db.get_order_by_foid("Not Exist") is None
    assert temp_db.trades_all() is None

    order = OrderModel(
        asset="000001.SZ",
        price=10.5,
        shares=100,
        side=OrderSide.BUY,
        bid_type=BidType.MARKET,
        tm=tm,
        foid="foid1"
    )
    qtoid = temp_db.insert_order(order)

    # Create and save multiple trade records
    trade1 = TradeModel(
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

    trade2 = TradeModel(
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
    temp_db.insert_trades([trade1, trade2])

    # Test get_trade to retrieve a single trade by tid
    retrieved_trade = temp_db.get_trade("trade1")
    assert retrieved_trade is not None
    assert retrieved_trade.tid == "trade1"
    assert retrieved_trade.qtoid == qtoid
    assert retrieved_trade.asset == "000001.SZ"
    assert retrieved_trade.shares == 100
    assert retrieved_trade.price == 10.5
    assert retrieved_trade.amount == 1050.0
    assert retrieved_trade.side == OrderSide.BUY

    # Test query_trade to retrieve trades by qtoid
    trades_by_qtoid = temp_db.query_trade(qtoid=qtoid)
    assert len(trades_by_qtoid) == 2  # Should return trade1 and trade2
    trade_tids = trades_by_qtoid["tid"]
    assert "trade1" in trade_tids
    assert "trade2" in trade_tids

    # Test query_trade to retrieve trades by foid
    trades_by_foid = temp_db.query_trade(foid="foid1")
    assert len(trades_by_foid) == 2  # Should return trade1 and trade2
    assert trades_by_foid["tid"][0] == "trade1"
    assert isinstance(trades_by_foid["tm"][0], datetime.datetime)

    # Test query_trade with no parameters to get all trades
    all_trades = temp_db.query_trade()
    assert len(all_trades) == 2
    assert "trade1" in all_trades["tid"]
    assert "trade2" in all_trades["tid"]

    # Test saving a single trade record
    trade3 = TradeModel(
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
    temp_db.insert_trades(trade3)

    # Verify single trade was added
    all_trades = temp_db.query_trade()
    assert len(all_trades) == 3
    assert "trade3" in all_trades["tid"]

    # Test get_trade for the new single trade
    retrieved_trade3 = temp_db.get_trade("trade3")
    assert retrieved_trade3 is not None
    assert retrieved_trade3.tid == "trade3"
    assert retrieved_trade3.asset == "000004.SZ"
    assert retrieved_trade3.shares == 300

    # 返回所有的 trades
    df = temp_db.trades_all()
    assert len(df) == 3


def test_foreign_key_constraint(temp_db):
    """Test foreign key constraint enforcement"""

    

    # Create an order first
    order = OrderModel(        asset="000001.SZ",
        price=10.5,
        shares=100,
        side=OrderSide.BUY,
        bid_type=BidType.MARKET,
        tm=datetime.datetime.now(),
    )
    qtoid = temp_db.insert_order(order)
    
    # Create a trade that references the order - this should succeed
    valid_trade = TradeModel(
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
    temp_db.insert_trades(valid_trade)

    # Verify the trade was saved
    retrieved_trade = temp_db.get_trade("valid_trade")
    assert retrieved_trade is not None
    assert retrieved_trade.qtoid == qtoid

    # Verify foreign key constraint is defined
    foreign_keys = list(temp_db["trades"].foreign_keys)
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
    invalid_trade = TradeModel(
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
        temp_db.insert_trades(invalid_trade)


def test_positions_crud(temp_db):
    """Test get_positions method to cover that code path"""
    

    # 01 Insert single position record directly
    pos01 = PositionModel(
        dt=datetime.date.today(),
        asset="000001.SZ",
        shares=1000,
        avail=800,
        price=10.5,
        mv=10500.0,
        profit=1000.0,
    )

    temp_db.upsert_positions(pos01)

    # Test get_positions
    positions = temp_db.get_positions(datetime.date.today())
    assert len(positions) == 1
    assert positions["asset"][0] == "000001.SZ"
    assert positions["shares"][0] == 1000
    assert isinstance(positions["dt"][0], datetime.date)

    # 02 update single position record
    pos02 = copy.deepcopy(pos01)
    pos02.asset = "000002.SZ"
    temp_db.upsert_positions(pos02)

    # Test get_positions for the new asset
    positions = temp_db.get_positions(datetime.date.today())
    assert len(positions) == 2
    assert "000001.SZ" in positions["asset"]
    assert "000002.SZ" in positions["asset"]

    # 03 get all positions
    posistions = temp_db.positions_all()
    assert len(posistions) == 2

    # 04 batch upsert
    temp_db.upsert_positions([pos01, pos02])
    positions = temp_db.positions_all()
    assert len(positions) == 2
    assert "000001.SZ" in positions["asset"]
    assert "000002.SZ" in positions["asset"]
    assert positions["dt"][0] == datetime.date.today()

def test_proxy_methods(temp_db):
    """Test __getitem__ and __getattr__ proxy methods"""

    

    # Test __getitem__ proxy
    orders_table = temp_db["orders"]
    assert orders_table is not None

    # Test __getattr__ proxy by calling a method from the underlying db
    table_names = temp_db.table_names()
    assert "orders" in table_names
    assert "trades" in table_names
    assert "positions" in table_names
    assert "assets" in table_names


def test_foreign_key_constraint_in_init_tables(temp_db):
    """Test that foreign keys are properly created in _init_tables"""

    

    # Check that foreign key constraint exists on trades table
    trades_table = temp_db["trades"]
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


def test_assets(temp_db):

    

    assert temp_db.assets_all() is None
    # 01 save/query
    dt = datetime.datetime.today()
    asset = AssetModel(dt, 1_000_000, 1_000_000, 0, 0, 1_000_000)
    temp_db.insert_asset(asset)
    asset_from_db = temp_db.get_asset(dt)
    assert asset_from_db is not None
    assert asset_from_db == asset

    # 02 update
    new_principal = 5_000_000
    asset.principal = new_principal
    temp_db.update_asset(dt, principal=new_principal)
    asset_from_db = temp_db.get_asset(dt)
    assert asset_from_db is not None
    assert asset_from_db == asset

    # 03 dt 为 datetime.date
    dt = datetime.date.today() - datetime.timedelta(days=1)
    asset = AssetModel(dt, new_principal, 1_000_000, 0, 0, 1_000_000)
    temp_db.insert_asset(asset)
    asset_from_db = temp_db.get_asset(dt)
    assert asset_from_db is not None
    assert asset_from_db == asset

    # 04 assets_all
    assets = temp_db.assets_all()
    assert len(assets) == 2
