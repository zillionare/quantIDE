import pytest
import tempfile
from pathlib import Path
from pyqmt.dal.tradedb import TradeDB
from pyqmt.models import OrderModel, TradeModel, PositionModel, AssetModel
from pyqmt.core.enums import OrderSide, BidType, OrderStatus
import datetime
from dataclasses import asdict

@pytest.fixture
def temp_db_file():
    _, temp_file_path = tempfile.mkstemp(suffix=".db")
    yield temp_file_path
    Path(temp_file_path).unlink()
    
def test_table_creation(temp_db_file):
    """Test that all model tables are created during initialization"""
    db = TradeDB()
    db.init(temp_db_file)
    
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


def test_order(temp_db_file):
    """Test order CRUD """
    db = TradeDB()
    db.init(temp_db_file)
    
    # 01 Test saving order
    qtoid = db.save_order(asset = "000001.SZ", price=10.5, shares=100, side=OrderSide.BUY, bid_type=BidType.MARKET, bid_time=datetime.datetime.now())
    
    # Verify it was saved
    saved_order = db.get_order(qtoid)
    assert saved_order is not None
    assert saved_order.qtoid == qtoid
    assert saved_order.asset == "000001.SZ"
    assert saved_order.foid is None
    assert saved_order.cid is None
    assert saved_order.status_msg == ""

    # 02 Test updating order
    db.update_order(qtoid, 
                    status = OrderStatus.REPORTED_CANCEL,
                    status_msg = "Canceled by user",
                    foid = str(1234),
                    cid = "567")
    
    # 03 Verify update
    updated_order = db.get_order(qtoid)
    assert updated_order.status == OrderStatus.REPORTED_CANCEL
    assert updated_order.status_msg == "Canceled by user"
    assert updated_order.foid == str(1234)
    assert updated_order.cid == "567"

    # 04 fetch by foid
    db_order = db.get_order_by_foid(str(1234))
    assert db_order is not None
    assert db_order.qtoid == qtoid
    assert db_order.foid == str(1234)


def test_get_order_by_foid(temp_db_file):
    """Test getting order by external foid"""
    db = TradeDB()
    db.init(temp_db_file)
    
    # Save the order
    db.save_order(asset="000001.SZ",
                  price=10.5,
                  shares=100,
                  side=1,
                  bid_type=0,
                  bid_time=datetime.datetime.now(),
                  qtoid="internal_1",
                  foid = 123
            )
    
    # Retrieve by foid
    retrieved_order = db.get_order_by_foid(123)
    assert retrieved_order is not None
    assert retrieved_order.qtoid == "internal_1"
    assert retrieved_order.foid == "123"

def test_trades_crud(temp_db_file):
    """Test trades CRUD operations with self-contained workflow"""
    db = TradeDB()
    db.init(temp_db_file)
    
    # Create and save multiple trade records
    trade1 = TradeModel(
        tid="trade1",
        qtoid="qtoid1",
        foid="foid1",
        asset="000001.SZ",
        shares=100,
        price=10.5,
        amount=1050.0,
        tm=datetime.datetime.now(),
        side=OrderSide.BUY,
        cid="cid1"
    )
    
    trade2 = TradeModel(
        tid="trade2",
        qtoid="qtoid2",
        foid="foid2",
        asset="000002.SZ",
        shares=200,
        price=20.5,
        amount=4100.0,
        tm=datetime.datetime.now(),
        side=OrderSide.SELL,
        cid="cid2"
    )
    
    trade3 = TradeModel(
        tid="trade3",
        qtoid="qtoid1",  # Same qtoid as trade1 to test query by qtoid
        foid="foid1",    # Same foid as trade1 to test query by foid
        asset="000001.SZ",
        shares=150,
        price=11.0,
        amount=1650.0,
        tm=datetime.datetime.now(),
        side=OrderSide.BUY,
        cid="cid1"
    )
    
    # Test save_trades with multiple trades (batch insert)
    db.save_trades([trade1, trade2, trade3])
    
    # Test get_trade to retrieve a single trade by tid
    retrieved_trade = db.get_trade("trade1")
    assert retrieved_trade is not None
    assert retrieved_trade.tid == "trade1"
    assert retrieved_trade.qtoid == "qtoid1"
    assert retrieved_trade.asset == "000001.SZ"
    assert retrieved_trade.shares == 100
    assert retrieved_trade.price == 10.5
    assert retrieved_trade.amount == 1050.0
    assert retrieved_trade.side == OrderSide.BUY
    
    # Test query_trade to retrieve trades by qtoid
    trades_by_qtoid = db.query_trade(qtoid="qtoid1")
    assert len(trades_by_qtoid) == 2  # Should return trade1 and trade3
    trade_tids = [t.tid for t in trades_by_qtoid]
    assert "trade1" in trade_tids
    assert "trade3" in trade_tids
    
    # Test query_trade to retrieve trades by foid
    trades_by_foid = db.query_trade(foid="foid1")
    assert len(trades_by_foid) == 2  # Should return trade1 and trade3
    trade_tids = [t.tid for t in trades_by_foid]
    assert "trade1" in trade_tids
    assert "trade3" in trade_tids
    
    # Test query_trade with no parameters to get all trades
    all_trades = db.query_trade()
    assert len(all_trades) == 3
    all_tids = [t.tid for t in all_trades]
    assert "trade1" in all_tids
    assert "trade2" in all_tids
    assert "trade3" in all_tids
    
    # Test saving a single trade record
    trade4 = TradeModel(
        tid="trade4",
        qtoid="qtoid4",
        foid="foid4",
        asset="000004.SZ",
        shares=300,
        price=30.5,
        amount=9150.0,
        tm=datetime.datetime.now(),
        side=OrderSide.BUY,
        cid="cid4"
    )
    
    # Save single trade
    db.save_trades(trade4)
    
    # Verify single trade was added
    all_trades = db.query_trade()
    assert len(all_trades) == 4
    trade_tids = [t.tid for t in all_trades]
    assert "trade4" in trade_tids
    
    # Test get_trade for the new single trade
    retrieved_trade4 = db.get_trade("trade4")
    assert retrieved_trade4 is not None
    assert retrieved_trade4.tid == "trade4"
    assert retrieved_trade4.asset == "000004.SZ"
    assert retrieved_trade4.shares == 300
