import pytest
import tempfile
from pathlib import Path
from pyqmt.dal.tradedb import TradeDB
from pyqmt.models import OrderModel, TradeModel, PositionModel, AssetModel
from pyqmt.core.enums import OrderSide, BidType, OrderStatus
import datetime
from dataclasses import asdict

def test_table_creation():
    """Test that all model tables are created during initialization"""
    db = TradeDB()
    db.init(":memory:")
    
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
    assert "oid" in orders_table.pks
    assert "tid" in trades_table.pks


def test_data_insertion_with_write_lock():
    """Test that data can be inserted using write_lock"""
    db = TradeDB()
    db.init(":memory:")
    
    # Test inserting an order using the actual model
    order = OrderModel(
        oid="test_order_1",
        asset="000001.SZ",
        side=1,  # BUY
        shares=100,
        price=10.5,
        bid_type=0,  # FIXED
        tm=datetime.datetime.now()
    )
    
    with db.write_lock:
        db["orders"].insert(asdict(order))
    
    # Verify insertion
    result = list(db["orders"].rows)
    assert len(result) == 1
    assert result[0]["oid"] == "test_order_1"
    assert result[0]["asset"] == "000001.SZ"


def test_index_creation():
    """Test that indexes are created correctly"""
    db = TradeDB()
    db.init(":memory:")
    
    # Check that indexes exist by trying to insert duplicate data
    # This will depend on your exact index configuration
    
    order1 = OrderModel(
        oid="test_order_1",
        fid="external_1",
        asset="000001.SZ",
        side=1,
        shares=100,
        price=10.5,
        bid_type=0,
        tm=datetime.datetime.now()
    )
    
    order2 = OrderModel(
        oid="test_order_2",
        fid="external_1",  # Same fid
        asset="000002.SZ",
        side=1,
        shares=200,
        price=11.5,
        bid_type=0,
        tm=datetime.datetime.now()
    )
    
    # Insert first order
    with db.write_lock:
        db["orders"].insert(asdict(order1))
    
    # Insert second order - should work if index allows duplicates
    with db.write_lock:
        db["orders"].insert(asdict(order2))


def test_order():
    """Test order CRUD """
    db = TradeDB()
    db.init(":memory:")
    
    order = OrderModel(
        oid="test_order_1",
        asset="000001.SZ",
        side=1,
        shares=100,
        price=10.5,
        bid_type=0,
        tm=datetime.datetime.now()
    )
    
    # 01 Test saving order
    db.save_order(order)
    
    # Verify it was saved
    saved_order = db.get_order("test_order_1")
    assert saved_order is not None
    assert saved_order.oid == "test_order_1"
    assert saved_order.asset == "000001.SZ"
    assert saved_order.fid is None
    assert saved_order.cid is None
    assert saved_order.status_msg == ""

    # 02 Test updating order
    saved_order.status = OrderStatus.REPORTED_CANCEL
    saved_order.status_msg = "Canceled by user"
    saved_order.fid = str(1234)
    saved_order.cid = "567"
    db.update_order(saved_order)
    
    # 03 Verify update
    updated_order = db.get_order("test_order_1")
    assert updated_order.status == OrderStatus.REPORTED_CANCEL
    assert updated_order.status_msg == "Canceled by user"
    assert updated_order.fid == str(1234)
    assert updated_order.cid == "567"

    # 04 fetch by fid
    db_order = db.get_order_by_fid(str(1234))
    assert db_order is not None
    assert db_order.oid == "test_order_1"
    assert db_order.fid == str(1234)


def test_get_order_by_fid():
    """Test getting order by external fid"""
    db = TradeDB()
    db.init(":memory:")
    
    order = OrderModel(
        oid="internal_1",
        fid="external_123",
        asset="000001.SZ",
        side=1,
        shares=100,
        price=10.5,
        bid_type=0,
        tm=datetime.datetime.now()
    )
    
    # Save the order
    db.save_order(order)
    
    # Retrieve by fid
    retrieved_order = db.get_order_by_fid("external_123")
    assert retrieved_order is not None
    assert retrieved_order.oid == "internal_1"
    assert retrieved_order.fid == "external_123"
