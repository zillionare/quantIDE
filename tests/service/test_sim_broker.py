import asyncio
import datetime
import os
import sys
from unittest.mock import MagicMock, patch

import cfg4py
import polars as pl
import pytest

# --- Mock Config Setup ---
# Initialize config mock before importing modules that use it
# We need to mock cfg4py.get_instance() to return a mock object that has:
# 1. server.TRADE_ADDRESS
# 2. server.get("quote_mode") -> "qmt"
# 3. get("redis") -> None (or whatever logic requires)

mock_config_instance = MagicMock()
mock_config_instance.server = MagicMock()
mock_config_instance.server.TRADE_ADDRESS = "tcp://localhost:5555"
# Allow dict-like access or attribute access for 'get' depending on usage
# If code uses cfg.server["quote_mode"], we need __getitem__
# If code uses cfg.server.get("quote_mode"), we need get method
mock_config_instance.server.get.return_value = "qmt"
mock_config_instance.server.__getitem__.side_effect = lambda k: "qmt" if k == "quote_mode" else None

# Mock cfg.get()
mock_config_instance.get.return_value = None

# Patch cfg4py
patcher_cfg4py = patch('cfg4py.get_instance', return_value=mock_config_instance)
patcher_cfg4py.start()

# Patch pyqmt.config.Config
patcher_config_cls = patch('pyqmt.config.Config', return_value=mock_config_instance)
patcher_config_cls.start()

# Patch xtquant modules if they don't exist
if 'xtquant' not in sys.modules:
    sys.modules['xtquant'] = MagicMock()
if 'xtquant.xtdata' not in sys.modules:
    sys.modules['xtquant.xtdata'] = MagicMock()

# --- Imports after mocking ---
from pyqmt.core.enums import BrokerKind, OrderSide, OrderStatus
from pyqmt.core.errors import (
    InsufficientCash,
    InsufficientPosition,
    NonMultipleOfLotSize,
)
from pyqmt.data.sqlite import Asset, Order, Portfolio, Position, Trade, db
from pyqmt.service.sim_broker import SimulationBroker

# --- Fixtures ---

@pytest.fixture
def mock_live_quote():
    with patch("pyqmt.service.sim_broker.live_quote") as mock:
        yield mock

@pytest.fixture
def broker(mock_live_quote):
    """Standard broker fixture using in-memory database."""
    db.init(":memory:")
    b = SimulationBroker(
        portfolio_id="test_sim",
        principal=1000000,
        portfolio_name="Test Simulation",
    )
    return b

@pytest.fixture
def persistence_db(tmp_path):
    """Fixture for file-based database tests."""
    db_path = tmp_path / "test_persistence.db"
    db.init(str(db_path))
    yield str(db_path)
    # Cleanup is handled by tmp_path, but we might want to ensure db is closed or reset
    # db.init(":memory:") # Reset to memory after test? Not strictly necessary if next test inits.

# --- Basic Functionality Tests ---

def test_init(broker):
    p = db.get_portfolio("test_sim")
    assert p is not None
    assert p.portfolio_id == "test_sim"

    asset = db.get_asset(dt=datetime.date.today(), portfolio_id="test_sim")
    assert asset is not None
    assert asset.cash == 1000000
    assert asset.total == 1000000

@pytest.mark.asyncio
async def test_buy(broker, mock_live_quote):
    # Mock quote for validation and matching
    mock_live_quote.get_quote.return_value = {
        "lastPrice": 10.0,
        "upLimit": 11.0,
        "downLimit": 9.0,
    }

    task = asyncio.create_task(broker.buy("600000.SH", 100, 10.0))

    # Give it a moment to register order
    await asyncio.sleep(0.1)

    # Verify order is active
    assert "600000.SH" in broker._active_orders
    order = broker._active_orders["600000.SH"][0]
    assert order.status == OrderStatus.UNREPORTED

    # Trigger quote update
    quote_data = {
        "600000.SH": {
            "lastPrice": 10.0,
            "upLimit": 11.0,
            "downLimit": 9.0,
        }
    }
    broker._on_quote_update(quote_data)

    # Wait for result
    res = await task

    assert res is not None
    assert res.qt_oid == order.qtoid
    assert len(res.trades) == 1
    trade = res.trades[0]
    assert trade.asset == "600000.SH"
    assert trade.shares == 100
    assert trade.price == 10.0

    # Check DB
    db_order = db.get_order(order.qtoid)
    assert db_order.status == OrderStatus.SUCCEEDED.value

    trades = db.trades_all("test_sim")
    assert len(trades) == 1

    # Check Position
    pos = db.get_positions(portfolio_id="test_sim").row(0, named=True)
    assert pos["asset"] == "600000.SH"
    assert pos["shares"] == 100

    # Check Cash
    # 100 * 10 = 1000 + commission
    # Commission = max(5, 1000 * 1e-4) = 5
    # Cash = 1000000 - 1005 = 998995
    asset = db.get_asset(portfolio_id="test_sim")
    assert asset.cash == 998995.0

@pytest.mark.asyncio
async def test_sell(broker, mock_live_quote):
    # Setup position
    mock_live_quote.get_quote.return_value = {"lastPrice": 10.0}

    # Manually insert position
    p = Position(
        portfolio_id="test_sim",
        dt=datetime.date.today(),
        asset="600000.SH",
        shares=200,
        avail=200, # Need avail to sell
        price=10.0,
        mv=2000,
        profit=0
    )
    broker._positions["600000.SH"] = p
    db.upsert_positions(p)

    # Sell 100
    task = asyncio.create_task(broker.sell("600000.SH", 100, 10.0))
    await asyncio.sleep(0.1)

    quote_data = {
        "600000.SH": {
            "lastPrice": 10.0,
            "upLimit": 11.0,
            "downLimit": 9.0,
        }
    }
    broker._on_quote_update(quote_data)

    res = await task
    assert len(res.trades) == 1

    # Check remaining position
    pos = broker._positions["600000.SH"]
    assert pos.shares == 100

    # Check DB
    db_pos = db.get_positions(portfolio_id="test_sim").row(0, named=True)
    assert db_pos["shares"] == 100

@pytest.mark.asyncio
async def test_insufficient_cash(broker, mock_live_quote):
    mock_live_quote.get_quote.return_value = {"lastPrice": 10.0}
    with pytest.raises(InsufficientCash):
        await broker.buy("600000.SH", 200000, 10.0)

@pytest.mark.asyncio
async def test_insufficient_position(broker):
    with pytest.raises(InsufficientPosition):
        await broker.sell("600000.SH", 100, 10.0)

@pytest.mark.asyncio
async def test_cancel_order(broker, mock_live_quote):
    mock_live_quote.get_quote.return_value = {
        "lastPrice": 10.0,
        "upLimit": 11.0,
        "downLimit": 9.0,
    }

    # Place a limit buy order at a low price so it doesn't fill immediately
    task = asyncio.create_task(broker.buy("600000.SH", 100, 9.0))
    await asyncio.sleep(0.1)

    order = broker._active_orders["600000.SH"][0]

    # Cancel order
    await broker.cancel_order(order.qtoid)

    # Wait for task to complete (it should return with empty trades)
    res = await task
    assert res is not None
    assert len(res.trades) == 0

    # Verify order is removed from active orders
    if "600000.SH" in broker._active_orders:
        assert len(broker._active_orders["600000.SH"]) == 0

    # Verify DB status
    db_order = db.get_order(order.qtoid)
    assert db_order.status == OrderStatus.CANCELED.value

@pytest.mark.asyncio
async def test_cancel_all_orders(broker, mock_live_quote):
    mock_live_quote.get_quote.return_value = {"lastPrice": 10.0, "upLimit": 11.0, "downLimit": 9.0}

    task1 = asyncio.create_task(broker.buy("600000.SH", 100, 9.0))
    task2 = asyncio.create_task(broker.buy("600001.SH", 100, 9.0))
    await asyncio.sleep(0.1)

    await broker.cancel_all_orders()

    res1 = await task1
    res2 = await task2

    assert len(res1.trades) == 0
    assert len(res2.trades) == 0
    assert "600000.SH" not in broker._active_orders
    assert "600001.SH" not in broker._active_orders

@pytest.mark.asyncio
async def test_on_day_close(broker, mock_live_quote):
    p = Position(
        portfolio_id="test_sim",
        dt=datetime.date.today(),
        asset="600000.SH",
        shares=1000,
        avail=1000,
        price=10.0,
        mv=10000,
        profit=0
    )
    broker._positions["600000.SH"] = p
    broker._cash = 990000

    mock_live_quote.get_quote.return_value = {"lastPrice": 11.0}

    # Call on_day_close with specific close prices
    close_prices = {"600000.SH": 12.0}
    await broker.on_day_close(close_prices)

    db_pos = db.get_positions(portfolio_id="test_sim").row(0, named=True)
    assert db_pos["asset"] == "600000.SH"
    assert db_pos["dt"] == datetime.date.today()
    assert db_pos["mv"] == 12000.0
    assert db_pos["profit"] == 2000.0

    asset = db.get_asset(portfolio_id="test_sim")
    assert asset.market_value == 12000.0
    assert asset.cash == 990000.0
    assert asset.total == 990000.0 + 12000.0

    # Test fallback to live quote
    p2 = Position(
        portfolio_id="test_sim",
        dt=datetime.date.today(),
        asset="600001.SH",
        shares=1000,
        avail=1000,
        price=20.0,
        mv=20000,
        profit=0
    )
    broker._positions["600001.SH"] = p2

    def side_effect(asset):
        if asset == "600001.SH":
            return {"lastPrice": 22.0}
        return {}
    mock_live_quote.get_quote.side_effect = side_effect

    await broker.on_day_close(close_prices={})

    df = db.get_positions(portfolio_id="test_sim")
    row = df.filter(pl.col("asset") == "600001.SH").row(0, named=True)
    assert row["mv"] == 22000.0
    assert row["profit"] == 2000.0

@pytest.mark.asyncio
async def test_total_assets_update(broker, mock_live_quote):
    p = Position(
        portfolio_id="test_sim",
        dt=datetime.date.today(),
        asset="600000.SH",
        shares=100,
        avail=100,
        price=10.0,
        mv=1000.0,
        profit=0
    )
    broker._positions["600000.SH"] = p
    broker._cash = 10000.0

    assert broker.total_assets == 11000.0

    quote_data = {
        "600000.SH": {
            "lastPrice": 11.0,
        }
    }
    broker._on_quote_update(quote_data)

    assert broker._positions["600000.SH"].mv == 1100.0
    assert broker.total_assets == 11100.0


# --- Partial Fill & Volume Tests ---

@pytest.mark.asyncio
async def test_partial_fill_with_volume_limit(broker, mock_live_quote):
    # Overwrite broker fixture portfolio_id just in case, but safe to reuse test_sim
    # as db is :memory: and reset per test
    asset = "000001.SZ"

    async def push_quotes():
        await asyncio.sleep(0.1)
        # 1st push: 200 shares
        broker._on_quote_update({
            asset: {
                "lastPrice": 10.0,
                "volume": 2,
                "upLimit": 11.0,
                "downLimit": 9.0
            }
        })

        await asyncio.sleep(0.1)
        # 2nd push: 800 shares
        broker._on_quote_update({
            asset: {
                "lastPrice": 10.1,
                "volume": 10,
                "upLimit": 11.0,
                "downLimit": 9.0
            }
        })

    asyncio.create_task(push_quotes())

    res = await broker.buy(asset, 1000, price=10.5, timeout=1.0)

    assert res is not None
    assert len(res.trades) == 2

    t1 = res.trades[0]
    t2 = res.trades[1]

    assert t1.shares == 200
    assert t1.price == 10.0
    assert t2.shares == 800
    assert t2.price == 10.1
    assert broker._positions[asset].shares == 1000

    order = db.get_order(res.qt_oid)
    assert order.status.name == "SUCCEEDED"
    assert order.filled == 1000

@pytest.mark.asyncio
async def test_partial_fill_timeout(broker):
    asset = "000002.SZ"

    async def push_quotes():
        await asyncio.sleep(0.1)
        # 1st push: 200 shares
        broker._on_quote_update({
            asset: {
                "lastPrice": 10.0,
                "volume": 2,
                "upLimit": 11.0,
                "downLimit": 9.0
            }
        })
        # No 2nd push, let it timeout

    asyncio.create_task(push_quotes())

    res = await broker.buy(asset, 1000, price=10.5, timeout=0.5)

    assert len(res.trades) == 1
    assert res.trades[0].shares == 200

    order = db.get_order(res.qt_oid)
    assert order.status.name == "PART_SUCC"
    assert order.filled == 200
    assert broker._positions[asset].shares == 200

@pytest.mark.asyncio
async def test_volume_consumption_across_orders(broker, mock_live_quote):
    """测试同一 tick 内多个订单对成交量的消耗"""
    # 1. 模拟两个买单，分别需要 1000 股和 2000 股
    task1 = asyncio.create_task(broker.buy("000001", 1000, 10.0))
    task2 = asyncio.create_task(broker.buy("000001", 2000, 10.0))
    await asyncio.sleep(0.01) # 等待订单注册

    # 2. 推送行情：只有 15 手（1500 股）成交量
    # broker 处理逻辑是 active_orders 列表顺序撮合
    mock_live_quote.get_quote.return_value = {
        "lastPrice": 10.0,
        "volume": 15, # 15手 = 1500股
        "upLimit": 11.0,
        "downLimit": 9.0
    }
    broker._on_quote_update({"000001": mock_live_quote.get_quote.return_value})

    res1 = await task1
    res2 = await task2

    # 3. 验证结果
    # 订单1 (1000股) 应该完全成交
    assert res1.qt_oid != ""
    assert len(res1.trades) == 1
    assert res1.trades[0].shares == 1000

    # 订单2 (2000股) 应该只成交 500 股 (1500 - 1000)
    assert res2.qt_oid != ""
    # 注意：如果不完全成交且超时，这里返回的可能是空 trades (如果 timeout 到了)
    # 或者如果没超时，它还在等。
    # 在我们的测试里，buy 默认 timeout 0.5s。
    # 刚才 _on_quote_update 触发了一次撮合。
    # 订单2 变成了 PART_SUCC。buy() 方法还在 await self.wait_for_trade(timeout)
    # 因为没有完全成交，所以它会继续等，直到超时。

    # 检查数据库状态确认部分成交
    order2 = db.get_order(res2.qt_oid)
    assert order2.filled == 500
    assert order2.status == OrderStatus.PART_SUCC.value

@pytest.mark.asyncio
async def test_volume_consumption_exhausted(broker):
    asset = "000003.SZ"

    task1 = asyncio.create_task(broker.buy(asset, 200, price=10.0, timeout=1.0))
    await asyncio.sleep(0.01)
    task2 = asyncio.create_task(broker.buy(asset, 200, price=10.0, timeout=1.0))

    await asyncio.sleep(0.01)

    # Push quote with 2 hands = 200 shares (only enough for first order)
    broker._on_quote_update({
        asset: {
            "lastPrice": 10.0,
            "volume": 2,
            "upLimit": 11.0,
            "downLimit": 9.0
        }
    })

    res1 = await task1

    assert len(res1.trades) == 1
    assert res1.trades[0].shares == 200

    # Push another quote to fill second order
    broker._on_quote_update({
        asset: {
            "lastPrice": 10.0,
            "volume": 2,
            "upLimit": 11.0,
            "downLimit": 9.0
        }
    })

    res2 = await task2
    assert len(res2.trades) == 1
    assert res2.trades[0].shares == 200

@pytest.mark.asyncio
async def test_no_volume_in_quote_full_match(broker):
    """Test that if quote has no volume, order matches fully."""
    asset = "000001.SZ"

    task = asyncio.create_task(broker.buy(asset, 1000, price=10.0, timeout=1.0))

    await asyncio.sleep(0.01)

    broker._on_quote_update({
        asset: {
            "lastPrice": 10.0,
            # No "volume" field
            "upLimit": 11.0,
            "downLimit": 9.0
        }
    })

    res = await task

    assert len(res.trades) == 1
    assert res.trades[0].shares == 1000
    assert broker._positions[asset].shares == 1000


# --- Persistence Tests (File DB) ---

def test_trade_persistence_and_recovery(persistence_db):
    """Test persistence and recovery using a file-based database."""
    # persistence_db fixture initializes db with file path
    portfolio_id = "test_persist_pid"

    # --- Stage 1: Run broker, buy ---
    broker1 = SimulationBroker(portfolio_id, principal=100000)

    async def run_buy():
        quote = {
            "000001": {
                "lastPrice": 10.0,
                "volume": 1000,
                "upLimit": 11.0,
                "downLimit": 9.0
            }
        }
        task = asyncio.create_task(broker1.buy("000001", 100, price=10.0))
        await asyncio.sleep(0.1)
        broker1._on_quote_update(quote)
        return await task

    res = asyncio.run(run_buy())

    assert len(res.trades) == 1
    assert "000001" in broker1._positions
    assert broker1._positions["000001"].shares == 100

    # Verify DB (Stage 1)
    trades_df = db.query_trade(qtoid=res.qt_oid)
    assert trades_df is not None
    assert len(trades_df) == 1

    pos_df = db.get_positions(dt=datetime.date.today(), portfolio_id=portfolio_id)
    assert not pos_df.is_empty()
    pos_dict = pos_df.row(0, named=True)
    assert pos_dict["asset"] == "000001"
    assert pos_dict["shares"] == 100

    asset_rec = db.get_asset(dt=None, portfolio_id=portfolio_id)
    # 100000 - 1000 - 5 = 98995
    assert asset_rec.cash == 98995.0

    # --- Stage 2: Restart ---
    del broker1

    # We don't need to re-init db because it's the same file path, but SimulationBroker reads from it.
    broker2 = SimulationBroker(portfolio_id)

    assert "000001" in broker2._positions
    restored_pos = broker2._positions["000001"]
    assert restored_pos.shares == 100
    assert restored_pos.price == 10.0
    assert broker2._cash == 98995.0

def test_historical_position_loading(persistence_db):
    """Test loading positions from latest date."""
    portfolio_id = "test_hist_pid"
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    pf = Portfolio(
        portfolio_id=portfolio_id,
        kind=BrokerKind.SIMULATION,
        start=yesterday,
        name="Test Hist",
        info="Test"
    )
    db.insert_portfolio(pf)

    # Insert yesterday's position
    pos_prev = Position(
        portfolio_id=portfolio_id,
        dt=yesterday,
        asset="000001",
        shares=200,
        avail=200,
        price=9.0,
        mv=1800,
        profit=0
    )
    db.upsert_positions(pos_prev)

    asset_prev = Asset(
        portfolio_id=portfolio_id,
        dt=yesterday,
        principal=100000,
        cash=100000,
        frozen_cash=0,
        market_value=1800,
        total=101800
    )
    db.upsert_asset(asset_prev)

    # Start Broker
    broker = SimulationBroker(portfolio_id)
    assert "000001" in broker._positions
    assert broker._positions["000001"].shares == 200

    # Simulate today's position update (as if trade happened today)
    pos_curr = Position(
        portfolio_id=portfolio_id,
        dt=today,
        asset="000001",
        shares=300,
        avail=300,
        price=9.5,
        mv=3000,
        profit=100
    )
    db.upsert_positions(pos_curr)

    # Restart Broker
    broker2 = SimulationBroker(portfolio_id)
    assert broker2._positions["000001"].shares == 300


# --- Additional Coverage Tests ---

def test_init_consistency_missing_portfolio_has_asset(broker):
    # broker fixture inits memory db.
    # We need to manually create inconsistent state.
    # Create Asset but no Portfolio
    db.init(":memory:") # Reset

    asset = Asset(
        portfolio_id="bad_state",
        dt=datetime.date.today(),
        principal=1000,
        cash=1000,
        frozen_cash=0,
        market_value=0,
        total=1000
    )
    db.upsert_asset(asset)

    with pytest.raises(RuntimeError, match="Portfolio bad_state missing but has asset records"):
        SimulationBroker("bad_state")

def test_init_consistency_missing_portfolio_has_positions(broker):
    db.init(":memory:")

    p = Position(
        portfolio_id="bad_state_2",
        dt=datetime.date.today(),
        asset="000001",
        shares=100,
        avail=100,
        price=10,
        mv=1000,
        profit=0
    )
    db.upsert_positions(p)

    with pytest.raises(RuntimeError, match="Portfolio bad_state_2 missing but has position records"):
        SimulationBroker("bad_state_2")

def test_init_consistency_has_portfolio_missing_asset(broker):
    db.init(":memory:")

    pf = Portfolio(
        portfolio_id="bad_state_3",
        kind=BrokerKind.SIMULATION,
        start=datetime.date.today(),
        name="Test",
        info=""
    )
    db.insert_portfolio(pf)

    with pytest.raises(RuntimeError, match="Portfolio bad_state_3 exists but has no asset records"):
        SimulationBroker("bad_state_3")

@pytest.mark.asyncio
async def test_buy_percent(broker, mock_live_quote):
    # Cash 1M. Price 10.
    # Buy 50% -> 500k. Shares = 500k / 10 = 50k.
    mock_live_quote.get_quote.return_value = {"lastPrice": 10.0, "upLimit": 11.0, "downLimit": 9.0}

    task = asyncio.create_task(broker.buy_percent("000001", 0.5))
    await asyncio.sleep(0.01)

    # Check active order
    assert "000001" in broker._active_orders
    order = broker._active_orders["000001"][0]
    assert order.shares == 50000

    # Cleanup
    await broker.cancel_all_orders()
    await task

@pytest.mark.asyncio
async def test_buy_amount(broker, mock_live_quote):
    mock_live_quote.get_quote.return_value = {"lastPrice": 10.0, "upLimit": 11.0, "downLimit": 9.0}

    # Buy 100k amount -> 10k shares
    task = asyncio.create_task(broker.buy_amount("000001", 100000))
    await asyncio.sleep(0.01)

    order = broker._active_orders["000001"][0]
    assert order.shares == 10000

    await broker.cancel_all_orders()
    await task

@pytest.mark.asyncio
async def test_sell_percent(broker, mock_live_quote):
    # Setup position: 1000 shares
    p = Position(
        portfolio_id="test_sim",
        dt=datetime.date.today(),
        asset="000001",
        shares=1000,
        avail=1000,
        price=10.0,
        mv=10000,
        profit=0
    )
    broker._positions["000001"] = p

    # Sell 50% -> 500 shares
    task = asyncio.create_task(broker.sell_percent("000001", 0.5))
    await asyncio.sleep(0.01)

    order = broker._active_orders["000001"][0]
    assert order.shares == 500
    assert order.side == OrderSide.SELL

    await broker.cancel_all_orders()
    await task

    # Test clear all (100%)
    task2 = asyncio.create_task(broker.sell_percent("000001", 1.0))
    await asyncio.sleep(0.01)
    order2 = broker._active_orders["000001"][0]
    assert order2.shares == 1000

    await broker.cancel_all_orders()
    await task2

@pytest.mark.asyncio
async def test_sell_amount(broker, mock_live_quote):
    p = Position(
        portfolio_id="test_sim",
        dt=datetime.date.today(),
        asset="000001",
        shares=1000,
        avail=1000,
        price=10.0,
        mv=10000,
        profit=0
    )
    broker._positions["000001"] = p
    mock_live_quote.get_quote.return_value = {"lastPrice": 10.0}

    # Sell 5000 amount -> 500 shares
    task = asyncio.create_task(broker.sell_amount("000001", 5000))
    await asyncio.sleep(0.01)

    order = broker._active_orders["000001"][0]
    assert order.shares == 500

    await broker.cancel_all_orders()
    await task

@pytest.mark.asyncio
async def test_trade_target_pct(broker, mock_live_quote):
    # Initial: Cash 1M. Position 0. Total 1M.
    mock_live_quote.get_quote.return_value = {"lastPrice": 10.0, "upLimit": 11.0, "downLimit": 9.0}

    # Target 10% -> 100k value -> 10k shares
    task = asyncio.create_task(broker.trade_target_pct("000001", 0.1))
    await asyncio.sleep(0.01)

    order = broker._active_orders["000001"][0]
    assert order.shares == 10000
    assert order.side == OrderSide.BUY

    await broker.cancel_all_orders()
    await task

    # Now simulate holding 20% (200k value, 20k shares)
    p = Position(
        portfolio_id="test_sim",
        dt=datetime.date.today(),
        asset="000001",
        shares=20000,
        avail=20000,
        price=10.0,
        mv=200000,
        profit=0
    )
    broker._positions["000001"] = p
    broker._cash = 800000
    # Total = 1M

    # Target 10% -> Should sell 10% (100k value -> 10k shares)
    task2 = asyncio.create_task(broker.trade_target_pct("000001", 0.1))
    await asyncio.sleep(0.01)

    order2 = broker._active_orders["000001"][0]
    assert order2.shares == 10000
    assert order2.side == OrderSide.SELL

    await broker.cancel_all_orders()
    await task2

@pytest.mark.asyncio
async def test_price_limits(broker, mock_live_quote):
    # Test Buy > UpLimit
    mock_live_quote.get_quote.return_value = {"lastPrice": 11.0, "upLimit": 10.0, "downLimit": 8.0}

    task = asyncio.create_task(broker.buy("000001", 100, 11.0)) # Price > UpLimit? Logic checks LastPrice vs UpLimit
    # Logic: if order.side == OrderSide.BUY and up_limit > 0 and last_price >= up_limit: return 0.0, None

    await asyncio.sleep(0.01)
    broker._on_quote_update({"000001": {"lastPrice": 10.0, "upLimit": 10.0, "downLimit": 8.0}}) # At limit

    # Actually, the logic checks LAST PRICE vs limit. If last_price >= up_limit, cannot buy.
    # Let's push a quote where lastPrice = upLimit
    broker._on_quote_update({"000001": {"lastPrice": 10.0, "upLimit": 10.0, "downLimit": 8.0}})

    # Wait a bit. Should NOT match.
    await asyncio.sleep(0.1)

    # Check if matched. If not matched, active orders should still have it.
    assert "000001" in broker._active_orders

    # Now push normal quote
    broker._on_quote_update({"000001": {"lastPrice": 9.0, "upLimit": 10.0, "downLimit": 8.0}})
    res = await task
    assert len(res.trades) == 1

@pytest.mark.asyncio
async def test_cancel_partially_filled_order(broker, mock_live_quote):
    asset = "000001"
    mock_live_quote.get_quote.return_value = {"lastPrice": 10.0, "upLimit": 11.0, "downLimit": 9.0}

    task = asyncio.create_task(broker.buy(asset, 1000, 10.0))
    await asyncio.sleep(0.01)

    # Partial fill
    broker._on_quote_update({
        asset: {
            "lastPrice": 10.0,
            "volume": 2, # 200 shares
            "upLimit": 11.0,
            "downLimit": 9.0
        }
    })

    await asyncio.sleep(0.01)

    # Cancel
    order = broker._active_orders[asset][0]
    await broker.cancel_order(order.qtoid)

    res = await task
    assert len(res.trades) == 1
    assert res.trades[0].shares == 200

    db_order = db.get_order(order.qtoid)
    assert db_order.status == OrderStatus.PARTSUCC_CANCEL.value

@pytest.mark.asyncio
async def test_on_day_open(broker):
    await broker.on_day_open()
    # Just ensure it runs without error

@pytest.mark.asyncio
async def test_price_limit_rejects(broker, mock_live_quote):
    """Test price limit and fixed price logic in _try_match."""
    # 1. Buy at Limit Up
    mock_live_quote.get_quote.return_value = {"lastPrice": 11.0, "upLimit": 11.0, "downLimit": 9.0, "volume": 1000}
    task = asyncio.create_task(broker.buy("000001", 100))
    await asyncio.sleep(0.01)
    # Trigger match logic
    broker._on_quote_update({"000001": mock_live_quote.get_quote.return_value})
    # Order should NOT be filled
    assert len(broker._active_orders["000001"]) == 1
    assert broker._active_orders["000001"][0].filled == 0
    await broker.cancel_all_orders()
    await task

    # 2. Sell at Limit Down
    # Need position first
    broker._positions["000001"] = Position(
        portfolio_id="test_sim", dt=datetime.date.today(), asset="000001",
        shares=100, price=10.0, avail=100, mv=1000, profit=0
    )
    mock_live_quote.get_quote.return_value = {"lastPrice": 9.0, "upLimit": 11.0, "downLimit": 9.0, "volume": 1000}
    task = asyncio.create_task(broker.sell("000001", 100))
    await asyncio.sleep(0.01)
    broker._on_quote_update({"000001": mock_live_quote.get_quote.return_value})
    assert len(broker._active_orders["000001"]) == 1
    assert broker._active_orders["000001"][0].filled == 0
    await broker.cancel_all_orders()
    await task

    # 3. Fixed Buy Price too low
    mock_live_quote.get_quote.return_value = {"lastPrice": 10.0, "upLimit": 11.0, "downLimit": 9.0, "volume": 1000}
    task = asyncio.create_task(broker.buy("000001", 100, price=9.9))
    await asyncio.sleep(0.01)
    broker._on_quote_update({"000001": mock_live_quote.get_quote.return_value})
    assert len(broker._active_orders["000001"]) == 1
    assert broker._active_orders["000001"][0].filled == 0
    await broker.cancel_all_orders()
    await task

    # 4. Fixed Sell Price too high
    task = asyncio.create_task(broker.sell("000001", 100, price=10.1))
    await asyncio.sleep(0.01)
    broker._on_quote_update({"000001": mock_live_quote.get_quote.return_value})
    assert len(broker._active_orders["000001"]) == 1
    assert broker._active_orders["000001"][0].filled == 0
    await broker.cancel_all_orders()
    await task

    # 5. Last Price Invalid
    mock_live_quote.get_quote.return_value = {"lastPrice": 0.0, "volume": 1000}
    task = asyncio.create_task(broker.buy("000001", 100))
    await asyncio.sleep(0.01)
    broker._on_quote_update({"000001": mock_live_quote.get_quote.return_value})
    assert len(broker._active_orders["000001"]) == 1
    assert broker._active_orders["000001"][0].filled == 0
    await broker.cancel_all_orders()
    await task


@pytest.mark.asyncio
async def test_sell_all_removes_position(broker, mock_live_quote):
    """Test that selling all shares removes the position from memory."""
    broker._positions["000001"] = Position(
        portfolio_id="test_sim", dt=datetime.date.today(), asset="000001",
        shares=100, price=10.0, avail=100, mv=1000, profit=0
    )
    mock_live_quote.get_quote.return_value = {"lastPrice": 10.0, "volume": 10000}

    # Sell all
    task = asyncio.create_task(broker.sell("000001", 100))
    await asyncio.sleep(0.01)
    broker._on_quote_update({"000001": mock_live_quote.get_quote.return_value})
    await task

    assert "000001" not in broker._positions


@pytest.mark.asyncio
async def test_proportional_trading_edge_cases(broker, mock_live_quote):
    """Test edge cases for buy/sell percent/amount."""
    # buy_percent
    mock_live_quote.get_quote.return_value = None
    res = await broker.buy_percent("000001", 0.5)
    assert res.qt_oid == ""

    mock_live_quote.get_quote.return_value = {"lastPrice": 0}
    res = await broker.buy_percent("000001", 0.5)
    assert res.qt_oid == ""

    mock_live_quote.get_quote.return_value = {"lastPrice": 1000000} # Too expensive
    res = await broker.buy_percent("000001", 0.5) # 1M * 0.5 = 500k. Price 1M. Shares = 0.
    assert res.qt_oid == ""

    # buy_amount
    mock_live_quote.get_quote.return_value = None
    res = await broker.buy_amount("000001", 10000)
    assert res.qt_oid == ""

    mock_live_quote.get_quote.return_value = {"lastPrice": 0}
    res = await broker.buy_amount("000001", 10000)
    assert res.qt_oid == ""

    mock_live_quote.get_quote.return_value = {"lastPrice": 10000}
    res = await broker.buy_amount("000001", 100) # 100 < 10000 * 100
    assert res.qt_oid == ""

    # sell_percent
    res = await broker.sell_percent("INVALID", 0.5)
    assert res.qt_oid == ""

    broker._positions["000001"] = Position(
        portfolio_id="test_sim", dt=datetime.date.today(), asset="000001",
        shares=100, price=10.0, avail=100, mv=1000, profit=0
    )
    res = await broker.sell_percent("000001", 0.001) # Very small percent -> 0 shares
    assert res.qt_oid == ""

    # sell_amount
    mock_live_quote.get_quote.return_value = None
    res = await broker.sell_amount("000001", 1000)
    assert res.qt_oid == ""

    mock_live_quote.get_quote.return_value = {"lastPrice": 0}
    res = await broker.sell_amount("000001", 1000)
    assert res.qt_oid == ""

    mock_live_quote.get_quote.return_value = {"lastPrice": 10000}
    res = await broker.sell_amount("000001", 100) # Small amount -> 0 shares
    assert res.qt_oid == ""

    # trade_target_pct
    mock_live_quote.get_quote.return_value = None
    res = await broker.trade_target_pct("000001", 0.5)
    assert res.qt_oid == ""

    mock_live_quote.get_quote.return_value = {"lastPrice": 0}
    res = await broker.trade_target_pct("000001", 0.5)
    assert res.qt_oid == ""

    # Target same as current
    broker._cash = 0
    # Pos: 100 shares * 10.0 = 1000 MV. Total = 1000.
    # Target 1.0 (100%) -> 1000. Current 1000. Diff 0.
    mock_live_quote.get_quote.return_value = {"lastPrice": 10.0}
    res = await broker.trade_target_pct("000001", 1.0)
    assert res.qt_oid == ""

    # Target clear (small target)
    # Total 1000. Target 0.
    # res = await broker.trade_target_pct("000001", 0.0)  <-- Removed this blocking call that caused duplicate order

    # We run it in a task to avoid waiting for timeout
    task = asyncio.create_task(broker.trade_target_pct("000001", 0.0))
    await asyncio.sleep(0.01)
    # Check if sell order created
    assert len(broker._active_orders["000001"]) == 1
    assert broker._active_orders["000001"][0].side == OrderSide.SELL
    assert broker._active_orders["000001"][0].shares == 100
    await broker.cancel_all_orders()
    await task


@pytest.mark.asyncio
async def test_cancel_all_orders_coverage(broker, mock_live_quote):
    """Test cancel_all_orders with side filtering and partial fills."""
    mock_live_quote.get_quote.return_value = {"lastPrice": 10.0, "volume": 1} # Small volume (1 hand = 100 shares)

    # 1. Create Buy Order
    task_buy = asyncio.create_task(broker.buy("000001", 1000))
    await asyncio.sleep(0.01)

    # 2. Create Sell Order (need pos)
    broker._positions["000002"] = Position(
        portfolio_id="test_sim", dt=datetime.date.today(), asset="000002",
        shares=1000, price=10.0, avail=1000, mv=10000, profit=0
    )
    task_sell = asyncio.create_task(broker.sell("000002", 1000))
    await asyncio.sleep(0.01)

    # 3. Partially fill Buy Order
    broker._on_quote_update({"000001": mock_live_quote.get_quote.return_value})
    # Now buy order should be partially filled (100 shares)

    # 4. Cancel only Sell orders
    await broker.cancel_all_orders(side=OrderSide.SELL)

    # Sell order should be canceled
    assert "000002" not in broker._active_orders

    # Buy order should remain
    assert "000001" in broker._active_orders
    order_buy = broker._active_orders["000001"][0]
    assert order_buy.filled == 100
    assert order_buy.status == OrderStatus.PART_SUCC

    # 5. Cancel Buy order (partially filled)
    await broker.cancel_all_orders(side=OrderSide.BUY)

    # Verify status became PARTSUCC_CANCEL
    # Note: the order object in memory is updated.
    assert order_buy.status == OrderStatus.PARTSUCC_CANCEL
    assert "000001" not in broker._active_orders

    # Cleanup tasks
    try:
        await task_buy
    except:
        pass
    try:
        await task_sell
    except:
        pass

@pytest.mark.asyncio
async def test_sell_timeout(broker):
    """Test sell timeout returns partial trades."""
    # Need position to sell
    broker._positions["000003"] = Position(
        portfolio_id="test_sim", dt=datetime.date.today(), asset="000003",
        shares=1000, price=10.0, avail=1000, mv=10000, profit=0
    )

    async def push_quotes():
        await asyncio.sleep(0.1)
        # 1st push: 200 shares
        broker._on_quote_update({
            "000003": {
                "lastPrice": 10.0,
                "volume": 2,
                "upLimit": 11.0,
                "downLimit": 9.0
            }
        })
        # No 2nd push

    asyncio.create_task(push_quotes())

    res = await broker.sell("000003", 1000, price=10.0, timeout=0.5)

    assert len(res.trades) == 1
    assert res.trades[0].shares == 200

    order = db.get_order(res.qt_oid)
    assert order.status.name == "PART_SUCC"

@pytest.mark.asyncio
async def test_sell_more_than_holdings(broker, mock_live_quote):
    """测试卖出数量超过持仓量"""
    # 1. 初始持仓 100 股
    p = Position(
        portfolio_id="test_sim", dt=datetime.date.today(), asset="000001",
        shares=100, price=10.0, avail=100, mv=1000, profit=0
    )
    broker._positions["000001"] = p

    # 2. 尝试卖出 200 股
    mock_live_quote.get_quote.return_value = {"lastPrice": 11.0, "volume": 1000}

    # 预期应该抛出 InsufficientPosition 异常
    with pytest.raises(InsufficientPosition):
        await broker.sell("000001", 200, 11.0)

@pytest.mark.asyncio
async def test_sell_unavailable_shares(broker, mock_live_quote):
    """测试卖出未解冻持仓（T+1限制）"""
    # 1. 初始持仓 100 股，可用 0
    p = Position(
        portfolio_id="test_sim", dt=datetime.date.today(), asset="000001",
        shares=100, price=10.0, avail=0, mv=1000, profit=0
    )
    broker._positions["000001"] = p

    # 2. 尝试卖出 100 股
    mock_live_quote.get_quote.return_value = {"lastPrice": 11.0}

    with pytest.raises(InsufficientPosition):
        await broker.sell("000001", 100, 11.0)

@pytest.mark.asyncio
async def test_sell_timeout_no_match(broker, mock_live_quote):
    """测试卖出超时"""
    # 1. 直接构造持仓，确保有货可卖
    p = Position(
        portfolio_id="test_sim", dt=datetime.date.today(), asset="000001",
        shares=100, price=10.0, avail=100, mv=1000, profit=0
    )
    broker._positions["000001"] = p

    # 2. 卖出超时
    # 模拟长时间不返回 quote 或者不成交
    # 这里通过不设置 quote 来模拟不成交，从而触发 timeout
    mock_live_quote.get_quote.return_value = {} # No quote

    res = await broker.sell("000001", 100, 11.0, timeout=0.1)

    # 3. 验证
    assert res.qt_oid != ""
    assert len(res.trades) == 0

    # 订单应该保持未报状态（因为只是等待超时，并未撤单）
    orders_df = db.get_orders(portfolio_id=broker.portfolio_id)
    assert len(orders_df) > 0
    latest_status = orders_df["status"][-1]
    assert latest_status == OrderStatus.UNREPORTED.value

@pytest.mark.asyncio
async def test_sell_odd_lot_with_frozen_shares(broker, mock_live_quote):
    """测试存在冻结持仓时，卖出零股（应该失败，因为不能算清仓）"""
    # 1. 构造场景：持仓 150，可用 50（意味着 100 冻结）
    p = Position(
        portfolio_id="test_sim", dt=datetime.date.today(), asset="000001",
        shares=150, price=10.0, avail=50, mv=1500, profit=0
    )
    broker._positions["000001"] = p

    # 2. 尝试卖出 50 股（可用部分的全额，但是是零股）
    mock_live_quote.get_quote.return_value = {"lastPrice": 11.0, "volume": 1000}

    # 3. 预期：因为有冻结持仓，不能算作清仓，所以必须遵守整手规则
    # 50 股不足一手，应该抛出 NonMultipleOfLotSize
    with pytest.raises(NonMultipleOfLotSize):
        await broker.sell("000001", 50, 11.0)

@pytest.mark.asyncio
async def test_limit_price_matching_rules(broker, mock_live_quote):
    """测试涨跌停时的撮合规则（涨停不买，跌停不卖）"""
    # 1. 涨停不买
    # 挂买单
    task_buy = asyncio.create_task(broker.buy("000001", 100, 11.0))
    await asyncio.sleep(0.01)

    # 推送涨停价行情 (lastPrice == upLimit)
    mock_live_quote.get_quote.return_value = {
        "lastPrice": 11.0,
        "upLimit": 11.0,
        "downLimit": 9.0,
        "volume": 1000
    }
    broker._on_quote_update({"000001": mock_live_quote.get_quote.return_value})

    # 预期：不成交
    res_buy = await task_buy
    assert len(res_buy.trades) == 0
    order_buy = db.get_order(res_buy.qt_oid)
    assert order_buy.filled == 0

    # 2. 跌停不卖
    # 构造持仓
    p = Position(
        portfolio_id="test_sim", dt=datetime.date.today(), asset="000001",
        shares=100, price=10.0, avail=100, mv=1000, profit=0
    )
    broker._positions["000001"] = p

    # 挂卖单
    task_sell = asyncio.create_task(broker.sell("000001", 100, 9.0))
    await asyncio.sleep(0.01)

    # 推送跌停价行情 (lastPrice == downLimit)
    mock_live_quote.get_quote.return_value = {
        "lastPrice": 9.0,
        "upLimit": 11.0,
        "downLimit": 9.0,
        "volume": 1000
    }
    broker._on_quote_update({"000001": mock_live_quote.get_quote.return_value})

    # 预期：不成交
    res_sell = await task_sell
    assert len(res_sell.trades) == 0
    order_sell = db.get_order(res_sell.qt_oid)
    assert order_sell.filled == 0


@pytest.mark.asyncio
async def test_buy_percent_small_amount(broker, mock_live_quote):
    """测试按比例买入时，计算数量不足一手的情况"""
    # 现金很少，只够买 50 股
    broker._cash = 500
    mock_live_quote.get_quote.return_value = {
        "lastPrice": 10.0,
        "upLimit": 11.0,
        "downLimit": 9.0,
        "volume": 1000
    }

    # 全仓买入 -> 500元 / 10元 = 50股 -> 取整后 0 股
    res = await broker.buy_percent("000001", 1.0)

    # 预期：不生成订单
    assert res.qt_oid == ""
    assert len(res.trades) == 0

@pytest.mark.asyncio
async def test_buy_amount_small_amount(broker, mock_live_quote):
    """测试按金额买入时，计算数量不足一手的情况"""
    mock_live_quote.get_quote.return_value = {
        "lastPrice": 10.0,
        "upLimit": 11.0,
        "downLimit": 9.0,
        "volume": 1000
    }

    # 买入 500 元 -> 50 股 -> 取整后 0 股
    res = await broker.buy_amount("000001", 500)

    # 预期：不生成订单
    assert res.qt_oid == ""
    assert len(res.trades) == 0

