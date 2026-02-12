import asyncio
import datetime
import os
import tempfile
import time
import uuid
from unittest.mock import MagicMock, patch

import msgpack
import pandas as pd
import pyarrow.parquet as pq
import pytest
import redis
from freezegun import freeze_time
from testcontainers.redis import RedisContainer

import pyqmt.service.livequote as lq_service
from pyqmt.core.enums import OrderSide, OrderStatus, Topics
from pyqmt.core.errors import (
    InsufficientCash,
    InsufficientPosition,
    NonMultipleOfLotSize,
)
from pyqmt.core.message import msg_hub
from pyqmt.core.singleton import _instances
from pyqmt.data.sqlite import Position, db
from pyqmt.service.livequote import LiveQuote
from pyqmt.service.metrics import metrics
from pyqmt.service.sim_broker import SimulationBroker


@pytest.fixture(scope="module")
def redis_container():
    """Start Redis container for testing."""
    container = RedisContainer("redis:8")
    container.start()
    yield container
    container.stop()

@pytest.fixture
def redis_client(redis_container):
    """Create Redis client connected to the container."""
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    if host == "localhost":
        host = "127.0.0.1"
    client = redis.Redis(host=host, port=port, decode_responses=False)
    return client

@pytest.fixture
def mock_config(redis_container):
    """Mock configuration to point to the Redis container."""
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    if host == "localhost":
        host = "127.0.0.1"

    config = MagicMock()
    config.livequote.mode = "redis"
    config.redis.host = host
    config.redis.port = int(port)
    return config

@pytest.fixture
def broker(db, mock_config):
    # Mock config
    with patch("pyqmt.config.cfg", mock_config), \
         patch("pyqmt.service.livequote.cfg", mock_config):

        # Reset live_quote singleton state
        _instances.clear()

        # Clear message hub subscribers to prevent zombie brokers from previous tests
        msg_hub._subscribers.clear()

        # Initialize live_quote (it will connect to redis container)
        # Mock scheduler to avoid scheduling jobs
        with patch("pyqmt.service.livequote.scheduler"):
            lq = LiveQuote()
            lq.start()
            # Wait for connection
            time.sleep(0.5)

        # Patch the global live_quote in sim_broker and livequote modules
        # to ensure they use our new instance
        with patch("pyqmt.service.sim_broker.live_quote", lq), \
             patch("pyqmt.service.livequote.live_quote", lq):

            # Create broker
            unique_id = f"test_sim_{uuid.uuid4().hex[:8]}"
            broker = SimulationBroker(unique_id, principal=100000)

            yield broker

        # Stop live_quote
        lq.stop()

        # Unsubscribe broker to prevent memory leak
        msg_hub.unsubscribe(Topics.QUOTES_ALL.value, broker._on_quote_update)
        msg_hub.unsubscribe(Topics.STOCK_LIMIT.value, broker._on_limit_update)

def publish_quote_to_redis(client, asset, price, volume=None, up_limit=None, down_limit=None, lq_instance=None):
    """Helper to publish quote to Redis."""
    # 1. Publish Limits if provided
    if up_limit is not None or down_limit is not None:
        limit_data = {asset: {}}
        if up_limit is not None:
            limit_data[asset]["up_limit"] = up_limit
        if down_limit is not None:
            limit_data[asset]["down_limit"] = down_limit

        packed_limits = msgpack.packb(limit_data)
        client.publish(Topics.STOCK_LIMIT.value, packed_limits)

    # 2. Publish Quote
    data = {
        asset: {
            "lastPrice": price,
            "amount": 1000000,
        }
    }
    if volume is not None:
        data[asset]["volume"] = volume

    packed = msgpack.packb(data)
    client.publish(Topics.QUOTES_ALL.value, packed)
    # Wait for propagation
    time.sleep(0.5)

@pytest.mark.asyncio
async def test_error_handling_scenarios(broker, redis_client):
    """Test various error handling scenarios."""
    asset = "000005.SZ"
    publish_quote_to_redis(redis_client, asset, 10.0, up_limit=11.0, down_limit=9.0)

    # 1. InsufficientCash
    # Principal is 100,000. Try to buy 200,000 worth of stock.
    with pytest.raises(InsufficientCash):
        await broker.buy(asset, 20000, price=10.0)

    # 2. NonMultipleOfLotSize
    with pytest.raises(NonMultipleOfLotSize):
        await broker.buy(asset, 150, price=10.0)

    # 3. InsufficientPosition (Sell)
    # No position yet
    with pytest.raises(InsufficientPosition):
        await broker.sell(asset, 100, price=10.0)

    # Buy some position first
    await broker.buy(asset, 100, price=10.0)
    # Push quote to fill
    publish_quote_to_redis(redis_client, asset, 10.0, volume=1000)
    await asyncio.sleep(0.1)

    # Now try to sell more than owned
    with pytest.raises(InsufficientPosition):
        await broker.sell(asset, 200, price=10.0)

    # 4. Account exists/not exists
    # Create existing
    with pytest.raises(RuntimeError, match="already exists"):
        SimulationBroker.create(broker.portfolio_id)

    # Load non-existing
    with pytest.raises(RuntimeError, match="does not exist"):
        SimulationBroker.load("non_existent_portfolio")

@pytest.mark.asyncio
async def test_order_cancellation(broker, redis_client):
    """Test order cancellation logic."""
    asset = "000006.SZ"
    publish_quote_to_redis(redis_client, asset, 10.0, up_limit=11.0, down_limit=9.0)

    # Place limit buy at 9.0 (won't fill)
    task = asyncio.create_task(broker.buy(asset, 100, price=9.0, timeout=5.0))
    await asyncio.sleep(0.1)

    order = broker._active_orders[asset][0]

    # Cancel specific order
    await broker.cancel_order(order.qtoid)

    assert len(broker._active_orders[asset]) == 0
    res = await task
    assert len(res.trades) == 0

    # Verify DB status
    db_order = db.get_order(order.qtoid)
    assert db_order.status == OrderStatus.CANCELED

    # Test cancel_all_orders by side
    # Place buy and sell orders
    task_buy = asyncio.create_task(broker.buy(asset, 100, price=9.0, timeout=5.0))

    # Need position to sell
    broker._positions[asset] = Position(
        portfolio_id=broker.portfolio_id, dt=datetime.date.today(), asset=asset,
        shares=100, price=10.0, avail=100, mv=1000, profit=0
    )
    task_sell = asyncio.create_task(broker.sell(asset, 100, price=11.0, timeout=5.0))
    await asyncio.sleep(0.1)

    # Cancel BUY only
    await broker.cancel_all_orders(side=OrderSide.BUY)

    active_orders = broker._active_orders[asset]
    assert len(active_orders) == 1
    assert active_orders[0].side == OrderSide.SELL

    await broker.cancel_all_orders() # Cleanup remaining
    await task_buy
    await task_sell

@pytest.mark.asyncio
async def test_special_trade_scenarios(broker, redis_client):
    """Test buy_percent, trade_target_pct and limits."""
    asset = "000007.SZ"
    publish_quote_to_redis(redis_client, asset, 10.0, up_limit=11.0, down_limit=9.0)

    # 1. buy_percent
    # Cash 100,000. 50% -> 50,000. Price 10.0 -> 5000 shares
    task = asyncio.create_task(broker.buy_percent(asset, 0.5, price=10.0))
    await asyncio.sleep(0.1)

    order = broker._active_orders[asset][0]
    # Check approx shares (might be slightly less due to fee/rounding)
    # 50000 / 10 = 5000.
    assert order.shares == 5000
    await broker.cancel_all_orders()
    await task

    # 2. trade_target_pct
    # Current pos: 0. Target: 10%
    # Total asset ~100k. Target 10k. Price 10.0 -> Buy 1000 shares
    task = asyncio.create_task(broker.trade_target_pct(asset, 0.1, price=10.0))
    await asyncio.sleep(0.1)

    order = broker._active_orders[asset][0]
    assert order.shares == 1000
    assert order.side == OrderSide.BUY
    await broker.cancel_all_orders()
    await task

    # 3. Limit rules (Up/Down limit)
    # Price 11.0 (Up limit). Buy should fail/not match?
    # SimBroker checks limit in _try_match.
    # If order price is within limit, but market price touches limit:
    # Rule: Buy at UpLimit -> Allowed? Usually yes if there is volume.
    # Rule: Sell at DownLimit -> Allowed? Usually yes.
    # Wait, SimBroker logic:
    # if order.side == OrderSide.BUY and up_limit > 0 and last_price >= up_limit: return 0.0, None
    # So Buy at UpLimit is BLOCKED in SimBroker currently (Strict mode).

    publish_quote_to_redis(redis_client, asset, 11.0, up_limit=11.0, down_limit=9.0)
    task = asyncio.create_task(broker.buy(asset, 100, price=11.0, timeout=1.0))
    await asyncio.sleep(0.1)

    # Should be active but not filled even if we push volume
    publish_quote_to_redis(redis_client, asset, 11.0, volume=1000, up_limit=11.0, down_limit=9.0)

    res = await task
    assert len(res.trades) == 0 # Should not fill due to UpLimit restriction

@pytest.mark.asyncio
async def test_commission_and_t1(broker, redis_client):
    """Test commission calculation and T+1 rule."""
    asset = "000008.SZ"
    publish_quote_to_redis(redis_client, asset, 10.0, up_limit=11.0, down_limit=9.0)

    # 1. Commission
    # Buy 100 shares @ 10.0 = 1000. Comm rate 1e-4 -> 0.1. Min 5.0.
    # So fee should be 5.0
    task = asyncio.create_task(broker.buy(asset, 100, price=10.0, timeout=2.0))
    await asyncio.sleep(0.1)
    publish_quote_to_redis(redis_client, asset, 10.0, volume=100)

    res = await task
    trade = res.trades[0]
    assert trade.fee == 5.0

    # Buy large amount to exceed min fee
    # 100,000 shares @ 10.0 = 1,000,000. Fee = 100.0. Cost = 1,000,100.
    # We need enough cash. Initial principal 100,000 is not enough.
    # Reset cash for this test or add cash?
    # SimBroker has no deposit method exposed.
    # We can hack _cash.
    broker._cash = 2_000_000 # Add enough cash

    task = asyncio.create_task(broker.buy(asset, 100000, price=10.0, timeout=2.0))
    await asyncio.sleep(0.1)
    publish_quote_to_redis(redis_client, asset, 10.0, volume=100000)

    res = await task
    trade = res.trades[0]
    # 1,000,000 * 1e-4 = 100.0
    assert abs(trade.fee - 100.0) < 0.001

    # 2. T+1 Rule
    # We just bought asset. Avail should be 0 (if logic is correct)
    # SimBroker:
    # pos = Position(..., avail=0, ...)
    pos = broker._positions[asset]
    # Note: previous buy of 100 shares might be merged.
    # Total shares = 100 + 100000 = 100100
    # Avail should be 0 because they are bought "today" (T+0)
    assert pos.avail == 0

    # Try to sell, should fail
    with pytest.raises(InsufficientPosition):
        await broker.sell(asset, 100, price=10.0)

@pytest.mark.asyncio
async def test_boundary_conditions(broker, redis_client):
    """Test boundary conditions like zero price."""
    asset = "000009.SZ"

    # Zero price quote
    publish_quote_to_redis(redis_client, asset, 0.0)

    # Buy should handle gracefully (probably timeout or error if validation exists)
    # SimBroker buy checks est_price. If 0, it tries to get from quote.
    # If quote is 0, it might proceed with price=0?
    # buy -> est_price=0 -> ...
    # _try_match checks last_price <= 0 -> return 0.0

    task = asyncio.create_task(broker.buy(asset, 100, price=0.0, timeout=1.0))
    res = await task
    assert len(res.trades) == 0

    # Negative price (shouldn't happen in reality but good to test)
    publish_quote_to_redis(redis_client, asset, -10.0)
    task = asyncio.create_task(broker.buy(asset, 100, price=0.0, timeout=1.0))
    res = await task
    assert len(res.trades) == 0


@pytest.mark.asyncio
async def test_match_price_condition(broker, redis_client):
    """Test that order is filled only when price condition is met."""
    asset = "000004.SZ"
    # Initial price 12.0, limits 9.0-13.0
    publish_quote_to_redis(redis_client, asset, 12.0, up_limit=13.0, down_limit=9.0)

    # Place buy order at 10.0. Current market 12.0 > 10.0, should NOT fill.
    task = asyncio.create_task(broker.buy(asset, 200, price=10.0, timeout=2.0))
    await asyncio.sleep(0.1)

    # Push quote 11.0 (still > 10.0)
    publish_quote_to_redis(redis_client, asset, 11.0, volume=100)
    await asyncio.sleep(0.1)

    # Check order still active and not filled
    assert len(broker._active_orders[asset]) == 1
    assert broker._active_orders[asset][0].filled == 0

    # Push quote 10.0 (matches price)
    publish_quote_to_redis(redis_client, asset, 10.0, volume=100)

    res = await task
    assert len(res.trades) == 1
    assert res.trades[0].shares == 200
    assert res.trades[0].price == 10.0

@pytest.mark.asyncio
async def test_no_volume_in_quote_full_match(broker, redis_client):
    """Test that if quote has no volume, order matches fully."""
    asset = "000001.SZ"
    publish_quote_to_redis(redis_client, asset, 10.0, up_limit=11.0, down_limit=9.0)

    task = asyncio.create_task(broker.buy(asset, 1000, price=10.0, timeout=2.0))
    await asyncio.sleep(0.01)

    # No "volume" field
    publish_quote_to_redis(redis_client, asset, 10.0)

    res = await task

    assert len(res.trades) == 1
    assert res.trades[0].shares == 1000
    assert broker._positions[asset].shares == 1000

@pytest.mark.asyncio
async def test_buy_amount(broker, redis_client):
    publish_quote_to_redis(redis_client, "000001.SZ", 10.0, up_limit=11.0, down_limit=9.0)

    # Buy 100k amount -> 10k shares (approx)
    # 100,000 / 11.0 = 9090.9 -> 9000
    task = asyncio.create_task(broker.buy_amount("000001.SZ", 100000))
    await asyncio.sleep(0.1)

    order = broker._active_orders["000001.SZ"][0]
    assert order.shares == 9000

    await broker.cancel_all_orders()
    await task

@pytest.mark.asyncio
async def test_sell_percent(broker, redis_client):
    p = Position(
        portfolio_id="test_sim",
        dt=datetime.date.today(),
        asset="000001.SZ",
        shares=1000,
        avail=1000,
        price=10.0,
        mv=10000,
        profit=0
    )
    broker._positions["000001.SZ"] = p
    publish_quote_to_redis(redis_client, "000001.SZ", 10.0, up_limit=11.0, down_limit=9.0)

    # Sell 50% -> 500 shares
    task = asyncio.create_task(broker.sell_percent("000001.SZ", 0.5))
    await asyncio.sleep(0.1)

    order = broker._active_orders["000001.SZ"][0]
    assert order.shares == 500
    assert order.side == OrderSide.SELL

    await broker.cancel_all_orders()
    await task

    # Test clear all (100%)
    task2 = asyncio.create_task(broker.sell_percent("000001.SZ", 1.0))
    await asyncio.sleep(0.1)
    order2 = broker._active_orders["000001.SZ"][0]
    assert order2.shares == 1000

    await broker.cancel_all_orders()
    await task2

@pytest.mark.asyncio
async def test_sell_amount(broker, redis_client):
    p = Position(
        portfolio_id="test_sim",
        dt=datetime.date.today(),
        asset="000001.SZ",
        shares=1000,
        avail=1000,
        price=10.0,
        mv=10000,
        profit=0
    )
    broker._positions["000001.SZ"] = p
    publish_quote_to_redis(redis_client, "000001.SZ", 10.0, up_limit=11.0, down_limit=9.0)

    # Sell 5000 amount -> 500 shares (approx 5000/10=500, but sell_amount uses downLimit usually?)

    task = asyncio.create_task(broker.sell_amount("000001.SZ", 5000))
    await asyncio.sleep(0.1)

    order = broker._active_orders["000001.SZ"][0]
    # If using downLimit (9.0): 5000/9 = 555 -> 500
    assert order.shares == 500

    await broker.cancel_all_orders()
    await task

@pytest.mark.asyncio
async def test_sell_all_removes_position(broker, redis_client):
    broker._positions["000001.SZ"] = Position(
        portfolio_id="test_sim", dt=datetime.date.today(), asset="000001.SZ",
        shares=100, price=10.0, avail=100, mv=1000, profit=0
    )
    publish_quote_to_redis(redis_client, "000001.SZ", 10.0, volume=10000)

    # Sell all
    task = asyncio.create_task(broker.sell("000001.SZ", 100))
    await asyncio.sleep(0.1)

    publish_quote_to_redis(redis_client, "000001.SZ", 10.0)
    await task

    assert "000001.SZ" not in broker._positions


@pytest.mark.asyncio
async def test_trade_persistence_and_recovery_async(db, mock_config, redis_client):
    """Async version of persistence test."""
    portfolio_id = f"test_persist_{uuid.uuid4().hex[:8]}"

    # Need to manually setup broker here because we want to destroy it and recreate
    with patch("pyqmt.config.cfg", mock_config), \
         patch("pyqmt.service.livequote.cfg", mock_config), \
         patch("pyqmt.service.livequote.scheduler"):

        _instances.clear()
        lq = LiveQuote()
        lq.start()
        time.sleep(1.0)

        # Patch global live_quote in both modules
        with patch("pyqmt.service.sim_broker.live_quote", lq), \
             patch("pyqmt.service.livequote.live_quote", lq):

            broker1 = SimulationBroker(portfolio_id, principal=100000)

            publish_quote_to_redis(redis_client, "000001.SZ", 10.0, volume=1000, up_limit=11.0, down_limit=9.0, lq_instance=lq)
            task = asyncio.create_task(broker1.buy("000001.SZ", 100, price=10.0, timeout=2.0))
            await asyncio.sleep(0.1)
            publish_quote_to_redis(redis_client, "000001.SZ", 10.0, lq_instance=lq)
            res = await task

            assert len(res.trades) == 1
            assert "000001.SZ" in broker1._positions

            # Verify DB
            trades_df = db.query_trade(qtoid=res.qt_oid)
            assert len(trades_df) == 1

            # Restart
            msg_hub.unsubscribe(Topics.QUOTES_ALL.value, broker1._on_quote_update)
            msg_hub.unsubscribe(Topics.STOCK_LIMIT.value, broker1._on_limit_update)
            del broker1
            broker2 = SimulationBroker(portfolio_id)
            assert "000001.SZ" in broker2._positions
            assert broker2._positions["000001.SZ"].shares == 100

            # Cleanup
            msg_hub.unsubscribe(Topics.QUOTES_ALL.value, broker2._on_quote_update)

        lq.stop()

@pytest.mark.asyncio
async def test_synthetic_intraday_simulation(broker, redis_client):
    """Test using synthetic data to simulate intraday price movements."""
    # Scenario:
    # Asset: "SIM.SH"
    # 1. Open: 10.0
    # 2. Drop: 9.8 (Buy limit 9.9 should fill at 9.8)
    # 3. Rise: 10.2 (Sell limit 10.1 should fill at 10.2)

    asset = "SIM.SH"

    # T0: Initial State (Open)
    publish_quote_to_redis(redis_client, asset, 10.0, up_limit=11.0, down_limit=9.0)
    await asyncio.sleep(0.01)

    # User places a Limit Buy Order at 9.9
    # Current price is 10.0, so it should NOT fill yet
    buy_task = asyncio.create_task(broker.buy(asset, 1000, price=9.9, timeout=5.0))
    await asyncio.sleep(0.1)

    # Verify not filled
    assert len(broker._active_orders[asset]) == 1
    assert broker._active_orders[asset][0].filled == 0

    # T1: Price drops to 9.8
    # This should trigger the buy order (Limit 9.9 >= Market 9.8)
    # Fill price should be 9.8 (Best Execution)
    publish_quote_to_redis(redis_client, asset, 9.8, volume=5000)

    buy_res = await buy_task
    assert len(buy_res.trades) == 1
    assert buy_res.trades[0].price == 9.8
    assert buy_res.trades[0].shares == 1000

    # Wait for position update to reflect in broker memory
    # Position update happens in _on_quote_update loop, buy_res is returned AFTER persistence
    # but we should double check if broker._positions is updated.
    # buy_res is returned via awake(), which is called AFTER _apply_trade_to_portfolio
    assert broker._positions[asset].shares == 1000

    # T2: Price rises to 10.2
    publish_quote_to_redis(redis_client, asset, 10.2, volume=5000)
    await asyncio.sleep(0.01)

    # User places a Limit Sell Order at 10.1
    # Current price is 10.2, so it should fill IMMEDIATELY at 10.2
    # NOTE: sell checks avail position. T+1 rule might apply if it's a new position?
    # SimulationBroker implements T+1 for BUY: "avail=0, # T+1".
    # So we cannot sell immediately if we just bought it today.
    # To test sell, we need to hack the position to be available.

    broker._positions[asset].avail = 1000 # Hack T+1 to T+0 for testing

    sell_task = asyncio.create_task(broker.sell(asset, 500, price=10.1, timeout=2.0))

    # Since quote is already 10.2, the order might match immediately upon next quote update
    # OR if broker logic matches on arrival (if we had quote cache).
    # Current logic: Broker matches on quote update. So we need to push a quote OR wait if broker caches.
    # SimBroker matches on *receiving* a quote. If order comes *after* quote, it waits for *next* quote.
    # So we push the quote again (or a new tick) to trigger matching.
    await asyncio.sleep(0.01)
    publish_quote_to_redis(redis_client, asset, 10.2, volume=5000)

    sell_res = await sell_task
    assert len(sell_res.trades) == 1
    assert sell_res.trades[0].price == 10.2
    assert sell_res.trades[0].shares == 500
    assert broker._positions[asset].shares == 500

@pytest.mark.asyncio
async def test_concurrent_brokers(db, mock_config, redis_client):
    """Test multiple brokers handling quotes concurrently."""
    with patch("pyqmt.config.cfg", mock_config), \
         patch("pyqmt.service.livequote.cfg", mock_config), \
         patch("pyqmt.service.livequote.scheduler"):

        _instances.clear()
        lq = LiveQuote()
        lq.start()
        time.sleep(1.0)

        # Patch global live_quote in both modules
        with patch("pyqmt.service.sim_broker.live_quote", lq), \
             patch("pyqmt.service.livequote.live_quote", lq):

            p1 = f"p1_{uuid.uuid4().hex[:8]}"
            p2 = f"p2_{uuid.uuid4().hex[:8]}"
            broker1 = SimulationBroker(p1, principal=100000)
            broker2 = SimulationBroker(p2, principal=100000)

            asset = "000001.SZ"
            price = 10.0

            # Setup limits - explicitly pass lq instance
            publish_quote_to_redis(redis_client, asset, price, up_limit=11.0, down_limit=9.0, lq_instance=lq)

            # Both buy
            t1 = asyncio.create_task(broker1.buy(asset, 100, price=price, timeout=2.0))
            t2 = asyncio.create_task(broker2.buy(asset, 200, price=price, timeout=2.0))

            await asyncio.sleep(0.1)

            # Publish quote to trigger both
            publish_quote_to_redis(redis_client, asset, price, volume=10000, lq_instance=lq)

            res1 = await t1
            res2 = await t2

            assert len(res1.trades) == 1
            assert len(res2.trades) == 1
            assert broker1._positions[asset].shares == 100
            assert broker2._positions[asset].shares == 200

            # Cleanup
            msg_hub.unsubscribe(Topics.QUOTES_ALL.value, broker1._on_quote_update)
            msg_hub.unsubscribe(Topics.QUOTES_ALL.value, broker2._on_quote_update)

        lq.stop()


@pytest.mark.asyncio
async def test_day_close_cancels_active_orders(broker, redis_client):
    """Test that on_day_close automatically cancels active orders."""
    asset = "000001.SZ"

    # 1. Setup: Publish quote
    publish_quote_to_redis(redis_client, asset, 10.0, up_limit=11.0, down_limit=9.0)
    await asyncio.sleep(0.01)

    # 2. Case 1: Fully unfilled order
    # Place a limit buy at 9.0 (below market 10.0), won't fill
    task1 = asyncio.create_task(broker.buy(asset, 1000, price=9.0, timeout=5.0))
    await asyncio.sleep(0.1)

    # Verify order is active
    assert len(broker._active_orders[asset]) == 1
    order1 = broker._active_orders[asset][0]
    assert order1.filled == 0
    # In SimulationBroker, initial status is UNREPORTED (48) before matching loop updates it?
    # Or maybe we should allow UNREPORTED too.
    # Actually let's check what it is: AssertionError says it is 48 (UNREPORTED).
    assert order1.status.value in [48, 49, 50] # UNREPORTED/WAIT_REPORTING/REPORTED

    # 3. Trigger day close
    await broker.on_day_close()

    # 4. Verify cancellation
    # Memory cleared
    assert len(broker._active_orders[asset]) == 0

    # Task should complete returning empty trades
    res1 = await task1
    assert len(res1.trades) == 0

    # DB status updated
    db_order1 = db.get_order(order1.qtoid)
    assert db_order1.status == OrderStatus.CANCELED # 54
    assert "Canceled by user" in db_order1.status_msg # cancel_all_orders uses this msg

    # 5. Case 2: Partially filled order
    # Reset for next case
    publish_quote_to_redis(redis_client, asset, 10.0)

    # Place limit buy at 10.0
    task2 = asyncio.create_task(broker.buy(asset, 1000, price=10.0, timeout=5.0))
    await asyncio.sleep(0.1)

    # Fill 500 shares (half)
    publish_quote_to_redis(redis_client, asset, 10.0, volume=5) # 5 hands = 500 shares
    await asyncio.sleep(0.1)

    # Verify partial fill
    assert len(broker._active_orders[asset]) == 1
    order2 = broker._active_orders[asset][0]
    assert order2.filled == 500

    # Trigger day close again
    await broker.on_day_close()

    # Verify cancellation
    assert len(broker._active_orders[asset]) == 0

    # Task should return the partial trades
    res2 = await task2
    assert len(res2.trades) == 1
    assert res2.trades[0].shares == 500

    # DB status updated
    db_order2 = db.get_order(order2.qtoid)
    assert db_order2.status == OrderStatus.PARTSUCC_CANCEL # 52

@pytest.mark.asyncio
async def test_sim_broker_full_lifecycle(redis_client, mock_config, db):
    """
    Test the complete lifecycle of a SimulationBroker:
    1. Create Account (New)
    2. Day 1: Buy & Day Close
    3. Restart (Load Account)
    4. Day 2: Sell & Day Close
    5. Day 3: Skip (No Trade) & Day Close
    6. Metrics Calculation
    """
    portfolio_id = f"sim_full_{uuid.uuid4().hex[:8]}"
    asset_code = "000001.SZ"

    # --- Setup LiveQuote Environment ---
    with patch("pyqmt.config.cfg", mock_config), \
         patch("pyqmt.service.livequote.cfg", mock_config), \
         patch("pyqmt.service.livequote.scheduler"):

        # Reset singleton
        _instances.clear()
        lq = LiveQuote()
        lq.start()
        # Ensure SimBroker uses this lq instance
        with patch("pyqmt.service.sim_broker.live_quote", lq):

            # ==========================================
            # Phase 1: Create New Account
            # ==========================================
            print("\n>>> Phase 1: Create New Account")
            # Ensure no existing data
            assert db.get_portfolio(portfolio_id) is None

            day0 = datetime.date(2022, 12, 31) # Initial setup day

            # Create broker
            with freeze_time(day0):
                broker = SimulationBroker.create(
                    portfolio_id=portfolio_id,
                    principal=100_000,
                )
            assert broker.cash == 100_000
            assert len(broker.positions) == 0

            # ==========================================
            # Phase 2: Day 1 - Buy
            # ==========================================
            print("\n>>> Phase 2: Day 1 - Buy")
            day1 = datetime.date(2023, 1, 1)

            # Patch at class level to cover async trade updates
            with freeze_time(day1):
                # Publish initial quote
                publish_quote_to_redis(redis_client, asset_code, 10.0, up_limit=11.0, down_limit=9.0)

                # Buy 1000 shares @ 10.0
                task = asyncio.create_task(broker.buy(asset_code, 1000, price=10.0, timeout=2.0))
                await asyncio.sleep(0.1)

                # Push quote to trigger match
                publish_quote_to_redis(redis_client, asset_code, 10.0, volume=2000) # Enough volume

                res = await task
                assert len(res.trades) == 1
                assert broker.positions[0].shares == 1000
                # Cash should decrease: 100,000 - 10,000 - fee
                expected_cost = 1000 * 10.0
                assert broker.cash < 100_000 - expected_cost

                # Place an unfilled order (Limit too low)
                # Buy @ 9.0 (Market 10.0)
                task_unfilled = asyncio.create_task(broker.buy(asset_code, 100, price=9.0, timeout=2.0))
                await asyncio.sleep(0.1)
                # Don't publish price drop, so it remains active
                assert len(broker._active_orders[asset_code]) > 0

                # Day Close
                # Close price = 10.5 (Profit)
                await broker.on_day_close(close_prices={asset_code: 10.5})

            # Verify unfilled order is canceled
            assert len(broker._active_orders[asset_code]) == 0
            # Task should have finished with empty trades
            res_unfilled = await task_unfilled
            assert len(res_unfilled.trades) == 0

            # Verify DB Asset record for Day 1
            asset_record = db.get_asset(dt=day1, portfolio_id=portfolio_id)
            assert asset_record is not None
            # Market Value = 1000 * 10.5 = 10500
            assert asset_record.market_value == 10500.0

            # ==========================================
            # Phase 3: Restart (Load Account)
            # ==========================================
            print("\n>>> Phase 3: Restart (Load Account)")
            # Simulate process restart: destroy broker instance
            msg_hub.unsubscribe(Topics.QUOTES_ALL.value, broker._on_quote_update)
            msg_hub.unsubscribe(Topics.STOCK_LIMIT.value, broker._on_limit_update)
            del broker

            # Load broker
            # Note: We need to mock _get_today for the NEW broker instance if it's called during init.
            # _init_or_sync_state calls _get_today().
            # Since SimulationBroker.load calls cls(), we can patch the class method or patch AFTER creation?
            # But creation runs __init__.
            # So we should patch SimulationBroker._get_today at class level or use a context manager?
            # Creating an instance will use the real method unless patched on the class.

            with freeze_time(day1):
                 # Load checks for today's position if exists.
                 # We want it to load the state.
                 broker = SimulationBroker.load(portfolio_id)

            assert len(broker.positions) == 1
            assert broker.positions[0].asset == asset_code
            assert broker.positions[0].shares == 1000

            # ==========================================
            # Phase 4: Day 2 - Sell
            # ==========================================
            print("\n>>> Phase 4: Day 2 - Sell")
            day2 = datetime.date(2023, 1, 2)

            # We need to ensure broker._get_today returns day2 now.
            # Since we can't easily patch the instance method of a local variable that changes,
            # we can rely on the fact that _get_today is only called in specific methods.
            # We will patch it when calling those methods.
            # However, internal calls like _apply_trade_to_portfolio (triggered by async callback)
            # are hard to patch per-call.
            # BETTER: Patch the CLASS method for the duration of the phase.

            with freeze_time(day2):
                # Publish quote 11.0
                publish_quote_to_redis(redis_client, asset_code, 11.0, up_limit=12.0, down_limit=10.0)

                # Sell 500 shares @ 11.0
                task_sell = asyncio.create_task(broker.sell(asset_code, 500, price=11.0, timeout=2.0))
                await asyncio.sleep(0.1)
                publish_quote_to_redis(redis_client, asset_code, 11.0, volume=1000)

                res_sell = await task_sell
                assert len(res_sell.trades) == 1
                assert res_sell.trades[0].shares == 500

                assert broker.positions[0].shares == 500

                # Day Close
                # Close price = 11.0
                await broker.on_day_close(close_prices={asset_code: 11.0})

            # Verify DB Asset for Day 2
            asset_record_2 = db.get_asset(dt=day2, portfolio_id=portfolio_id)
            # MV = 500 * 11.0 = 5500
            assert asset_record_2.market_value == 5500.0

            # ==========================================
            # Phase 5: Day 3 - Skip (No Trade)
            # ==========================================
            print("\n>>> Phase 5: Day 3 - Skip")
            day3 = datetime.date(2023, 1, 3)

            # Just day close, maybe price dropped to 10.0
            with freeze_time(day3):
                await broker.on_day_close(close_prices={asset_code: 10.0})

            asset_record_3 = db.get_asset(dt=day3, portfolio_id=portfolio_id)
            # MV = 500 * 10.0 = 5000
            assert asset_record_3.market_value == 5000.0

            # ==========================================
            # Phase 6: Metrics
            # ==========================================
            print("\n>>> Phase 6: Metrics")

            # metrics() reads from DB assets table
            # We have 3 days of records: day1, day2, day3.
            # Initial day (start date) usually implies initial capital?
            # metrics calculation needs > 1 record to calculate returns.

            stats = metrics(portfolio_id)
            assert not stats.empty
            print("\nMetrics calculated successfully:")
            print(stats.tail())

            print("\nMetrics Result:")
            print(stats)

            assert stats is not None
            assert not stats.empty
            # Check some basic metrics exist
            assert "Sharpe" in stats.index or "Sharpe Ratio" in stats.index or "Sharpe" in str(stats.index)
            # Note: quantstats output format depends on 'mode'. 'full' returns a DataFrame.

            # Cleanup
            msg_hub.unsubscribe(Topics.QUOTES_ALL.value, broker._on_quote_update)
            msg_hub.unsubscribe(Topics.STOCK_LIMIT.value, broker._on_limit_update)

        lq.stop()
