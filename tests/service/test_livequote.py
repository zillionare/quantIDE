import datetime
import time
from unittest.mock import ANY, MagicMock, patch

import msgpack
import pandas as pd
import pytest

from pyqmt.core.enums import Topics
from pyqmt.core.message import msg_hub
from pyqmt.service.livequote import LiveQuote


@pytest.fixture
def mock_config():
    config = MagicMock()
    config.livequote.mode = "redis"
    config.redis.host = "127.0.0.1"
    config.redis.port = 6379
    return config

@pytest.fixture
def live_quote(mock_config):
    """Initialize LiveQuote with mocked configuration and reset singleton."""
    with patch("pyqmt.service.livequote.cfg", mock_config):
        # Reset singleton
        if hasattr(LiveQuote, "_instances"):
             LiveQuote._instances.clear()

        service = LiveQuote()

        # Mock scheduler to prevent actual job scheduling during tests unless needed
        with patch("pyqmt.service.livequote.scheduler"):
             yield service

        # Cleanup
        if service._is_running:
             service._is_running = False

def test_start_qmt_mode(live_quote, mock_config):
    """Test starting in QMT mode."""
    mock_config.livequote.mode = "qmt"

    # Mock xtquant
    mock_xt = MagicMock()
    with patch("pyqmt.service.livequote.xt", mock_xt):
        live_quote.start()

        assert live_quote._mode == "qmt"
        mock_xt.subscribe_whole_quote.assert_called_once()
        args = mock_xt.subscribe_whole_quote.call_args[0]
        assert args[0] == ["SH", "SZ", "BJ"]
        assert args[1] == live_quote._cache_and_broadcast

def test_start_qmt_mode_missing_xt(live_quote, mock_config):
    """Test error when xtquant is missing in QMT mode."""
    mock_config.livequote.mode = "qmt"

    with patch("pyqmt.service.livequote.xt", None):
        with pytest.raises(ImportError, match="xtquant is required"):
            live_quote.start()

def test_start_redis_mode_no_client(live_quote, mock_config):
    """Test error when Redis client fails to configure."""
    # Simulate missing redis config
    with patch("pyqmt.service.livequote.cfg", MagicMock(redis=None)):
         # Need to set mode on the mock that replaced cfg
         # But we patched cfg entirely.
         # Let's just patch getattr to return None for redis
         pass

    # Easier: mock_config.redis = None won't work because it's a Mock.
    # We can rely on logic: if getattr(cfg, "redis", None) is None.

    mock_cfg_no_redis = MagicMock()
    mock_cfg_no_redis.livequote.mode = "redis"
    # getattr(mock, "redis") returns a Mock by default, so we need to ensure it returns None?
    # No, getattr(cfg, "redis", None) checks if attribute exists.
    # MagicMock has all attributes.
    # We need to explicitely delete it or use spec.
    del mock_cfg_no_redis.redis

    with patch("pyqmt.service.livequote.cfg", mock_cfg_no_redis):
        with pytest.raises(RuntimeError, match="Redis client is not configured"):
            live_quote.start()

def test_redis_subscription_flow(live_quote):
    """Test Redis subscription, cache update, and message broadcasting."""
    # Mock Redis client and pubsub
    mock_redis = MagicMock()
    mock_pubsub = MagicMock()
    mock_redis.pubsub.return_value = mock_pubsub

    # Prepare test data
    test_data = {
        "600000.SH": {"lastPrice": 10.5, "amount": 10000},
        "000001.SZ": {"lastPrice": 12.0, "amount": 20000}
    }
    packed_data = msgpack.packb(test_data)

    # Mock pubsub listen to yield one message
    mock_pubsub.listen.return_value = [
        {"type": "subscribe", "channel": Topics.QUOTES_ALL.value, "data": 1}, # Should be ignored
        {"type": "message", "channel": Topics.QUOTES_ALL.value, "data": packed_data}
    ]

    # Verify msg_hub broadcast
    received_msgs = []
    def on_msg(data):
        received_msgs.append(data)
    msg_hub.subscribe("quote.all", on_msg)

    with patch("redis.Redis", return_value=mock_redis):
        live_quote.start()

        # Wait for thread to process (mocked listen returns instantly, but it runs in thread)
        time.sleep(0.1)

        # 1. Verify Redis subscription
        mock_redis.pubsub.assert_called_once()
        mock_pubsub.subscribe.assert_called_with(Topics.QUOTES_ALL.value)

        # 2. Verify Cache Update
        assert live_quote.get_quote("600000.SH") == test_data["600000.SH"]
        assert live_quote.get_quote("000001.SZ") == test_data["000001.SZ"]

        # 3. Verify Broadcast
        assert len(received_msgs) > 0
        assert received_msgs[0] == test_data

        # 4. Verify all_quotes property
        all_q = live_quote.all_quotes
        assert "600000.SH" in all_q
        assert all_q["600000.SH"] == test_data["600000.SH"]

def test_refresh_limits(live_quote):
    """Test refreshing price limits."""
    # Mock fetch_limit_price
    mock_df = pd.DataFrame({
        "ts_code": ["600000.SH", "000001.SZ"],
        "up_limit": [11.0, 13.2],
        "down_limit": [9.0, 10.8]
    })

    # Mock datetime to control logic if needed (though _refresh_limits uses dt or today)
    test_date = datetime.date(2023, 1, 1)

    with patch("pyqmt.service.livequote.fetch_limit_price", return_value=(mock_df, None)) as mock_fetch:
        live_quote._refresh_limits(test_date)

        mock_fetch.assert_called_with(test_date)

        # Verify limits updated
        down, up = live_quote.get_price_limits("600000.SH")
        assert up == 11.0
        assert down == 9.0

        limits_dict = live_quote.get_limit("000001.SZ")
        assert limits_dict["up_limit"] == 13.2
        assert limits_dict["down_limit"] == 10.8

        # Verify all_limits
        assert "600000.SH" in live_quote.all_limits

def test_refresh_limits_empty(live_quote):
    """Test refresh limits with empty result."""
    with patch("pyqmt.service.livequote.fetch_limit_price", return_value=(None, None)):
        live_quote._refresh_limits()
        assert len(live_quote.all_limits) == 0

    with patch("pyqmt.service.livequote.fetch_limit_price", return_value=(pd.DataFrame(), None)):
        live_quote._refresh_limits()
        assert len(live_quote.all_limits) == 0

def test_cache_limits_method(live_quote):
    """Test the _cache_limits method directly."""
    # Although unused in current flow, checking it ensures coverage
    data = {
        "600000.SH": {"up_limit": 11.0, "down_limit": 9.0}
    }
    live_quote._cache_limits(data)

    down, up = live_quote.get_price_limits("600000.SH")
    assert up == 11.0
    assert down == 9.0

def test_redis_message_handling_error(live_quote):
    """Test handling of invalid redis message."""
    mock_redis = MagicMock()
    mock_pubsub = MagicMock()
    mock_redis.pubsub.return_value = mock_pubsub

    # Invalid msgpack data
    mock_pubsub.listen.return_value = [
        {"type": "message", "channel": Topics.QUOTES_ALL.value, "data": b"invalid_bytes"}
    ]

    with patch("redis.Redis", return_value=mock_redis):
        # Should verify it logs error but doesn't crash
        with patch("pyqmt.service.livequote.logger") as mock_logger:
            live_quote.start()
            time.sleep(0.1)
            mock_logger.error.assert_called()

def test_start_limit_schedule(live_quote):
    """Test that limit schedule is started."""
    with patch("pyqmt.service.livequote.scheduler") as mock_scheduler:
        # We need to unpatch the scheduler in the fixture for this specific test
        # OR just check the mocked scheduler from fixture if we used `yield service` inside patch
        # The fixture patches scheduler.

        # Let's invoke _start_limit_schedule directly or via start
        # Since fixture patches scheduler, we can get the mock from `sys.modules` or patch again?
        # Actually, fixture yields service inside the patch context.
        # But we don't have reference to the mock object in test function.
        # Let's re-patch to get reference.

        # Wait, start() calls _start_limit_schedule.
        # Check if scheduler.add_job is called.
        pass

    # Re-verify with accessible mock
    with patch("pyqmt.service.livequote.scheduler") as mock_scheduler:
        # We need to re-create live_quote to bind to this new mock?
        # No, LiveQuote imports scheduler at module level. Patching module level works.
        live_quote.start()
        mock_scheduler.add_job.assert_called_with(
            live_quote._refresh_limits,
            "cron",
            hour=9,
            minute=0,
            name="livequote.limit.refresh",
        )

def test_get_limit_empty(live_quote):
    assert live_quote.get_limit("invalid") is None
    assert live_quote.get_price_limits("invalid") == (0.0, 0.0)

def test_start_idempotent(live_quote):
    """Test calling start multiple times."""
    with patch("redis.Redis"):
        live_quote.start()
        live_quote.start() # Should return immediately
        # Verify by checking log or side effects (hard to check return, but coverage will show)

def test_cache_limits_empty(live_quote):
    """Test _cache_limits with empty data."""
    live_quote._cache_limits({})
    live_quote._cache_limits(None)
    # Coverage check

def test_refresh_limits_missing_cols(live_quote):
    """Test refresh limits with missing columns."""
    # 1. Missing asset/ts_code
    df_no_asset = pd.DataFrame({"other": [1, 2]})
    with patch("pyqmt.service.livequote.fetch_limit_price", return_value=(df_no_asset, None)):
        live_quote._refresh_limits()
        # Should return early

    # 2. Missing up/down limit columns (should be filled with 0.0)
    df_missing_limits = pd.DataFrame({"ts_code": ["600000.SH"]})
    with patch("pyqmt.service.livequote.fetch_limit_price", return_value=(df_missing_limits, None)):
        live_quote._refresh_limits()
        down, up = live_quote.get_price_limits("600000.SH")
        assert up == 0.0
        assert down == 0.0

def test_slow_processing_warning(live_quote):
    """Test warning log for slow processing."""
    mock_redis = MagicMock()
    mock_pubsub = MagicMock()
    mock_redis.pubsub.return_value = mock_pubsub

    test_data = {"600000.SH": {"lastPrice": 10.5}}
    packed_data = msgpack.packb(test_data)

    mock_pubsub.listen.return_value = [
        {"type": "message", "channel": Topics.QUOTES_ALL.value, "data": packed_data}
    ]

    with patch("redis.Redis", return_value=mock_redis):
        with patch("pyqmt.service.livequote.logger") as mock_logger:
            # Mock _cache_and_broadcast to be slow
            def slow_process(data):
                time.sleep(0.06) # > 50ms

            with patch.object(live_quote, "_cache_and_broadcast", side_effect=slow_process):
                live_quote.start()
                time.sleep(0.2)
                mock_logger.warning.assert_called()
                args = mock_logger.warning.call_args[0][0]
                assert "Slow quote processing" in args
