import pytest

from quantide.subscribe import _run_qmt_loop, subscribe_live, sync_1m_bars


def test_subscribe_live_is_removed():
    with pytest.raises(RuntimeError, match="行情订阅功能已从 quantide 主体移除"):
        subscribe_live()


def test_sync_1m_bars_is_removed():
    with pytest.raises(RuntimeError, match="行情订阅功能已从 quantide 主体移除"):
        sync_1m_bars(["000001.SZ"])


def test_run_qmt_loop_is_removed():
    with pytest.raises(RuntimeError, match="行情订阅功能已从 quantide 主体移除"):
        _run_qmt_loop()