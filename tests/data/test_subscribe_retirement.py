from pathlib import Path

import pytest

from pyqmt.core.legacy_qmt import LEGACY_LOCAL_QMT_ENV
from pyqmt.subscribe import _run_qmt_loop, subscribe_live, sync_1m_bars


def test_subscribe_live_requires_explicit_legacy_opt_in(monkeypatch):
    monkeypatch.delenv(LEGACY_LOCAL_QMT_ENV, raising=False)

    with pytest.raises(RuntimeError, match="xtdata 订阅"):
        subscribe_live()


def test_sync_1m_bars_requires_explicit_legacy_opt_in(monkeypatch):
    monkeypatch.delenv(LEGACY_LOCAL_QMT_ENV, raising=False)

    with pytest.raises(RuntimeError, match="xtdata 订阅"):
        sync_1m_bars(["000001.SZ"])


def test_run_qmt_loop_requires_explicit_legacy_opt_in(monkeypatch):
    monkeypatch.delenv(LEGACY_LOCAL_QMT_ENV, raising=False)

    with pytest.raises(RuntimeError, match="xtdata 订阅"):
        _run_qmt_loop()