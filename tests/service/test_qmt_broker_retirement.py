import pytest

from pyqmt.service.qmt_broker import LEGACY_LOCAL_QMT_ENV, QMTBroker


def test_qmt_broker_requires_explicit_legacy_opt_in(monkeypatch):
    monkeypatch.delenv(LEGACY_LOCAL_QMT_ENV, raising=False)

    with pytest.raises(RuntimeError, match="QMTBroker 已退役为兼容路径"):
        QMTBroker("demo-account")