import datetime
from pathlib import Path
from types import SimpleNamespace

from pyqmt.config.runtime import (
    get_runtime_config,
    get_runtime_dingtalk_access_token,
    get_runtime_dingtalk_secret,
    get_runtime_epoch,
    get_runtime_home,
    get_runtime_mail_receivers,
    get_runtime_mail_sender,
    get_runtime_mail_server,
    get_runtime_tushare_token,
    get_runtime_timezone,
)
from pyqmt.data.models.app_state import AppState


def test_runtime_config_falls_back_to_cfg(monkeypatch):
    fake_cfg = SimpleNamespace(
        TIMEZONE=datetime.timezone.utc,
        home="~/pyqmt-home",
        server=SimpleNamespace(host="127.0.0.1", port=9100, prefix="/pyqmt"),
        gateway=SimpleNamespace(
            base_url="https://gateway.example.com/base",
            username="admin",
            password="secret",
            timeout=15,
        ),
        livequote=SimpleNamespace(mode="gateway"),
        runtime=SimpleNamespace(mode="live", market_adapter="", broker_adapter=""),
        apikeys=SimpleNamespace(clients=[{"key": "demo-key"}]),
        epoch=datetime.date(2015, 1, 1),
    )
    monkeypatch.setattr("cfg4py.get_instance", lambda: fake_cfg)

    runtime = get_runtime_config()
    assert runtime.app_home == "~/pyqmt-home"
    assert runtime.app_prefix == "/pyqmt"
    assert runtime.gateway_base_url == "https://gateway.example.com/base"
    assert runtime.gateway_username == "admin"
    assert runtime.livequote_mode == "gateway"
    assert runtime.runtime_mode == "live"
    assert runtime.epoch == datetime.date(2015, 1, 1)


def test_runtime_config_prefers_app_state(db, monkeypatch, tmp_path: Path):
    fake_cfg = SimpleNamespace(
        TIMEZONE=datetime.timezone.utc,
        home="~/pyqmt-home",
        server=SimpleNamespace(host="127.0.0.1", port=9100, prefix="/pyqmt"),
        gateway=SimpleNamespace(
            base_url="http://127.0.0.1:8000",
            username="admin",
            password="secret",
            timeout=15,
        ),
        livequote=SimpleNamespace(mode="none"),
        runtime=SimpleNamespace(mode="backtest", market_adapter="", broker_adapter=""),
        apikeys=SimpleNamespace(clients=[{"key": "demo-key"}]),
        epoch=datetime.date(2015, 1, 1),
    )
    monkeypatch.setattr("cfg4py.get_instance", lambda: fake_cfg)

    db["app_state"].upsert(
        AppState(
            app_home=str(tmp_path),
            app_prefix="/db-prefix",
            gateway_enabled=True,
            gateway_scheme="https",
            gateway_server="gateway.internal",
            gateway_port=8443,
            gateway_base_url="/qmt",
            gateway_username="db-admin",
            gateway_password="db-secret",
            gateway_timeout=30,
            livequote_mode="gateway",
            runtime_mode="live",
        ).to_dict(),
        pk="id",
    )

    runtime = get_runtime_config()
    assert runtime.app_home == str(tmp_path)
    assert runtime.app_prefix == "/db-prefix"
    assert runtime.gateway_base_url == "https://gateway.internal:8443/qmt"
    assert runtime.gateway_username == "db-admin"
    assert runtime.gateway_password == "db-secret"
    assert runtime.gateway_timeout == 30
    assert runtime.livequote_mode == "gateway"
    assert runtime.runtime_mode == "live"


def test_runtime_config_allows_db_to_disable_gateway(db, monkeypatch):
    fake_cfg = SimpleNamespace(
        TIMEZONE=datetime.timezone.utc,
        home="~/pyqmt-home",
        server=SimpleNamespace(host="127.0.0.1", port=9100, prefix="/pyqmt"),
        gateway=SimpleNamespace(
            base_url="http://127.0.0.1:8000",
            username="admin",
            password="secret",
            timeout=15,
        ),
        livequote=SimpleNamespace(mode="gateway"),
        runtime=SimpleNamespace(mode="live", market_adapter="", broker_adapter=""),
        apikeys=SimpleNamespace(clients=[{"key": "demo-key"}]),
        epoch=datetime.date(2015, 1, 1),
    )
    monkeypatch.setattr("cfg4py.get_instance", lambda: fake_cfg)

    db["app_state"].upsert(
        AppState(
            gateway_enabled=False,
            gateway_server="gateway.internal",
            gateway_port=8443,
            gateway_base_url="/qmt",
            livequote_mode="none",
            runtime_mode="backtest",
        ).to_dict(),
        pk="id",
    )

    runtime = get_runtime_config()
    assert runtime.gateway_enabled is False
    assert runtime.livequote_mode == "none"
    assert runtime.runtime_mode == "backtest"


def test_runtime_helpers_follow_db_state(db, monkeypatch, tmp_path: Path):
    fake_cfg = SimpleNamespace(
        TIMEZONE=datetime.timezone.utc,
        home="~/pyqmt-home",
        server=SimpleNamespace(host="127.0.0.1", port=9100, prefix="/pyqmt"),
        gateway=SimpleNamespace(base_url="http://127.0.0.1:8000"),
        livequote=SimpleNamespace(mode="gateway"),
        runtime=SimpleNamespace(mode="live", market_adapter="", broker_adapter=""),
        apikeys=SimpleNamespace(clients=[]),
        epoch=datetime.date(2015, 1, 1),
    )
    monkeypatch.setattr("cfg4py.get_instance", lambda: fake_cfg)

    db["app_state"].upsert(
        AppState(
            app_home=str(tmp_path),
            epoch=datetime.date(2020, 1, 2),
        ).to_dict(),
        pk="id",
    )

    assert get_runtime_home() == str(tmp_path)
    assert get_runtime_epoch() == datetime.date(2020, 1, 2)
    assert get_runtime_timezone() == datetime.timezone.utc


def test_runtime_notify_and_token_helpers_follow_db_state(db, monkeypatch):
    fake_cfg = SimpleNamespace(
        TIMEZONE=datetime.timezone.utc,
        home="~/pyqmt-home",
        server=SimpleNamespace(host="127.0.0.1", port=9100, prefix="/pyqmt"),
        gateway=SimpleNamespace(base_url="http://127.0.0.1:8000"),
        livequote=SimpleNamespace(mode="gateway"),
        runtime=SimpleNamespace(mode="live", market_adapter="", broker_adapter=""),
        apikeys=SimpleNamespace(clients=[]),
        notify=SimpleNamespace(
            dingtalk=SimpleNamespace(access_token="cfg-token", secret="cfg-secret", keyword="cfg-keyword"),
            mail=SimpleNamespace(mail_to=["cfg@example.com"], mail_from="cfg-from@example.com", mail_server="smtp.cfg.example.com"),
        ),
        tushare_token="cfg-ts-token",
        epoch=datetime.date(2015, 1, 1),
    )
    monkeypatch.setattr("cfg4py.get_instance", lambda: fake_cfg)

    db["app_state"].upsert(
        AppState(
            notify_dingtalk_access_token="db-token",
            notify_dingtalk_secret="db-secret",
            notify_mail_to="a@example.com,b@example.com",
            notify_mail_from="db-from@example.com",
            notify_mail_server="smtp.db.example.com",
            tushare_token="db-ts-token",
        ).to_dict(),
        pk="id",
    )

    assert get_runtime_dingtalk_access_token() == "db-token"
    assert get_runtime_dingtalk_secret() == "db-secret"
    assert get_runtime_mail_receivers() == ["a@example.com", "b@example.com"]
    assert get_runtime_mail_sender() == "db-from@example.com"
    assert get_runtime_mail_server() == "smtp.db.example.com"
    assert get_runtime_tushare_token() == "db-ts-token"