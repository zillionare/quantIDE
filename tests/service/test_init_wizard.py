import datetime
from types import SimpleNamespace

import pytest
import pyqmt.service.init_wizard as init_wizard_module

from pyqmt.data.models.app_state import AppState
from pyqmt.service.init_wizard import InitWizardService


@pytest.fixture(autouse=True)
def reset_service_state(db):
    db["app_state"].delete_where("1=1")
    service = InitWizardService()
    service._state = None
    yield
    db["app_state"].delete_where("1=1")
    service._state = None


def test_get_state_loads_defaults_from_config(db, monkeypatch):
    fake_cfg = SimpleNamespace(
        TIMEZONE=datetime.timezone.utc,
        home="~/pyqmt-home",
        server=SimpleNamespace(host="127.0.0.1", port=9100, prefix="/pyqmt"),
        gateway=SimpleNamespace(
            base_url="http://127.0.0.1:8000",
            username="gateway-user",
            password="gateway-pass",
            timeout=15,
        ),
        livequote=SimpleNamespace(mode="gateway"),
        runtime=SimpleNamespace(
            mode="live",
            market_adapter="demo-market",
            broker_adapter="demo-broker",
        ),
        apikeys=SimpleNamespace(clients=[{"key": "demo-key"}]),
        notify=SimpleNamespace(
            dingtalk=SimpleNamespace(
                access_token="dt-token",
                secret="dt-secret",
                keyword="dt-keyword",
            ),
            mail=SimpleNamespace(
                mail_to="to@example.com",
                mail_from="from@example.com",
                mail_server="smtp.example.com",
            ),
        ),
        tushare_token="ts-cfg-token",
        epoch=datetime.date(2015, 1, 1),
    )
    monkeypatch.setattr("cfg4py.get_instance", lambda: fake_cfg)

    service = InitWizardService()
    state = service.get_state(force_refresh=True)

    assert state.app_home == "~/pyqmt-home"
    assert state.app_host == "127.0.0.1"
    assert state.app_port == 9100
    assert state.app_prefix == "/pyqmt"
    assert state.gateway_base_url == "/"
    assert state.gateway_enabled is True
    assert state.gateway_api_key == "demo-key"
    assert state.gateway_username == "gateway-user"
    assert state.gateway_password == "gateway-pass"
    assert state.gateway_timeout == 15
    assert state.livequote_mode == "gateway"
    assert state.runtime_mode == "live"
    assert state.runtime_market_adapter == "demo-market"
    assert state.runtime_broker_adapter == "demo-broker"
    assert state.notify_dingtalk_access_token == "dt-token"
    assert state.notify_dingtalk_secret == "dt-secret"
    assert state.notify_dingtalk_keyword == "dt-keyword"
    assert state.notify_mail_to == "to@example.com"
    assert state.notify_mail_from == "from@example.com"
    assert state.notify_mail_server == "smtp.example.com"
    assert state.tushare_token == "ts-cfg-token"
    assert state.epoch == datetime.date(2015, 1, 1)
    assert state.history_start_date >= datetime.date(2015, 1, 1)


def test_feature_status_and_redirect_follow_gateway_state(db):
    service = InitWizardService()

    state = AppState(
        init_completed=True,
        init_step=7,
        tushare_token="ts-token",
        gateway_enabled=False,
        gateway_base_url="",
    )
    service.save_state(state)

    status = service.get_feature_status()
    assert status["backtest"] is True
    assert status["simulation"] is False
    assert status["live_trading"] is False
    assert service.get_completion_redirect() == "/strategy"

    service.save_gateway_config(
        enabled=True,
        server="127.0.0.1",
        port=8000,
        prefix="/",
        api_key="k1",
    )
    service.complete_initialization()

    status = service.get_feature_status()
    assert status["simulation"] is True
    assert status["live_trading"] is True
    assert service.get_completion_redirect() == "/trade"


def test_gateway_connection_handles_invalid_url_input(db):
    service = InitWizardService()

    ok, msg = service.test_gateway_connection(server="", port=8000, prefix="/")
    assert ok is False
    assert "server" in msg

    assert service._normalize_gateway_url("127.0.0.1:8000") == "http://127.0.0.1:8000"


def test_save_gateway_config_uses_slash_default_and_redirect_by_server(db):
    service = InitWizardService()
    service.save_state(
        AppState(
            init_completed=True,
            init_step=7,
            gateway_enabled=False,
            gateway_base_url="/",
        )
    )

    service.save_gateway_config(
        enabled=True,
        server="127.0.0.1",
        port=8000,
        prefix="/",
        api_key="k1",
    )
    service.complete_initialization()

    state = service.get_state(force_refresh=True)
    assert state.gateway_base_url == "/"
    assert service.get_completion_redirect() == "/auth/login"


def test_save_admin_password_updates_existing_admin(db, monkeypatch):
    class FakeRepo:
        def __init__(self):
            self.updated = None

        def get_by_username(self, username):
            assert username == "admin"
            return SimpleNamespace(id=7, username="admin")

        def update(self, user_id, **kwargs):
            self.updated = (user_id, kwargs)
            return True

    fake_auth = SimpleNamespace(user_repo=FakeRepo())
    monkeypatch.setattr(init_wizard_module.AuthManager, "get_instance", staticmethod(lambda: fake_auth))

    service = InitWizardService()
    service.save_admin_password("new-secret")

    assert fake_auth.user_repo.updated == (7, {"password": "new-secret"})


def test_get_progress_uses_new_step_labels(db):
    service = InitWizardService()
    service.save_state(AppState(init_step=3))

    progress = service.get_progress()
    names = [step["name"] for step in progress["steps"]]

    assert names == [
        "欢迎",
        "运行环境",
        "管理员密码",
        "行情与交易网关",
        "通知告警",
        "数据初始化与下载",
        "完成",
    ]
