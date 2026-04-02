import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import quantide.service.init_wizard as init_wizard_module

from quantide.core.init_wizard_steps import WIZARD_FINAL_STEP, WIZARD_TOTAL_STEPS
from quantide.config.paths import DEFAULT_DATA_HOME
from quantide.data.models.app_state import AppState
from quantide.service.init_wizard import InitWizardService


@pytest.fixture(autouse=True)
def reset_service_state(db):
    db["app_state"].delete_where("1=1")
    service = InitWizardService()
    service._state = None
    yield
    db["app_state"].delete_where("1=1")
    service._state = None


def test_get_state_loads_defaults_from_config(db, monkeypatch):
    fake_settings = SimpleNamespace(
        app_home=str(Path("~/quantide-home").expanduser()),
        app_host="127.0.0.1",
        app_port=9100,
        app_prefix="/quantide",
        gateway_enabled=True,
        gateway_base_url="http://127.0.0.1:8000",
        gateway_api_key="demo-key",
        gateway_username="gateway-user",
        gateway_password="gateway-pass",
        gateway_timeout=15,
        gateway_scheme="http",
        gateway_server="127.0.0.1",
        gateway_port=8000,
        livequote_mode="gateway",
        runtime_mode="live",
        runtime_market_adapter="demo-market",
        runtime_broker_adapter="demo-broker",
        epoch=datetime.date(2015, 1, 1),
    )
    monkeypatch.setattr(init_wizard_module, "get_settings", lambda: fake_settings)
    monkeypatch.setattr(init_wizard_module, "get_dingtalk_access_token", lambda: "dt-token")
    monkeypatch.setattr(init_wizard_module, "get_dingtalk_secret", lambda: "dt-secret")
    monkeypatch.setattr(init_wizard_module, "get_dingtalk_keyword", lambda: "dt-keyword")
    monkeypatch.setattr(init_wizard_module, "get_mail_receivers", lambda: ["to@example.com"])
    monkeypatch.setattr(init_wizard_module, "get_mail_sender", lambda: "from@example.com")
    monkeypatch.setattr(init_wizard_module, "get_mail_server", lambda: "smtp.example.com")
    monkeypatch.setattr(init_wizard_module, "get_tushare_token", lambda: "ts-cfg-token")

    service = InitWizardService()
    state = service.get_state(force_refresh=True)

    assert state.app_home == str(Path("~/quantide-home").expanduser())
    assert state.app_host == "127.0.0.1"
    assert state.app_port == 9100
    assert state.app_prefix == "/quantide"
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
        init_step=WIZARD_FINAL_STEP,
        app_home="/tmp/market-home",
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
            init_step=WIZARD_FINAL_STEP,
            app_home="/tmp/market-home",
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
        "数据源设置及下载",
        "完成",
    ]
    assert progress["total_steps"] == WIZARD_TOTAL_STEPS


def test_complete_initialization_uses_shared_final_step(db):
    service = InitWizardService()
    service.save_state(AppState(app_home="/tmp/market-home", init_step=5))

    service.complete_initialization()

    state = service.get_state(force_refresh=True)
    assert state.init_completed is True
    assert state.init_step == WIZARD_FINAL_STEP


def test_save_runtime_config_keeps_auth_on_fixed_config_db(db, monkeypatch, tmp_path):
    rebound: dict[str, str] = {}
    fake_auth = SimpleNamespace(
        auth_db=SimpleNamespace(db_path="/tmp/old.db"),
        rebind_database=lambda path: rebound.setdefault("db_path", path),
    )

    monkeypatch.setattr(
        init_wizard_module.AuthManager,
        "get_instance",
        staticmethod(lambda: fake_auth),
    )
    monkeypatch.setattr(
        init_wizard_module,
        "get_app_db_path",
        lambda: tmp_path / "config" / "quantide.db",
    )

    service = InitWizardService()
    service.save_runtime_config(
        home=str(tmp_path / "market-home"),
        host="127.0.0.1",
        port=8130,
        prefix="/",
    )

    assert rebound["db_path"] == str((tmp_path / "config" / "quantide.db").resolve())


def test_save_runtime_config_defaults_blank_home_and_expands_user(db):
    service = InitWizardService()

    service.save_runtime_config(
        home="   ",
        host="127.0.0.1",
        port=8130,
        prefix="/",
    )

    state = service.get_state(force_refresh=True)
    assert state.app_home == str(Path(DEFAULT_DATA_HOME).expanduser())
