import datetime
from types import SimpleNamespace

import pytest

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
        home="~/pyqmt-home",
        server=SimpleNamespace(host="127.0.0.1", port=9100, prefix="/pyqmt"),
        gateway=SimpleNamespace(base_url="http://127.0.0.1:8000"),
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
        epoch=datetime.date(2015, 1, 1),
    )
    monkeypatch.setattr("pyqmt.service.init_wizard.cfg", fake_cfg)

    service = InitWizardService()
    state = service.get_state(force_refresh=True)

    assert state.app_home == "~/pyqmt-home"
    assert state.app_host == "127.0.0.1"
    assert state.app_port == 9100
    assert state.app_prefix == "/pyqmt"
    assert state.gateway_base_url == "/"
    assert state.gateway_enabled is True
    assert state.gateway_api_key == "demo-key"
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
    assert service.get_completion_redirect() == "/trade"
