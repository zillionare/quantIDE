import datetime

import pytest

from quantide.core.init_wizard_steps import WIZARD_TOTAL_STEPS
from quantide.data.models.app_state import AppState
from quantide.web.pages import init_wizard as init_wizard_page


class FakeRequest:
    def __init__(self, form_data):
        self._form_data = form_data

    async def form(self):
        return self._form_data


class FakeInitWizard:
    def __init__(self):
        self.state = AppState(
            app_home="/existing/home",
            app_host="0.0.0.0",
            app_port=8130,
            app_prefix="/",
        )
        self.runtime_calls: list[dict[str, object]] = []
        self.gateway_calls: list[dict[str, object]] = []
        self.gateway_test_calls: list[dict[str, object]] = []
        self.data_calls: list[dict[str, object]] = []
        self.gateway_test_result = (True, "ok")
        self.updated_step: int | None = None

    def get_state(self, force_refresh: bool = False):
        return self.state

    def save_runtime_config(self, home: str, host: str, port: int, prefix: str):
        self.runtime_calls.append(
            {
                "home": home,
                "host": host,
                "port": port,
                "prefix": prefix,
            }
        )
        self.state.app_home = home
        self.state.app_host = host
        self.state.app_port = port
        self.state.app_prefix = prefix

    def test_gateway_connection(self, server: str, port: int, prefix: str):
        self.gateway_test_calls.append(
            {
                "server": server,
                "port": port,
                "prefix": prefix,
            }
        )
        return self.gateway_test_result

    def save_gateway_config(
        self,
        enabled: bool,
        server: str,
        port: int,
        prefix: str,
        api_key: str,
    ):
        self.gateway_calls.append(
            {
                "enabled": enabled,
                "server": server,
                "port": port,
                "prefix": prefix,
                "api_key": api_key,
            }
        )
        self.state.gateway_enabled = enabled
        self.state.gateway_server = server
        self.state.gateway_port = port
        self.state.gateway_base_url = prefix
        self.state.gateway_api_key = api_key

    def save_data_init_config(
        self,
        epoch: datetime.date,
        tushare_token: str,
        history_years: int,
    ):
        self.data_calls.append(
            {
                "epoch": epoch,
                "tushare_token": tushare_token,
                "history_years": history_years,
            }
        )
        self.state.epoch = epoch
        self.state.tushare_token = tushare_token
        self.state.history_years = history_years

    def update_step(self, step: int):
        self.updated_step = step
        self.state.init_step = step


def test_runtime_form_state_uses_canonical_defaults():
    values = init_wizard_page._runtime_form_state({})

    assert values[init_wizard_page.RUNTIME_FORM_FIELDS["home"]] == "~/.quantide"
    assert values[init_wizard_page.RUNTIME_FORM_FIELDS["port"]] == 8130
    assert values[init_wizard_page.RUNTIME_FORM_FIELDS["prefix"]] == "/quantide"
    assert values[init_wizard_page.RUNTIME_FORM_FIELDS["localhost_only"]] is True


def test_gateway_form_state_reads_persisted_prefix_alias():
    values = init_wizard_page._gateway_form_state({"gateway_base_url": "/gateway"})

    assert values[init_wizard_page.GATEWAY_FORM_FIELDS["prefix"]] == "/gateway"


def test_wizard_buttons_uses_shared_total_steps():
    assert init_wizard_page.WizardButtons.__defaults__ == (WIZARD_TOTAL_STEPS,)


@pytest.mark.asyncio
async def test_handle_step_keeps_progress_indicator_in_main_container_swap(monkeypatch):
    fake_wizard = FakeInitWizard()
    monkeypatch.setattr(init_wizard_page, "init_wizard", fake_wizard)

    response = await init_wizard_page.handle_step(
        FakeRequest(
            {
                "_current_step": "1",
                "nav": "next",
            }
        ),
        2,
    )

    html = str(response)

    assert 'id="step-indicator-wrapper"' in html
    assert 'hx-swap-oob' not in html
    assert 'id="wizard-main-container"' not in html


@pytest.mark.asyncio
async def test_handle_step_runtime_config_uses_app_field_names(monkeypatch):
    fake_wizard = FakeInitWizard()
    monkeypatch.setattr(init_wizard_page, "init_wizard", fake_wizard)

    await init_wizard_page.handle_step(
        FakeRequest(
            {
                "_current_step": "2",
                "nav": "next",
                "app_home": "/tmp/market-data",
                "app_port": "9100",
                "app_prefix": "/quantide",
                "localhost_only": "true",
            }
        ),
        3,
    )

    assert fake_wizard.runtime_calls == [
        {
            "home": "/tmp/market-data",
            "host": "127.0.0.1",
            "port": 9100,
            "prefix": "/quantide",
        }
    ]
    assert fake_wizard.updated_step == 3


@pytest.mark.asyncio
async def test_handle_step_runtime_config_accepts_legacy_field_names(monkeypatch):
    fake_wizard = FakeInitWizard()
    monkeypatch.setattr(init_wizard_page, "init_wizard", fake_wizard)

    await init_wizard_page.handle_step(
        FakeRequest(
            {
                "_current_step": "2",
                "nav": "next",
                "home": "/tmp/legacy-home",
                "port": "9200",
                "prefix": "/legacy",
            }
        ),
        3,
    )

    assert fake_wizard.runtime_calls == [
        {
            "home": "/tmp/legacy-home",
            "host": "0.0.0.0",
            "port": 9200,
            "prefix": "/legacy",
        }
    ]
    assert fake_wizard.updated_step == 3


@pytest.mark.asyncio
async def test_handle_step_gateway_config_uses_canonical_field_names(monkeypatch):
    fake_wizard = FakeInitWizard()
    monkeypatch.setattr(init_wizard_page, "init_wizard", fake_wizard)

    await init_wizard_page.handle_step(
        FakeRequest(
            {
                "_current_step": "4",
                "nav": "next",
                "gateway_enabled": "true",
                "gateway_server": "127.0.0.1",
                "gateway_port": "8001",
                "gateway_api_key": "gateway-key",
                "gateway_prefix": "/gateway",
            }
        ),
        5,
    )

    assert fake_wizard.gateway_test_calls == [
        {
            "server": "127.0.0.1",
            "port": 8001,
            "prefix": "/gateway",
        }
    ]
    assert fake_wizard.gateway_calls == [
        {
            "enabled": True,
            "server": "127.0.0.1",
            "port": 8001,
            "prefix": "/gateway",
            "api_key": "gateway-key",
        }
    ]
    assert fake_wizard.updated_step == 5


@pytest.mark.asyncio
async def test_handle_step_gateway_config_uses_persisted_prefix_alias_when_omitted(monkeypatch):
    fake_wizard = FakeInitWizard()
    fake_wizard.state.gateway_server = "persisted-host"
    fake_wizard.state.gateway_port = 8002
    fake_wizard.state.gateway_base_url = "/persisted"
    fake_wizard.state.gateway_api_key = "persisted-key"
    monkeypatch.setattr(init_wizard_page, "init_wizard", fake_wizard)

    await init_wizard_page.handle_step(
        FakeRequest(
            {
                "_current_step": "4",
                "nav": "next",
            }
        ),
        5,
    )

    assert fake_wizard.gateway_test_calls == []
    assert fake_wizard.gateway_calls == [
        {
            "enabled": False,
            "server": "persisted-host",
            "port": 8002,
            "prefix": "/persisted",
            "api_key": "persisted-key",
        }
    ]
    assert fake_wizard.updated_step == 5


@pytest.mark.asyncio
async def test_handle_step_data_setup_uses_canonical_field_names(monkeypatch):
    fake_wizard = FakeInitWizard()
    monkeypatch.setattr(init_wizard_page, "init_wizard", fake_wizard)

    await init_wizard_page.handle_step(
        FakeRequest(
            {
                "_current_step": "5",
                "nav": "next",
                "epoch": "2024-01-01",
                "tushare_token": "ts-token",
                "history_years": "3",
            }
        ),
        6,
    )

    assert fake_wizard.data_calls == [
        {
            "epoch": datetime.date(2024, 1, 1),
            "tushare_token": "ts-token",
            "history_years": 3,
        }
    ]
    assert fake_wizard.updated_step == 6