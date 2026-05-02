"""Helpers for authenticated system-settings end-to-end sessions."""

from __future__ import annotations

import datetime
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from starlette.testclient import TestClient

from quantide.app_factory import create_app
from quantide.config.paths import clear_app_config_dir_override
from quantide.core.init_wizard_steps import WIZARD_FINAL_STEP
from quantide.data.models.app_state import AppState
from quantide.service.init_wizard import init_wizard
from quantide.web.auth.manager import AuthManager
from quantide.web.pages import init_wizard as init_wizard_page


_SYNC_STATUS_TEMPLATE = {
    "is_running": False,
    "current_task": "",
    "progress": 0,
    "stage": "",
    "message": "",
    "completed": False,
    "error": None,
}


def _reset_global_state() -> None:
    init_wizard._state = None
    AuthManager._instance = None
    init_wizard_page._sync_status = dict(_SYNC_STATUS_TEMPLATE)
    init_wizard_page._set_download_error(None)
    init_wizard_page._set_reconfigure_mode(False)


@dataclass
class SystemSettingsE2ESession:
    """Runtime objects used by system-settings e2e tests."""

    client: TestClient
    market_home: Path
    app_config_dir: Path


def _login_as_admin(client: TestClient) -> None:
    response = client.post(
        "/auth/login",
        data={
            "username": "admin",
            "password": "admin123",
            "redirect_to": "/system/gateway/",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303


@contextmanager
def open_system_settings_client(app_config_dir: Path, market_home: Path | None = None):
    """Open an authenticated client against an existing app config dir."""

    app = create_app(
        app_config_dir=app_config_dir,
        enforce_single_instance=False,
    )
    if market_home is not None:
        _seed_initialized_state(market_home)
    with TestClient(app) as client:
        _login_as_admin(client)
        yield client


def _seed_initialized_state(market_home: Path) -> None:
    state = AppState(
        app_home=str(market_home),
        init_completed=True,
        init_step=WIZARD_FINAL_STEP,
        init_started_at=datetime.datetime.now(),
        init_completed_at=datetime.datetime.now(),
        data_source="tushare",
        tushare_token="seed-token",
        epoch=datetime.date(2024, 1, 1),
        history_years=2,
    )
    init_wizard.save_state(state)


@contextmanager
def system_settings_e2e_session():
    """Create an initialized and authenticated session for system pages."""

    with TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        app_config_dir = tmp_path / "config-home"
        market_home = tmp_path / "market-data"

        _reset_global_state()

        try:
            with open_system_settings_client(app_config_dir, market_home) as client:
                yield SystemSettingsE2ESession(
                    client=client,
                    market_home=market_home,
                    app_config_dir=app_config_dir,
                )
        finally:
            _reset_global_state()
            clear_app_config_dir_override()