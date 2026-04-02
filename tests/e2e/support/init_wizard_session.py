"""Helpers for init-wizard end-to-end sessions without pytest fixtures."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

from starlette.testclient import TestClient

from quantide.app_factory import create_app
from quantide.config.paths import clear_app_config_dir_override
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


def _reset_init_wizard_page_state() -> None:
    init_wizard_page._sync_status = dict(_SYNC_STATUS_TEMPLATE)
    init_wizard_page._set_download_error(None)
    init_wizard_page._set_reconfigure_mode(False)


def _reset_global_state() -> None:
    init_wizard._state = None
    AuthManager._instance = None
    _reset_init_wizard_page_state()


@dataclass
class InitWizardE2ESession:
    """Runtime objects used by an init-wizard e2e test."""

    client: TestClient
    market_home: Path
    app_config_dir: Path


@contextmanager
def init_wizard_e2e_session():
    """Create a fully isolated init-wizard e2e session."""

    with TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        app_config_dir = tmp_path / "config-home"
        market_home = tmp_path / "market-data"

        _reset_global_state()

        try:
            app = create_app(
                app_config_dir=app_config_dir,
                enforce_single_instance=False,
            )
            with TestClient(app) as client:
                yield InitWizardE2ESession(
                    client=client,
                    market_home=market_home,
                    app_config_dir=app_config_dir,
                )
        finally:
            _reset_global_state()
            clear_app_config_dir_override()
