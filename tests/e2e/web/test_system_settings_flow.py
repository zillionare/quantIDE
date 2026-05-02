"""End-to-end tests for system settings flows."""

from __future__ import annotations

import datetime

import pytest

from quantide.service.init_wizard import init_wizard
from quantide.web.pages.system import jobs as jobs_page
from tests.e2e.support.gateway_stub import running_gateway_stub
from tests.e2e.support.system_settings_session import (
    open_system_settings_client,
    system_settings_e2e_session,
)


@pytest.mark.e2e
def test_gateway_settings_can_test_and_persist_configuration():
    with system_settings_e2e_session() as session, running_gateway_stub(prefix="/qmt") as gateway_stub:
        page = session.client.get("/system/gateway/", follow_redirects=False)
        assert page.status_code == 200
        assert "保存配置" in page.text

        test_response = session.client.post(
            "/system/gateway/test",
            data={
                "gateway_enabled": "on",
                "gateway_server": gateway_stub.host,
                "gateway_port": str(gateway_stub.port),
                "gateway_prefix": gateway_stub.prefix,
                "gateway_api_key": "gateway-key",
                "gateway_timeout": "5",
            },
        )
        assert test_response.status_code == 200
        assert "连通性测试通过" in test_response.text
        assert gateway_stub.prefix in test_response.text

        save_response = session.client.post(
            "/system/gateway/save",
            data={
                "gateway_enabled": "on",
                "gateway_server": gateway_stub.host,
                "gateway_port": str(gateway_stub.port),
                "gateway_prefix": gateway_stub.prefix,
                "gateway_api_key": "gateway-key",
                "gateway_timeout": "5",
            },
        )
        assert save_response.status_code == 200
        assert "网关配置已保存" in save_response.text

        state = init_wizard.get_state(force_refresh=True)
        assert state.gateway_enabled is True
        assert state.gateway_server == gateway_stub.host
        assert state.gateway_port == gateway_stub.port
        assert state.gateway_base_url == gateway_stub.prefix
        assert state.gateway_api_key == "gateway-key"
        assert state.gateway_timeout == 5

        persisted_page = session.client.get("/system/gateway/", follow_redirects=False)
        assert persisted_page.status_code == 200
        assert gateway_stub.host in persisted_page.text
        assert str(gateway_stub.port) in persisted_page.text


@pytest.mark.e2e
def test_datasource_settings_can_persist_token_and_epoch():
    with system_settings_e2e_session() as session:
        page = session.client.get("/system/datasource/", follow_redirects=False)
        assert page.status_code == 200
        assert "保存配置" in page.text

        response = session.client.post(
            "/system/datasource/save",
            data={
                "data_source": "tushare",
                "tushare_token": "updated-token",
                "epoch": "2023-06-01",
                "history_years": "2",
            },
        )
        assert response.status_code == 200
        assert "数据源配置已保存" in response.text

        state = init_wizard.get_state(force_refresh=True)
        assert state.data_source == "tushare"
        assert state.tushare_token == "updated-token"
        assert state.epoch == datetime.date(2023, 6, 1)
        assert state.history_years == 2

        persisted_page = session.client.get("/system/datasource/", follow_redirects=False)
        assert persisted_page.status_code == 200
        assert "2023-06-01" in persisted_page.text


@pytest.mark.e2e
def test_jobs_toggle_persists_after_reopening_app():
    with system_settings_e2e_session() as session:
        page = session.client.get("/system/jobs/", follow_redirects=False)
        assert page.status_code == 200
        assert "运行中" in page.text

        toggle_response = session.client.get(
            "/system/jobs/toggle/daily_bars_sync",
            follow_redirects=False,
        )
        assert toggle_response.status_code == 303

        toggled_page = session.client.get("/system/jobs/", follow_redirects=False)
        assert toggled_page.status_code == 200
        assert "已停止" in toggled_page.text

        jobs_page._job_enabled_state.clear()

        with open_system_settings_client(session.app_config_dir) as reopened_client:
            reopened_page = reopened_client.get("/system/jobs/", follow_redirects=False)
            assert reopened_page.status_code == 200
            assert "已停止" in reopened_page.text