"""系统设置 - 交易网关模块

显示当前交易网关的配置状态，提供连接测试和配置查看功能。
"""

from __future__ import annotations

import datetime
import time
import urllib.request

from fasthtml.common import *
from loguru import logger
from monsterui.all import *

from quantide.config.settings import get_settings
from quantide.data.models.app_state import AppState
from quantide.data.sqlite import db
from quantide.web.layouts.main import MainLayout
from quantide.web.theme import AppTheme, PRIMARY_COLOR

# 定义子路由应用
system_gateway_app, rt = fast_app(hdrs=AppTheme.headers())


# ========== 数据获取 ==========

def _load_gateway_config() -> dict:
    """从数据库加载网关配置"""
    try:
        row = db["app_state"].get(1)
        if row:
            state = AppState.from_dict(dict(row))
            return {
                "enabled": state.gateway_enabled,
                "server": state.gateway_server,
                "port": state.gateway_port,
                "api_key": state.gateway_api_key,
                "scheme": state.gateway_scheme,
                "base_url": state.gateway_base_url,
                "timeout": state.gateway_timeout,
                "username": state.gateway_username,
            }
    except Exception as e:
        logger.warning(f"加载网关配置失败: {e}")

    settings = get_settings()
    return {
        "enabled": settings.gateway_enabled,
        "server": settings.gateway_server,
        "port": settings.gateway_port,
        "api_key": settings.gateway_api_key,
        "scheme": settings.gateway_scheme,
        "base_url": settings.gateway_base_url,
        "timeout": settings.gateway_timeout,
        "username": settings.gateway_username,
    }


def _test_gateway_connection(base_url: str, timeout: int = 10) -> dict:
    """测试网关连接

    Returns:
        包含 success, latency_ms, error 的字典
    """
    ping_url = f"{base_url}/ping"
    start = time.time()
    try:
        req = urllib.request.Request(ping_url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            latency_ms = int((time.time() - start) * 1000)
            if resp.getcode() == 200:
                return {"success": True, "latency_ms": latency_ms}
            return {"success": False, "latency_ms": latency_ms, "error": f"HTTP {resp.getcode()}"}
    except urllib.request.URLError as e:
        latency_ms = int((time.time() - start) * 1000)
        return {"success": False, "latency_ms": latency_ms, "error": str(e.reason)}
    except Exception as e:
        latency_ms = int((time.time() - start) * 1000)
        return {"success": False, "latency_ms": latency_ms, "error": str(e)}


# ========== UI 组件 ==========

def _build_connection_status(config: dict, test_result: dict | None = None) -> Div:
    """构建连接状态卡片"""
    if not config["enabled"]:
        return Div(
            Div(
                Span("⚫", cls="text-2xl"),
                cls="mb-3",
            ),
            P("网关未启用", cls="text-lg font-medium text-gray-700"),
            P(
                "请在初始化向导中配置并启用交易网关。",
                cls="text-sm text-gray-500 mt-1",
            ),
            cls="p-6 bg-gray-50 rounded-lg border border-gray-200",
        )

    base_url = config["base_url"]
    if not base_url:
        return Div(
            Div(
                Span("⚫", cls="text-2xl"),
                cls="mb-3",
            ),
            P("网关地址未配置", cls="text-lg font-medium text-gray-700"),
            P(
                "请在初始化向导中配置网关地址。",
                cls="text-sm text-gray-500 mt-1",
            ),
            cls="p-6 bg-yellow-50 rounded-lg border border-yellow-200",
        )

    if test_result:
        if test_result["success"]:
            status_color = "text-green-500"
            status_bg = "bg-green-50"
            status_border = "border-green-200"
            status_icon = "🟢"
            status_text = f"已连接 (延迟: {test_result['latency_ms']}ms)"
        else:
            status_color = "text-red-500"
            status_bg = "bg-red-50"
            status_border = "border-red-200"
            status_icon = "🔴"
            status_text = f"连接失败: {test_result['error']}"
    else:
        status_color = "text-gray-500"
        status_bg = "bg-gray-50"
        status_border = "border-gray-200"
        status_icon = "⚪"
        status_text = "未测试"

    return Div(
        Div(
            Span(status_icon, cls="text-2xl mr-3"),
            Span(base_url, cls="text-lg font-medium text-gray-700"),
            cls="flex items-center mb-3",
        ),
        P(f"状态: {status_text}", cls=f"text-sm {status_color}"),
        cls=f"p-6 {status_bg} rounded-lg border {status_border}",
    )


def _build_config_form(config: dict) -> Div:
    """构建配置信息卡片"""
    masked_key = ""
    if config.get("api_key"):
        key = config["api_key"]
        if len(key) > 4:
            masked_key = key[:2] + "•" * (len(key) - 4) + key[-2:]
        else:
            masked_key = "•" * len(key)

    return Div(
        Div(
            H3("连接配置", cls="text-lg font-semibold text-gray-900 mb-4"),
            cls="border-b pb-2 mb-4",
        ),
        Div(
            Div(
                Span("服务器地址:", cls="text-sm text-gray-500"),
                Span(config["server"] or "-", cls="text-sm font-medium text-gray-900 ml-2"),
                cls="flex justify-between py-2 border-b border-gray-100",
            ),
            Div(
                Span("端口:", cls="text-sm text-gray-500"),
                Span(str(config["port"]), cls="text-sm font-medium text-gray-900 ml-2"),
                cls="flex justify-between py-2 border-b border-gray-100",
            ),
            Div(
                Span("API Key:", cls="text-sm text-gray-500"),
                Span(masked_key or "-", cls="text-sm font-medium text-gray-900 ml-2"),
                cls="flex justify-between py-2 border-b border-gray-100",
            ),
            Div(
                Span("协议:", cls="text-sm text-gray-500"),
                Span(config["scheme"] or "http", cls="text-sm font-medium text-gray-900 ml-2"),
                cls="flex justify-between py-2 border-b border-gray-100",
            ),
            Div(
                Span("超时(秒):", cls="text-sm text-gray-500"),
                Span(str(config["timeout"]), cls="text-sm font-medium text-gray-900 ml-2"),
                cls="flex justify-between py-2",
            ),
            cls="text-sm",
        ),
        cls="p-6 bg-white rounded-lg shadow",
    )


# ========== 路由 ==========

@rt("/")
async def index():
    """交易网关页面"""
    config = _load_gateway_config()

    layout = MainLayout(title="交易网关")
    layout.set_sidebar_active("/system/gateway")

    page_content = Div(
        Div(
            Div(
                UkIcon("radio", size=32, cls="mr-3", style=f"color: {PRIMARY_COLOR};"),
                H2("交易网关", cls="text-2xl font-bold"),
                cls="flex items-center",
            ),
            cls="mb-6",
        ),
        # 连接状态
        Div(
            H3("连接状态", cls="text-lg font-semibold text-gray-900 mb-3"),
            Div(
                _build_connection_status(config),
                id="gateway-status",
                cls="mb-4",
            ),
            Div(
                A(
                    "连接测试",
                    href="/system/gateway/test",
                    cls="btn btn-primary",
                ),
                cls="mt-3",
            ),
            cls="mb-6",
        ),
        # 连接配置
        Div(
            _build_config_form(config),
            cls="mb-6",
        ),
        # 提示
        Div(
            UkIcon("info", size=16, cls="text-yellow-500 mr-2"),
            Span(
                "如需修改配置，请通过初始化向导重新配置。",
                cls="text-sm text-gray-500",
            ),
            cls="flex items-center p-4 bg-yellow-50 rounded-lg",
        ),
        cls="p-8",
    )

    layout.main_block = page_content
    return layout.render()


@rt("/test")
async def test_connection():
    """测试网关连接"""
    config = _load_gateway_config()

    if not config["enabled"] or not config["base_url"]:
        return _build_connection_status(config, None)

    test_result = _test_gateway_connection(
        config["base_url"],
        timeout=config["timeout"],
    )
    return _build_connection_status(config, test_result)
