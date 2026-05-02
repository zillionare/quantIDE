"""系统设置 - 交易网关模块

显示当前交易网关的配置状态，提供连接测试和配置查看功能。
"""

from __future__ import annotations

import datetime
import time
import urllib.request
from typing import Any

from fasthtml.common import *
from loguru import logger
from monsterui.all import *

from quantide.config.settings import get_settings
from quantide.data.models.app_state import AppState
from quantide.data.sqlite import db
from quantide.service.init_wizard import init_wizard
from quantide.web.layouts.main import MainLayout
from quantide.web.theme import AppTheme, PRIMARY_COLOR

# 定义子路由应用
system_gateway_app, rt = fast_app(hdrs=AppTheme.headers())


def _normalize_prefix(prefix: str) -> str:
    text = str(prefix or "/").strip() or "/"
    if not text.startswith("/"):
        text = f"/{text}"
    if text != "/":
        text = text.rstrip("/")
    return text


def _compose_gateway_base_url(config: dict[str, Any]) -> str:
    server = str(config.get("server") or "").strip()
    if not server:
        return ""

    scheme = str(config.get("scheme") or "http").strip() or "http"
    prefix = _normalize_prefix(str(config.get("prefix") or "/"))
    base = f"{scheme}://{server}:{int(config.get('port') or 8000)}"
    return base if prefix == "/" else f"{base}{prefix}"


# ========== 数据获取 ==========

def _load_gateway_config() -> dict[str, Any]:
    """从数据库加载网关配置"""
    try:
        row = db["app_state"].get(1)
        if row:
            state = AppState.from_dict(dict(row))
            config = {
                "enabled": state.gateway_enabled,
                "server": state.gateway_server,
                "port": state.gateway_port,
                "api_key": state.gateway_api_key,
                "scheme": state.gateway_scheme,
                "prefix": state.gateway_base_url or "/",
                "timeout": state.gateway_timeout,
                "username": state.gateway_username,
            }
            config["base_url"] = _compose_gateway_base_url(config)
            return config
    except Exception as e:
        logger.warning(f"加载网关配置失败: {e}")

    settings = get_settings()
    config = {
        "enabled": settings.gateway_enabled,
        "server": settings.gateway_server,
        "port": settings.gateway_port,
        "api_key": settings.gateway_api_key,
        "scheme": settings.gateway_scheme,
        "prefix": settings.gateway_base_url or "/",
        "timeout": settings.gateway_timeout,
        "username": settings.gateway_username,
    }
    config["base_url"] = _compose_gateway_base_url(config)
    return config


def _test_gateway_connection(base_url: str, timeout: int = 10) -> dict[str, Any]:
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


def _coerce_gateway_form(data: Mapping[str, Any]) -> dict[str, Any]:
    enabled = str(data.get("gateway_enabled", "")).lower() in {"on", "true", "1", "yes"}
    server = str(data.get("gateway_server", "")).strip()
    api_key = str(data.get("gateway_api_key", "")).strip()
    prefix = _normalize_prefix(str(data.get("gateway_prefix", "/")))

    port_raw = str(data.get("gateway_port", "8000")).strip() or "8000"
    timeout_raw = str(data.get("gateway_timeout", "10")).strip() or "10"

    port = int(port_raw)
    timeout = max(1, int(timeout_raw))

    config = {
        "enabled": enabled,
        "server": server,
        "port": port,
        "api_key": api_key,
        "scheme": "http",
        "prefix": prefix,
        "timeout": timeout,
    }
    config["base_url"] = _compose_gateway_base_url(config)
    return config


# ========== UI 组件 ==========

def _build_connection_status(config: dict[str, Any], test_result: dict[str, Any] | None = None) -> Div:
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


def _build_flash(message: str, tone: str) -> Div:
    color_map = {
        "success": ("bg-green-50", "border-green-200", "text-green-700"),
        "error": ("bg-red-50", "border-red-200", "text-red-700"),
        "info": ("bg-blue-50", "border-blue-200", "text-blue-700"),
    }
    bg, border, text = color_map.get(tone, color_map["info"])
    return Div(message, cls=f"mb-4 rounded-lg border px-4 py-3 text-sm {bg} {border} {text}")


def _build_config_form(config: dict[str, Any]) -> Div:
    """构建配置编辑表单"""
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
        Form(
            Div(
                Label(
                    Input(
                        type="checkbox",
                        name="gateway_enabled",
                        checked=bool(config.get("enabled")),
                        cls="mr-2",
                    ),
                    Span("启用交易网关", cls="text-sm font-medium text-gray-900"),
                    cls="flex items-center",
                ),
                cls="mb-4",
            ),
            Div(
                Div(
                    Label("服务器地址", cls="block text-sm font-medium text-gray-700 mb-1"),
                    Input(
                        type="text",
                        name="gateway_server",
                        value=config.get("server", ""),
                        placeholder="localhost",
                        cls="input input-bordered w-full",
                    ),
                    cls="mb-3",
                ),
                Div(
                    Label("端口", cls="block text-sm font-medium text-gray-700 mb-1"),
                    Input(
                        type="number",
                        name="gateway_port",
                        value=str(config.get("port", 8000)),
                        min="1",
                        cls="input input-bordered w-full",
                    ),
                    cls="mb-3",
                ),
                Div(
                    Label("路径前缀", cls="block text-sm font-medium text-gray-700 mb-1"),
                    Input(
                        type="text",
                        name="gateway_prefix",
                        value=config.get("prefix", "/"),
                        placeholder="/",
                        cls="input input-bordered w-full",
                    ),
                    cls="mb-3",
                ),
                Div(
                    Label("API Key", cls="block text-sm font-medium text-gray-700 mb-1"),
                    Input(
                        type="password",
                        name="gateway_api_key",
                        value=config.get("api_key", ""),
                        placeholder=masked_key or "请输入网关访问密钥",
                        cls="input input-bordered w-full",
                    ),
                    cls="mb-3",
                ),
                Div(
                    Label("超时(秒)", cls="block text-sm font-medium text-gray-700 mb-1"),
                    Input(
                        type="number",
                        name="gateway_timeout",
                        value=str(config.get("timeout", 10)),
                        min="1",
                        cls="input input-bordered w-full",
                    ),
                    cls="mb-4",
                ),
                cls="grid grid-cols-1 md:grid-cols-2 gap-4",
            ),
            Div(
                Button(
                    "连接测试",
                    type="submit",
                    formaction="/system/gateway/test",
                    formmethod="post",
                    cls="btn btn-secondary",
                ),
                Button(
                    "保存配置",
                    type="submit",
                    formaction="/system/gateway/save",
                    formmethod="post",
                    cls="btn btn-primary",
                ),
                cls="flex gap-3",
            ),
        ),
        cls="p-6 bg-white rounded-lg shadow",
    )


def _render_page(
    config: dict[str, Any],
    *,
    test_result: dict[str, Any] | None = None,
    success_message: str | None = None,
    error_message: str | None = None,
):
    layout = MainLayout(title="交易网关")
    layout.set_sidebar_active("/system/gateway")

    blocks: list[Any] = []
    if success_message:
        blocks.append(_build_flash(success_message, "success"))
    if error_message:
        blocks.append(_build_flash(error_message, "error"))

    blocks.extend(
        [
            Div(
                H3("连接状态", cls="text-lg font-semibold text-gray-900 mb-3"),
                Div(
                    _build_connection_status(config, test_result),
                    id="gateway-status",
                    cls="mb-4",
                ),
                cls="mb-6",
            ),
            Div(_build_config_form(config), cls="mb-6"),
            Div(
                UkIcon("info", size=16, cls="text-yellow-500 mr-2"),
                Span(
                    "保存后会持久化到应用配置；若运行时已启动，重新连接相关能力可能需要重新加载应用。",
                    cls="text-sm text-gray-500",
                ),
                cls="flex items-center p-4 bg-yellow-50 rounded-lg",
            ),
        ]
    )

    page_content = Div(
        Div(
            Div(
                UkIcon("radio", size=32, cls="mr-3", style=f"color: {PRIMARY_COLOR};"),
                H2("交易网关", cls="text-2xl font-bold"),
                cls="flex items-center",
            ),
            cls="mb-6",
        ),
        *blocks,
        cls="p-8",
    )

    layout.main_block = page_content
    return layout.render()


# ========== 路由 ==========

@rt("/")
async def index():
    """交易网关页面"""
    config = _load_gateway_config()
    return _render_page(config)


@rt("/test", methods=["GET", "POST"])
async def test_connection(req):
    """测试网关连接"""
    if req.method == "POST":
        form = await req.form()
        try:
            config = _coerce_gateway_form(form)
        except ValueError as exc:
            fallback = _load_gateway_config()
            return _render_page(fallback, error_message=f"参数错误：{exc}")
    else:
        config = _load_gateway_config()

    if not config["enabled"] or not config["base_url"]:
        return _render_page(config, error_message="网关未启用或地址未配置，无法执行连通性测试")

    test_result = _test_gateway_connection(
        config["base_url"],
        timeout=config["timeout"],
    )
    message = "连通性测试通过" if test_result["success"] else "连通性测试失败"
    return _render_page(
        config,
        test_result=test_result,
        success_message=message if test_result["success"] else None,
        error_message=None if test_result["success"] else message,
    )


@rt("/save", methods=["POST"])
async def save_config(req):
    """保存网关配置"""
    form = await req.form()
    try:
        config = _coerce_gateway_form(form)
        init_wizard.save_gateway_config(
            enabled=config["enabled"],
            server=config["server"],
            port=config["port"],
            prefix=config["prefix"],
            api_key=config["api_key"],
            timeout=config["timeout"],
        )
        saved = _load_gateway_config()
        return _render_page(saved, success_message="网关配置已保存")
    except Exception as exc:
        logger.warning(f"保存网关配置失败: {exc}")
        fallback = _load_gateway_config()
        try:
            fallback.update(_coerce_gateway_form(form))
        except Exception:
            pass
        return _render_page(fallback, error_message=f"保存失败：{exc}")
