"""运行时配置访问。

提供数据库优先、cfg4py 兜底的统一读取入口，避免业务运行时直接分散依赖
cfg4py。当前仅覆盖 Phase 4 的核心运行时路径。
"""

from __future__ import annotations

import datetime
import urllib.parse
from dataclasses import dataclass
from typing import Any

import cfg4py
import pytz
from loguru import logger


_LAST_APP_STATE_LOAD_ERROR: tuple[type[BaseException], str] | None = None


def _cfg_value(path: str, default: Any = None) -> Any:
    current = cfg4py.get_instance()
    for name in path.split("."):
        current = getattr(current, name, None)
        if current is None:
            return default
    return current


def _as_date(value: Any, default: datetime.date) -> datetime.date:
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, str):
        try:
            return datetime.datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError:
            return default
    return default


def _as_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_path_prefix(value: str, default: str = "/") -> str:
    text = str(value or "").strip()
    if not text:
        return default
    if not text.startswith("/"):
        text = "/" + text
    return "/" if text == "/" else text.rstrip("/")


def _load_app_state() -> Any | None:
    from pyqmt.data.sqlite import db
    from pyqmt.data.models.app_state import AppState

    global _LAST_APP_STATE_LOAD_ERROR

    if not db._initialized:
        return None

    try:
        row = db["app_state"].get(1)
    except Exception as exc:
        error_key = (type(exc), str(exc))
        if error_key != _LAST_APP_STATE_LOAD_ERROR:
            logger.debug(f"load app_state failed, fallback to cfg4py: {exc}")
            _LAST_APP_STATE_LOAD_ERROR = error_key
        return None

    _LAST_APP_STATE_LOAD_ERROR = None

    if not row:
        return None
    return AppState.from_dict(dict(row))


def _build_gateway_base_url(state: Any | None) -> str:
    db_value = str(getattr(state, "gateway_base_url", "") or "").strip()
    parsed = urllib.parse.urlparse(db_value)
    if parsed.scheme and parsed.netloc:
        return db_value.rstrip("/")

    server = str(getattr(state, "gateway_server", "") or "").strip()
    if not server:
        cfg_base_url = str(_cfg_value("gateway.base_url", "") or "").strip()
        return cfg_base_url.rstrip("/")

    scheme = str(getattr(state, "gateway_scheme", "") or "").strip() or "http"
    port = _as_int(getattr(state, "gateway_port", 0), 0)
    prefix = _normalize_path_prefix(db_value or "/")
    if server.startswith("http://") or server.startswith("https://"):
        base = server.rstrip("/")
    else:
        base = f"{scheme}://{server}"
        default_port = 443 if scheme == "https" else 80
        if port > 0 and port != default_port:
            base = f"{base}:{port}"
    if prefix == "/":
        return base
    return f"{base}{prefix}"


@dataclass(frozen=True)
class RuntimeConfig:
    """统一运行时配置视图。"""

    app_home: str
    app_host: str
    app_port: int
    app_prefix: str
    gateway_enabled: bool
    gateway_base_url: str
    gateway_api_key: str
    gateway_username: str
    gateway_password: str
    gateway_timeout: float
    gateway_scheme: str
    gateway_server: str
    gateway_port: int
    runtime_mode: str
    runtime_market_adapter: str
    runtime_broker_adapter: str
    livequote_mode: str
    epoch: datetime.date
    timezone: datetime.tzinfo


def get_runtime_config() -> RuntimeConfig:
    """获取数据库优先的运行时配置。"""
    state = _load_app_state()

    app_home = str(
        getattr(state, "app_home", "") or _cfg_value("home", "") or ""
    )
    app_host = str(
        getattr(state, "app_host", "") or _cfg_value("server.host", "0.0.0.0")
    )
    app_port = _as_int(
        getattr(state, "app_port", 0) or _cfg_value("server.port", 8130),
        8130,
    )
    app_prefix = _normalize_path_prefix(
        getattr(state, "app_prefix", "") or _cfg_value("server.prefix", "/zillionare-qmt")
    )
    if state is not None:
        gateway_enabled = bool(getattr(state, "gateway_enabled", False))
    else:
        gateway_enabled = _cfg_value("livequote.mode", "").strip().lower() == "gateway"
    gateway_base_url = _build_gateway_base_url(state)
    gateway_api_key = str(
        getattr(state, "gateway_api_key", "")
        or _cfg_value("apikeys.clients", [{}])[0].get("key", "")
        if _cfg_value("apikeys.clients", [])
        else ""
    )
    gateway_username = str(
        getattr(state, "gateway_username", "") or _cfg_value("gateway.username", "")
    )
    gateway_password = str(
        getattr(state, "gateway_password", "") or _cfg_value("gateway.password", "")
    )
    gateway_timeout = float(
        getattr(state, "gateway_timeout", 0) or _cfg_value("gateway.timeout", 10)
    )
    gateway_scheme = str(
        getattr(state, "gateway_scheme", "") or urllib.parse.urlparse(gateway_base_url).scheme or "http"
    )
    gateway_server = str(getattr(state, "gateway_server", "") or urllib.parse.urlparse(gateway_base_url).hostname or "")
    gateway_port = _as_int(
        getattr(state, "gateway_port", 0)
        or urllib.parse.urlparse(gateway_base_url).port
        or (443 if gateway_scheme == "https" else 80),
        80,
    )
    runtime_mode = str(
        getattr(state, "runtime_mode", "") or _cfg_value("runtime.mode", "") or ""
    ).strip().lower()
    runtime_market_adapter = str(
        getattr(state, "runtime_market_adapter", "")
        or _cfg_value("runtime.market_adapter", "")
        or ""
    ).strip().lower()
    runtime_broker_adapter = str(
        getattr(state, "runtime_broker_adapter", "")
        or _cfg_value("runtime.broker_adapter", "")
        or ""
    ).strip().lower()
    livequote_mode = str(
        getattr(state, "livequote_mode", "") or _cfg_value("livequote.mode", "none") or "none"
    ).strip().lower()
    epoch = _as_date(
        getattr(state, "epoch", None) or _cfg_value("epoch", None),
        datetime.date(2005, 1, 1),
    )
    timezone = getattr(cfg4py.get_instance(), "TIMEZONE", pytz.timezone("Asia/Shanghai"))

    return RuntimeConfig(
        app_home=app_home,
        app_host=app_host,
        app_port=app_port,
        app_prefix=app_prefix,
        gateway_enabled=gateway_enabled,
        gateway_base_url=gateway_base_url,
        gateway_api_key=gateway_api_key,
        gateway_username=gateway_username,
        gateway_password=gateway_password,
        gateway_timeout=gateway_timeout,
        gateway_scheme=gateway_scheme,
        gateway_server=gateway_server,
        gateway_port=gateway_port,
        runtime_mode=runtime_mode,
        runtime_market_adapter=runtime_market_adapter,
        runtime_broker_adapter=runtime_broker_adapter,
        livequote_mode=livequote_mode,
        epoch=epoch,
        timezone=timezone,
    )


def get_runtime_home() -> str:
    """获取运行时 home 目录。"""
    return get_runtime_config().app_home


def get_runtime_timezone() -> datetime.tzinfo:
    """获取运行时时区。"""
    return get_runtime_config().timezone


def get_runtime_epoch() -> datetime.date:
    """获取运行时数据起点日期。"""
    return get_runtime_config().epoch


def get_runtime_tushare_token() -> str:
    """获取运行时 tushare token。"""
    state = _load_app_state()
    return str(getattr(state, "tushare_token", "") or _cfg_value("tushare_token", "") or "")


def get_runtime_dingtalk_access_token() -> str:
    """获取运行时钉钉 access token。"""
    state = _load_app_state()
    return str(
        getattr(state, "notify_dingtalk_access_token", "")
        or _cfg_value("notify.dingtalk.access_token", "")
        or ""
    )


def get_runtime_dingtalk_secret() -> str:
    """获取运行时钉钉 secret。"""
    state = _load_app_state()
    return str(
        getattr(state, "notify_dingtalk_secret", "")
        or _cfg_value("notify.dingtalk.secret", "")
        or ""
    )


def get_runtime_dingtalk_keyword() -> str:
    """获取运行时钉钉 keyword。"""
    state = _load_app_state()
    return str(
        getattr(state, "notify_dingtalk_keyword", "")
        or _cfg_value("notify.dingtalk.keyword", "")
        or ""
    )


def _normalize_receivers(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    text = str(value or "").strip()
    if not text:
        return []
    text = text.replace(";", ",")
    return [item.strip() for item in text.split(",") if item.strip()]


def get_runtime_mail_receivers() -> list[str]:
    """获取运行时邮件接收者列表。"""
    state = _load_app_state()
    value = getattr(state, "notify_mail_to", "") or _cfg_value("notify.mail.mail_to", [])
    return _normalize_receivers(value)


def get_runtime_mail_sender() -> str:
    """获取运行时邮件发送者。"""
    state = _load_app_state()
    return str(
        getattr(state, "notify_mail_from", "")
        or _cfg_value("notify.mail.mail_from", "")
        or ""
    )


def get_runtime_mail_server() -> str:
    """获取运行时邮件服务器。"""
    state = _load_app_state()
    return str(
        getattr(state, "notify_mail_server", "")
        or _cfg_value("notify.mail.mail_server", "")
        or ""
    )


__all__ = [
    "RuntimeConfig",
    "get_runtime_config",
    "get_runtime_home",
    "get_runtime_timezone",
    "get_runtime_epoch",
    "get_runtime_tushare_token",
    "get_runtime_dingtalk_access_token",
    "get_runtime_dingtalk_secret",
    "get_runtime_dingtalk_keyword",
    "get_runtime_mail_receivers",
    "get_runtime_mail_sender",
    "get_runtime_mail_server",
]