"""Application settings access.

AppState remains the persisted schema. This module exposes the effective
settings view that business code should read from.
"""

from __future__ import annotations

import datetime
import urllib.parse
from dataclasses import dataclass
from typing import Any

import pytz
from loguru import logger

from quantide.config.paths import normalize_data_home


DEFAULT_TIMEZONE = pytz.timezone("Asia/Shanghai")
_LAST_APP_STATE_LOAD_ERROR: tuple[type[BaseException], str] | None = None


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


def _default_state() -> Any:
    from quantide.data.models.app_state import AppState

    state = AppState()
    state.app_home = normalize_data_home(state.app_home)
    return state


def _load_app_state() -> Any | None:
    from quantide.data.models.app_state import AppState
    from quantide.data.sqlite import db

    global _LAST_APP_STATE_LOAD_ERROR

    if not db._initialized:
        return None

    try:
        row = db["app_state"].get(1)
    except Exception as exc:
        error_key = (type(exc), str(exc))
        if error_key != _LAST_APP_STATE_LOAD_ERROR:
            logger.debug(f"load app_state failed: {exc}")
            _LAST_APP_STATE_LOAD_ERROR = error_key
        return None

    _LAST_APP_STATE_LOAD_ERROR = None

    if not row:
        return None
    return AppState.from_dict(dict(row))


def _build_gateway_base_url(state: Any) -> str:
    configured = str(getattr(state, "gateway_base_url", "") or "").strip()
    parsed = urllib.parse.urlparse(configured)
    if parsed.scheme and parsed.netloc:
        return configured.rstrip("/")

    server = str(getattr(state, "gateway_server", "") or "").strip()
    if not server:
        return ""

    scheme = str(getattr(state, "gateway_scheme", "") or "").strip() or "http"
    port = _as_int(getattr(state, "gateway_port", 0), 0)
    prefix = _normalize_path_prefix(configured or "/")

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
class Settings:
    """Effective application settings for runtime reads."""

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

    @classmethod
    def from_state(
        cls,
        state: Any | None,
        *,
        timezone: datetime.tzinfo = DEFAULT_TIMEZONE,
    ) -> "Settings":
        state = state or _default_state()
        gateway_base_url = _build_gateway_base_url(state)
        parsed = urllib.parse.urlparse(gateway_base_url)
        gateway_scheme = str(
            getattr(state, "gateway_scheme", "") or parsed.scheme or "http"
        )
        fallback_port = 443 if gateway_scheme == "https" else 80
        gateway_port = _as_int(
            getattr(state, "gateway_port", 0) or parsed.port or fallback_port,
            fallback_port,
        )

        return cls(
            app_home=normalize_data_home(getattr(state, "app_home", "")),
            app_host=str(getattr(state, "app_host", "") or "0.0.0.0"),
            app_port=_as_int(getattr(state, "app_port", 8130), 8130),
            app_prefix=_normalize_path_prefix(
                getattr(state, "app_prefix", "") or "/quantide"
            ),
            gateway_enabled=bool(getattr(state, "gateway_enabled", False)),
            gateway_base_url=gateway_base_url,
            gateway_api_key=str(getattr(state, "gateway_api_key", "") or ""),
            gateway_username=str(getattr(state, "gateway_username", "") or ""),
            gateway_password=str(getattr(state, "gateway_password", "") or ""),
            gateway_timeout=float(getattr(state, "gateway_timeout", 10) or 10),
            gateway_scheme=gateway_scheme,
            gateway_server=str(
                getattr(state, "gateway_server", "") or parsed.hostname or ""
            ),
            gateway_port=gateway_port,
            runtime_mode=str(getattr(state, "runtime_mode", "") or "").strip().lower(),
            runtime_market_adapter=str(
                getattr(state, "runtime_market_adapter", "") or ""
            ).strip().lower(),
            runtime_broker_adapter=str(
                getattr(state, "runtime_broker_adapter", "") or ""
            ).strip().lower(),
            livequote_mode=str(getattr(state, "livequote_mode", "") or "none").strip().lower(),
            epoch=_as_date(getattr(state, "epoch", None), datetime.date(2005, 1, 1)),
            timezone=timezone,
        )


def get_settings() -> Settings:
    """Return the effective application settings."""
    return Settings.from_state(_load_app_state())


def get_data_home() -> str:
    """Return the effective data home directory."""
    return get_settings().app_home


def get_timezone() -> datetime.tzinfo:
    """Return the effective timezone."""
    return get_settings().timezone


def get_epoch() -> datetime.date:
    """Return the effective data epoch."""
    return get_settings().epoch


def _state_or_default() -> Any:
    return _load_app_state() or _default_state()


def get_tushare_token() -> str:
    """Return the configured Tushare token."""
    state = _state_or_default()
    return str(getattr(state, "tushare_token", "") or "")


def get_dingtalk_access_token() -> str:
    """Return the configured Dingtalk access token."""
    state = _state_or_default()
    return str(getattr(state, "notify_dingtalk_access_token", "") or "")


def get_dingtalk_secret() -> str:
    """Return the configured Dingtalk secret."""
    state = _state_or_default()
    return str(getattr(state, "notify_dingtalk_secret", "") or "")


def get_dingtalk_keyword() -> str:
    """Return the configured Dingtalk keyword."""
    state = _state_or_default()
    return str(getattr(state, "notify_dingtalk_keyword", "") or "")


def _normalize_receivers(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]

    text = str(value or "").strip()
    if not text:
        return []

    text = text.replace(";", ",")
    return [item.strip() for item in text.split(",") if item.strip()]


def get_mail_receivers() -> list[str]:
    """Return the configured mail recipients."""
    state = _state_or_default()
    return _normalize_receivers(getattr(state, "notify_mail_to", ""))


def get_mail_sender() -> str:
    """Return the configured mail sender."""
    state = _state_or_default()
    return str(getattr(state, "notify_mail_from", "") or "")


def get_mail_server() -> str:
    """Return the configured mail server."""
    state = _state_or_default()
    return str(getattr(state, "notify_mail_server", "") or "")


__all__ = [
    "DEFAULT_TIMEZONE",
    "Settings",
    "get_settings",
    "get_data_home",
    "get_timezone",
    "get_epoch",
    "get_tushare_token",
    "get_dingtalk_access_token",
    "get_dingtalk_secret",
    "get_dingtalk_keyword",
    "get_mail_receivers",
    "get_mail_sender",
    "get_mail_server",
]