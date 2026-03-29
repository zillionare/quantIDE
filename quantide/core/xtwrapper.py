"""Removed local QMT data wrapper.

The quantide subject application no longer provides local broker or market-data
access. Live trading and quotes must go through qmt-gateway.
"""

from typing import Any


_REMOVED_MESSAGE = (
    "本地 QMT/行情能力已从 quantide 主体移除。"
    "请改用 qmt-gateway 或其它远程数据源。"
)


def _raise_removed() -> None:
    raise RuntimeError(_REMOVED_MESSAGE)


def require_xt() -> Any:
    _raise_removed()


def on_subscribe_callback(data):
    _raise_removed()


def subcribe_live():
    _raise_removed()


def cache_bars(frame_type):
    _raise_removed()


def get_bars(symbols, frame_type, start, end):
    _raise_removed()


def get_stock_list():
    _raise_removed()


def get_sectors():
    _raise_removed()


def get_calendar(end=None):
    _raise_removed()


def get_security_info(symbol: str):
    _raise_removed()


def get_factor_ratio(symbol: str, start, end):
    _raise_removed()