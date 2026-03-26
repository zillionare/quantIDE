"""Removed local sector/index data helpers.

This module exists only to report that subject-side sector/index补充数据能力
已经移除。
"""


_REMOVED_MESSAGE = "板块/指数 xtdata 补充功能已从 pyqmt 主体移除。"


def _raise_removed() -> None:
    raise RuntimeError(_REMOVED_MESSAGE)


def fetch_sector_list(trade_date=None):
    _raise_removed()


def fetch_sector_constituents(sector_id: str, trade_date=None):
    _raise_removed()


def fetch_sector_bars(sector_id: str, start_date, end_date):
    _raise_removed()


def get_index_list() -> list[str]:
    _raise_removed()


def get_tradeable_sectors() -> list[str]:
    _raise_removed()