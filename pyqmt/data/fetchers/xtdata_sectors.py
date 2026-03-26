"""Removed local sector/index data helpers.

The pyqmt subject application no longer supplements sector data from the local
QMT client. Sector-related metadata and bar synchronization are no longer
supported in the subject app.
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