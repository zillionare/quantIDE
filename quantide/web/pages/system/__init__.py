"""系统维护模块"""

from .calendar import calendar_page, calendar_sync
from .stocks import stocks_page, stocks_sync
from .market import market_page, market_sync

__all__ = [
    "calendar_page",
    "calendar_sync",
    "stocks_page",
    "stocks_sync",
    "market_page",
    "market_sync",
]
