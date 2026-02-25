"""分析导航 API"""

from .sectors import router as sector_router
from .indices import router as index_router
from .kline import router as kline_router

__all__ = ["sector_router", "index_router", "kline_router"]
