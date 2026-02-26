"""分析导航 API 模块"""

from pyqmt.web.apis.analysis.indices import app as index_router
from pyqmt.web.apis.analysis.kline import app as kline_router
from pyqmt.web.apis.analysis.search import app as search_router
from pyqmt.web.apis.analysis.sectors import app as sector_router

__all__ = ["sector_router", "index_router", "kline_router", "search_router"]
