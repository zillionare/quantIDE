"""分析导航 API 模块"""

from pyqmt.web.apis.analysis.kline import app as kline_router
from pyqmt.web.apis.analysis.search import app as search_router

__all__ = ["kline_router", "search_router"]
