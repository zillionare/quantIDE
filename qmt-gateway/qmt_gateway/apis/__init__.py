"""API 模块.

提供 HTTP API 和 WebSocket 接口。
"""

from qmt_gateway.apis.auth import login_required
from qmt_gateway.apis.auth import register_routes as register_auth_routes
from qmt_gateway.apis.history import register_routes as register_history_routes
from qmt_gateway.apis.quotes import quote_ws
from qmt_gateway.apis.quotes import register_routes as register_quotes_routes
from qmt_gateway.apis.stock import register_routes as register_stock_routes
from qmt_gateway.apis.trade import register_routes as register_trade_routes

__all__ = [
    "login_required",
    "register_auth_routes",
    "register_history_routes",
    "register_trade_routes",
    "register_quotes_routes",
    "register_stock_routes",
    "quote_ws",
]
