"""API 模块

提供 HTTP API 和 WebSocket 接口。
"""

from qmt_gateway.apis.auth import login_required, register_routes as register_auth_routes
from qmt_gateway.apis.quotes import quote_ws, register_routes as register_quotes_routes
from qmt_gateway.apis.sectors import register_routes as register_sectors_routes
from qmt_gateway.apis.trade import register_routes as register_trade_routes

__all__ = [
    "login_required",
    "register_auth_routes",
    "register_trade_routes",
    "register_sectors_routes",
    "register_quotes_routes",
    "quote_ws",
]
