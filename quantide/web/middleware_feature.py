"""功能可用性检查中间件."""

from __future__ import annotations

from functools import wraps

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import HTMLResponse, JSONResponse

from quantide.service.init_wizard import init_wizard


FEATURE_ROUTE_PREFIXES = {
    "/trade/simulation": "simulation",
    "/trade/live": "live_trading",
}


def _match_feature_for_path(path: str) -> str | None:
    for prefix, feature in FEATURE_ROUTE_PREFIXES.items():
        if path == prefix or path.startswith(f"{prefix}/"):
            return feature
    return None


def _is_htmx_request(headers) -> bool:
    return str(headers.get("HX-Request", "")).lower() == "true"


def _disabled_fragment_html(feature_name: str) -> str:
    return f"""
    <div class="max-w-xl mx-auto mt-10 p-6 bg-white rounded-lg shadow border border-gray-200 text-center">
        <h3 class="text-lg font-semibold text-red-700 mb-3">🔒 {feature_name}功能已禁用</h3>
        <p class="text-gray-700 mb-2">您当前未配置 gateway，因此无法使用{feature_name}功能。</p>
        <p class="text-gray-600 mb-4">如需启用，请先前往交易网关页面完成配置。</p>
        <a href="/system/gateway/" class="btn btn-primary" style="background: #D13527; color: white; text-decoration: none; padding: 10px 20px; border-radius: 6px; display: inline-block;">前往交易网关</a>
    </div>
    """


def _disabled_page_html(feature_name: str) -> str:
    fragment = _disabled_fragment_html(feature_name)
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>功能禁用 - Quantide</title>
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                background: #f5f5f5;
                padding: 40px;
            }}
            .btn:hover {{ background: #b52d20; }}
        </style>
    </head>
    <body>
        {fragment}
    </body>
    </html>
    """


def _feature_disabled_response(feature_name: str, *, htmx: bool = False):
    if htmx:
        return HTMLResponse(content=_disabled_fragment_html(feature_name), status_code=403)
    return HTMLResponse(content=_disabled_page_html(feature_name), status_code=403)


class FeatureCheckMiddleware(BaseHTTPMiddleware):
    """功能可用性检查中间件

    根据初始化后的功能状态收紧交易入口。
    """

    async def dispatch(self, request, call_next):
        """处理请求"""
        feature_key = _match_feature_for_path(request.url.path)
        if feature_key is not None:
            feature = get_feature_status().get(feature_key, {})
            if not feature.get("available", False):
                feature_name = str(feature.get("name") or feature_key)
                if request.method == "GET":
                    return _feature_disabled_response(
                        feature_name,
                        htmx=_is_htmx_request(request.headers),
                    )
                return JSONResponse(
                    {"error": f"{feature_name}功能已禁用，请先在交易网关页面配置 gateway"},
                    status_code=403,
                )
        response = await call_next(request)
        return response


def require_qmt_configured(func):
    """装饰器：要求必须配置 gateway 才能访问

    用于在路由处理函数中手动检查 gateway 配置。

    Args:
        func: 被装饰的函数

    Returns:
        如果未配置 gateway，返回提示页面；否则执行原函数
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        feature = get_feature_status().get("live_trading", {})
        if not feature.get("available", False):
            feature_name = str(feature.get("name") or "实盘交易")
            return _feature_disabled_response(feature_name)
        return func(*args, **kwargs)

    return wrapper


def get_feature_status() -> dict[str, dict]:
    """获取功能状态信息

    Returns:
        功能状态字典，包含各功能的可用状态和提示信息
    """
    try:
        status = init_wizard.get_feature_status()

        features = {
            "backtest": {
                "name": "回测功能",
                "available": status["backtest"],
                "icon": "📊",
                "description": "使用历史数据进行策略回测",
                "required": "完成初始化并配置 Tushare",
            },
            "simulation": {
                "name": "仿真交易",
                "available": status["simulation"],
                "icon": "🎮",
                "description": "使用 gateway 进行仿真交易",
                "required": "完成初始化并启用 gateway",
            },
            "live_trading": {
                "name": "实盘交易",
                "available": status["live_trading"],
                "icon": "💰",
                "description": "使用 gateway 进行实盘交易",
                "required": "完成初始化并启用 gateway",
            },
        }

        return features
    except Exception:
        # 如果检查失败，返回全部不可用
        return {
            "backtest": {
                "name": "回测功能",
                "available": False,
                "icon": "📊",
                "description": "使用历史数据进行策略回测",
                "required": "完成初始化",
            },
            "simulation": {
                "name": "仿真交易",
                "available": False,
                "icon": "🎮",
                "description": "使用 gateway 进行仿真交易",
                "required": "完成初始化",
            },
            "live_trading": {
                "name": "实盘交易",
                "available": False,
                "icon": "💰",
                "description": "使用 gateway 进行实盘交易",
                "required": "完成初始化",
            },
        }
