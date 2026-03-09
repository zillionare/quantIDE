"""功能可用性检查中间件

检查用户是否配置了 QMT，如果没有则禁用实盘/仿真交易功能。
"""

from functools import wraps

from fasthtml.common import Div, H3, P, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware

from pyqmt.service.init_wizard import init_wizard


class FeatureCheckMiddleware(BaseHTTPMiddleware):
    """功能可用性检查中间件

    根据 QMT 配置情况，控制实盘/仿真交易功能的可用性。
    未配置 QMT 时，这些功能会被禁用（显示提示页面）。
    """

    # 需要 QMT 配置才能访问的路径
    QMT_REQUIRED_PATHS = [
        "/live-trading",  # 实盘交易
        "/simulation",    # 仿真交易
        "/paper-trading", # 模拟交易
        "/realtime",      # 实时行情
    ]

    async def dispatch(self, request, call_next):
        """处理请求"""
        path = request.url.path

        # 检查是否是受限制的路径
        requires_qmt = any(path.startswith(p) for p in self.QMT_REQUIRED_PATHS)

        if requires_qmt:
            # 检查是否配置了 QMT
            try:
                if not init_wizard.has_qmt_configured():
                    # 未配置 QMT，返回提示页面
                    if request.method == "GET":
                        return self._render_disabled_page("实盘/仿真交易")
                    else:
                        from fasthtml.common import JSONResponse
                        return JSONResponse(
                            {
                                "error": "QMT 未配置",
                                "message": "请先配置 QMT 账号才能使用此功能",
                                "redirect": "/settings?qmt=required"
                            },
                            status_code=403,
                        )
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(f"QMT 配置检查失败: {e}")

        response = await call_next(request)
        return response

    def _render_disabled_page(self, feature_name: str):
        """渲染功能禁用提示页面"""
        from pyqmt.web.theme import AppTheme

        content = Div(
            H3(f"🔒 {feature_name}功能已禁用", cls="text-center mb-4"),
            P(
                f"您当前未配置 QMT 账号，因此无法使用{feature_name}功能。",
                cls="text-center text-gray-600 mb-4"
            ),
            P(
                "如需使用此功能，请前往设置页面配置 QMT 账号信息。",
                cls="text-center text-gray-600 mb-6"
            ),
            A(
                "前往设置",
                href="/settings",
                cls="btn btn-primary",
                style="background: #D13527;"
            ),
            cls="max-w-md mx-auto mt-10 p-6 text-center"
        )

        # 返回简单的 HTML 页面
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>功能禁用 - PyQMT</title>
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
                       background: #f5f5f5; padding: 40px; }}
                .container {{ max-width: 500px; margin: 0 auto; background: white; 
                            padding: 40px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
                h3 {{ color: #D13527; margin-bottom: 20px; }}
                .btn {{ display: inline-block; padding: 10px 24px; background: #D13527; 
                       color: white; text-decoration: none; border-radius: 4px; }}
                .btn:hover {{ background: #b52d20; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h3>🔒 {feature_name}功能已禁用</h3>
                <p>您当前未配置 QMT 账号，因此无法使用{feature_name}功能。</p>
                <p>如需使用此功能，请前往设置页面配置 QMT 账号信息。</p>
                <br>
                <a href="/settings" class="btn">前往设置</a>
                <a href="/" class="btn" style="background: #666; margin-left: 10px;">返回首页</a>
            </div>
        </body>
        </html>
        """
        from starlette.responses import HTMLResponse
        return HTMLResponse(content=html_content, status_code=403)


def require_qmt_configured(func):
    """装饰器：要求必须配置 QMT 才能访问

    用于在路由处理函数中手动检查 QMT 配置。

    Args:
        func: 被装饰的函数

    Returns:
        如果未配置 QMT，返回提示页面；否则执行原函数
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            if not init_wizard.has_qmt_configured():
                from fasthtml.common import RedirectResponse
                return RedirectResponse("/settings?qmt=required", status_code=302)
        except Exception:
            pass
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
                "description": "使用 QMT 进行仿真交易",
                "required": "配置 QMT 账号",
            },
            "live_trading": {
                "name": "实盘交易",
                "available": status["live_trading"],
                "icon": "💰",
                "description": "使用 QMT 进行实盘交易",
                "required": "配置 QMT 账号",
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
                "description": "使用 QMT 进行仿真交易",
                "required": "配置 QMT 账号",
            },
            "live_trading": {
                "name": "实盘交易",
                "available": False,
                "icon": "💰",
                "description": "使用 QMT 进行实盘交易",
                "required": "配置 QMT 账号",
            },
        }
