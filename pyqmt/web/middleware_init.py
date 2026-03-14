"""初始化检查中间件

在请求处理前检查应用是否已完成初始化，如果未完成则重定向到初始化向导。
"""

from fasthtml.common import RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware

from pyqmt.service.init_wizard import init_wizard


class InitCheckMiddleware(BaseHTTPMiddleware):
    """初始化状态检查中间件

    检查应用是否已完成初始化，如果未完成则重定向到初始化向导页面。
    以下路径不受此中间件限制：
    - /init-wizard/* 初始化向导相关页面
    - /static/* 静态资源
    - /api/health 健康检查接口
    """

    # 允许访问的路径（无需初始化）
    ALLOWED_PATHS = [
        "/init-wizard",
        "/static",
        "/api/health",
        "/login",
        "/logout",
    ]

    async def dispatch(self, request, call_next):
        """处理请求"""
        path = request.url.path

        # 检查是否是允许访问的路径
        if any(path.startswith(allowed) for allowed in self.ALLOWED_PATHS):
            response = await call_next(request)
            return response

        # 检查是否已完成初始化
        should_redirect = False
        try:
            should_redirect = not init_wizard.is_initialized()
        except Exception as e:
            # 如果检查失败（如数据库未连接），记录日志并允许继续访问
            # 初始化向导页面会处理这些问题
            import logging
            logging.getLogger(__name__).warning(f"初始化状态检查失败: {e}, 允许继续访问")

        if should_redirect:
            # 未初始化，重定向到初始化向导
            if request.method == "GET":
                return RedirectResponse("/init-wizard", status_code=302)
            else:
                # API 请求返回错误信息
                from fasthtml.common import JSONResponse

                return JSONResponse(
                    {"error": "应用未初始化，请先完成初始化向导"},
                    status_code=503,
                )

        response = await call_next(request)
        return response


def check_init_redirect():
    """检查初始化状态的快捷函数

    用于在路由处理函数中手动检查初始化状态。

    Returns:
        RedirectResponse | None: 如果未初始化返回重定向响应，否则返回 None
    """
    try:
        if not init_wizard.is_initialized():
            return RedirectResponse("/init-wizard", status_code=302)
    except Exception:
        pass
    return None
