import traceback

from fasthtml.common import JSONResponse

from pyqmt.core.errors import BaseTradeError, WebErrors


class BrokerRegistryMiddleware:
    def __init__(self, app, registry):
        self.app = app
        self.registry = registry

    async def __call__(self, scope, receive, send):
        if scope["type"] in ("http", "websocket"):
            scope["registry"] = self.registry
        await self.app(scope, receive, send)


async def exception_handler(request, exc):
    """全局异常处理函数"""
    if isinstance(exc, BaseTradeError):
        # 业务自定义异常
        status_code = 400
        # 如果是 WebErrors 相关的，可以映射状态码
        error_info = {
            "code": exc.code.value,
            "message": exc.msg,
            "type": exc.__class__.__name__,
        }
    else:
        # 未知异常
        status_code = 500
        error_info = {
            "code": WebErrors.INTERNAL_SERVER_ERROR.value,
            "message": str(exc),
            "type": exc.__class__.__name__,
        }

    # 包含堆栈信息
    error_info["traceback"] = traceback.format_exc()

    return JSONResponse(error_info, status_code=status_code)
