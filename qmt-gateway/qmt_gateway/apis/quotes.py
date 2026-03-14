"""行情 WebSocket API

提供实时行情数据推送。
"""

import asyncio
import json
from typing import Any

from fasthtml.common import *
from loguru import logger

from qmt_gateway.services.quote_service import quote_service


class QuoteWebSocket:
    """行情 WebSocket 处理器"""

    def __init__(self):
        self._clients: list = []
        self._started = False

    async def handle(self, ws):
        """处理 WebSocket 连接"""
        self._clients.append(ws)
        logger.info(f"WebSocket 客户端连接: {len(self._clients)} 个客户端")

        try:
            while True:
                # 接收消息（心跳或订阅请求）
                msg = await ws.receive_text()
                data = json.loads(msg)

                action = data.get("action")
                if action == "subscribe":
                    symbols = data.get("symbols", [])
                    logger.info(f"订阅行情: {symbols}")
                    # 这里可以添加订阅逻辑
                elif action == "ping":
                    await ws.send_text(json.dumps({"action": "pong"}))

        except Exception as e:
            logger.error(f"WebSocket 错误: {e}")
        finally:
            self._clients.remove(ws)
            logger.info(f"WebSocket 客户端断开: {len(self._clients)} 个客户端")

    def broadcast(self, data: dict):
        """广播行情数据到所有客户端"""
        if not self._clients:
            return

        msg = json.dumps(data)
        for ws in self._clients:
            try:
                asyncio.create_task(ws.send_text(msg))
            except Exception as e:
                logger.error(f"发送行情数据失败: {e}")

    def start(self):
        """启动行情推送"""
        if self._started:
            return

        # 订阅行情服务
        quote_service.subscribe(self._on_quote)
        quote_service.start()

        self._started = True
        logger.info("行情 WebSocket 服务已启动")

    def stop(self):
        """停止行情推送"""
        if not self._started:
            return

        quote_service.stop()
        self._started = False
        logger.info("行情 WebSocket 服务已停止")

    def _on_quote(self, data: dict):
        """行情数据回调"""
        self.broadcast(data)


# 全局 WebSocket 处理器
quote_ws = QuoteWebSocket()


def register_routes(app):
    """注册行情路由"""

    @app.ws("/ws/quotes")
    async def ws_quotes(ws):
        await quote_ws.handle(ws)

    @app.get("/api/v1/quotes/status")
    def get_quote_status():
        """获取行情服务状态"""
        return {
            "running": quote_service.is_running(),
            "clients": len(quote_ws._clients),
        }
