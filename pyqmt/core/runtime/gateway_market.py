"""qmt-gateway 行情端口适配器."""

import asyncio
import datetime
import json
import threading
from collections.abc import AsyncIterator
from typing import Any

from loguru import logger

from pyqmt.core.domain import MarketEvent, QuoteSnapshot
from pyqmt.core.enums import Topics
from pyqmt.core.message import msg_hub
from pyqmt.core.ports import MarketDataPort
from pyqmt.core.runtime.gateway_client import GatewayClient


class GatewayMarketDataAdapter(MarketDataPort):
    """基于 qmt-gateway WS 的行情适配器."""

    def __init__(self, client: GatewayClient):
        """初始化适配器.

        Args:
            client: gateway 客户端。
        """
        self._client = client
        self._quotes: dict[str, dict[str, Any]] = {}
        self._subscribed = set[str]()
        self._streaming = False
        self._stream_queue: asyncio.Queue[MarketEvent] | None = None
        self._stream_loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        """启动行情服务."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_ws_loop,
            name="GatewayQuoteWS",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        """停止行情服务."""
        self._streaming = False
        self._stop_event.set()

    def subscribe(self, symbols: list[str]) -> None:
        """登记关注标的."""
        for symbol in symbols:
            if symbol:
                self._subscribed.add(symbol)

    def unsubscribe(self, symbols: list[str]) -> None:
        """移除关注标的."""
        for symbol in symbols:
            self._subscribed.discard(symbol)

    async def stream(self) -> AsyncIterator[MarketEvent]:
        """获取行情事件流."""
        if self._stream_queue is not None:
            raise RuntimeError("stream already started")
        self._stream_queue = asyncio.Queue(maxsize=4000)
        self._stream_loop = asyncio.get_running_loop()
        self._streaming = True
        try:
            while self._streaming:
                event = await self._stream_queue.get()
                yield event
        finally:
            self._streaming = False
            self._stream_queue = None
            self._stream_loop = None

    def snapshot(self, symbols: list[str]) -> dict[str, QuoteSnapshot]:
        """获取行情快照."""
        result: dict[str, QuoteSnapshot] = {}
        for symbol in symbols:
            quote = self._quotes.get(symbol)
            if quote is None:
                continue
            result[symbol] = QuoteSnapshot(
                symbol=symbol,
                price=self._to_float_or_none(quote.get("lastPrice")),
                open=self._to_float_or_none(quote.get("open")),
                high=self._to_float_or_none(quote.get("high")),
                low=self._to_float_or_none(quote.get("low")),
                volume=self._to_float_or_none(quote.get("volume")),
                amount=self._to_float_or_none(quote.get("amount")),
                ts=datetime.datetime.now(),
            )
        return result

    def _run_ws_loop(self) -> None:
        """后台线程启动 WS 循环."""
        try:
            asyncio.run(self._ws_main())
        except Exception as exc:
            logger.error(f"gateway ws loop stopped: {exc}")

    async def _ws_main(self) -> None:
        """WS 接收主循环."""
        try:
            import websockets
        except ImportError as exc:
            logger.error(f"gateway ws requires websockets package: {exc}")
            return
        url = self._client.ws_url("/ws/quotes")
        while not self._stop_event.is_set():
            try:
                headers = self._build_ws_headers()
                async with websockets.connect(
                    url,
                    additional_headers=headers,
                    ping_interval=20,
                    ping_timeout=20,
                ) as ws:
                    while not self._stop_event.is_set():
                        text = await ws.recv()
                        payload = json.loads(text)
                        self._on_gateway_quote(payload)
            except Exception as exc:
                logger.error(f"gateway ws reconnect after error: {exc}")
                await asyncio.sleep(2)

    def _build_ws_headers(self) -> dict[str, str]:
        """构建 WS 连接头并确保登录态已就绪。"""
        self._client.ensure_login()
        cookie = self._client.cookie_header()
        if not cookie:
            return {}
        return {"Cookie": cookie}

    def _on_gateway_quote(self, payload: dict[str, Any]) -> None:
        """处理 gateway 推送数据."""
        symbol = str(payload.get("symbol") or "")
        if not symbol:
            return
        if self._subscribed and symbol not in self._subscribed:
            return
        bar = payload.get("1m") or {}
        quote = {
            "symbol": symbol,
            "lastPrice": self._to_float_or_none(bar.get("close")) or 0.0,
            "open": self._to_float_or_none(bar.get("open")) or 0.0,
            "high": self._to_float_or_none(bar.get("high")) or 0.0,
            "low": self._to_float_or_none(bar.get("low")) or 0.0,
            "volume": self._to_float_or_none(bar.get("volume")) or 0.0,
            "amount": self._to_float_or_none(bar.get("amount")) or 0.0,
            "time": payload.get("timestamp") or "",
        }
        self._quotes[symbol] = quote
        msg_hub.publish(Topics.QUOTES_ALL.value, {symbol: quote})
        event = MarketEvent(
            symbol=symbol,
            event_type="tick",
            ts=datetime.datetime.now(),
            payload=quote,
            source="gateway",
        )
        if self._stream_loop is not None:
            self._stream_loop.call_soon_threadsafe(self._put_event_safe, event)

    def _put_event_safe(self, event: MarketEvent) -> None:
        """将事件写入队列."""
        if self._stream_queue is None:
            return
        try:
            self._stream_queue.put_nowait(event)
        except asyncio.QueueFull:
            pass

    def _to_float_or_none(self, value: Any) -> float | None:
        """将值转浮点."""
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
