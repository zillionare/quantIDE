"""LiveQuote 到行情端口的桥接适配器."""

import asyncio
import datetime
from collections.abc import AsyncIterator
from typing import Any

from quantide.core.domain import MarketEvent, QuoteSnapshot
from quantide.core.enums import Topics
from quantide.core.message import msg_hub
from quantide.core.ports import MarketDataPort


class LiveQuoteMarketDataAdapter(MarketDataPort):
    """将 LiveQuote 适配为 MarketDataPort."""

    def __init__(self, live_quote: Any):
        """初始化适配器.

        Args:
            live_quote: LiveQuote 实例。
        """
        self._live_quote = live_quote
        self._stream_queue: asyncio.Queue[MarketEvent] | None = None
        self._stream_loop: asyncio.AbstractEventLoop | None = None
        self._subscribed = set[str]()
        self._streaming = False

    def start(self) -> None:
        """启动行情服务."""
        self._live_quote.start()

    def stop(self) -> None:
        """停止行情服务."""
        self._streaming = False
        self._live_quote.stop()

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
        self._stream_queue = asyncio.Queue(maxsize=2000)
        self._stream_loop = asyncio.get_running_loop()
        self._streaming = True
        msg_hub.subscribe(Topics.QUOTES_ALL.value, self._on_quotes)
        try:
            while self._streaming:
                event = await self._stream_queue.get()
                yield event
        finally:
            self._streaming = False
            msg_hub.unsubscribe(Topics.QUOTES_ALL.value, self._on_quotes)
            self._stream_queue = None
            self._stream_loop = None

    def snapshot(self, symbols: list[str]) -> dict[str, QuoteSnapshot]:
        """获取行情快照."""
        result: dict[str, QuoteSnapshot] = {}
        for symbol in symbols:
            quote = self._live_quote.get_quote(symbol)
            if quote is None:
                continue
            ts_raw = quote.get("time")
            ts = None
            if isinstance(ts_raw, (int, float)) and ts_raw > 0:
                ts = datetime.datetime.fromtimestamp(ts_raw / 1000)
            result[symbol] = QuoteSnapshot(
                symbol=symbol,
                price=self._to_float_or_none(quote.get("price")),
                open=self._to_float_or_none(quote.get("open")),
                high=self._to_float_or_none(quote.get("high")),
                low=self._to_float_or_none(quote.get("low")),
                volume=self._to_float_or_none(quote.get("volume")),
                amount=self._to_float_or_none(quote.get("amount")),
                ts=ts,
            )
        return result

    def _on_quotes(self, payload: dict[str, dict[str, Any]]) -> None:
        """接收消息总线行情推送."""
        if self._stream_queue is None or self._stream_loop is None:
            return
        if not payload:
            return
        now = datetime.datetime.now()
        for symbol, quote in payload.items():
            if self._subscribed and symbol not in self._subscribed:
                continue
            event = MarketEvent(
                symbol=symbol,
                event_type="tick",
                ts=now,
                payload=dict(quote),
                source="live_quote",
            )
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
        """将任意值转换为浮点."""
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
