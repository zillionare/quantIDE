"""行情端口抽象."""

from collections.abc import AsyncIterator
from typing import Protocol

from pyqmt.core.domain import MarketEvent, QuoteSnapshot


class MarketDataPort(Protocol):
    """行情端口."""

    def start(self) -> None:
        """启动行情服务."""
        ...

    def stop(self) -> None:
        """停止行情服务."""
        ...

    def subscribe(self, symbols: list[str]) -> None:
        """订阅行情."""
        ...

    def unsubscribe(self, symbols: list[str]) -> None:
        """取消订阅行情."""
        ...

    async def stream(self) -> AsyncIterator[MarketEvent]:
        """获取事件流."""
        ...

    def snapshot(self, symbols: list[str]) -> dict[str, QuoteSnapshot]:
        """获取行情快照."""
        ...
