"""运行时事件领域模型."""

import datetime
from dataclasses import dataclass, field
from typing import Any, Literal

EventType = Literal["tick", "bar", "order", "trade", "status", "error"]


@dataclass
class MarketEvent:
    """行情事件."""

    symbol: str
    event_type: EventType
    ts: datetime.datetime
    payload: dict[str, Any]
    source: str = "unknown"
    event_id: str = ""


@dataclass
class QuoteSnapshot:
    """行情快照."""

    symbol: str
    price: float | None
    open: float | None = None
    high: float | None = None
    low: float | None = None
    volume: float | None = None
    amount: float | None = None
    ts: datetime.datetime | None = None


@dataclass
class OrderEvent:
    """订单事件."""

    order_id: str
    portfolio_id: str
    status: str
    ts: datetime.datetime
    filled_qty: float = 0.0
    filled_price: float = 0.0
    reason: str = ""


@dataclass
class TradeEvent:
    """成交事件."""

    trade_id: str
    order_id: str
    portfolio_id: str
    symbol: str
    side: str
    qty: float
    price: float
    amount: float
    ts: datetime.datetime


@dataclass
class ErrorEvent:
    """错误事件."""

    code: str
    category: str
    message: str
    retryable: bool = False
    details: dict[str, Any] = field(default_factory=dict)
