"""交易端口抽象."""

import datetime
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from pyqmt.core.enums import BidType, OrderSide

OrderStyle = Literal["shares", "amount", "percent", "target_pct"]


@dataclass
class OrderRequest:
    """统一下单请求."""

    asset: str
    side: OrderSide
    value: float
    style: OrderStyle = "shares"
    price: float = 0.0
    bid_type: BidType = BidType.MARKET
    order_time: datetime.datetime | None = None
    timeout: float = 0.5
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class TradeView:
    """成交视图."""

    trade_id: str
    order_id: str
    asset: str
    side: str
    shares: float
    price: float
    amount: float
    tm: datetime.datetime


@dataclass
class PositionView:
    """持仓视图."""

    asset: str
    shares: float
    avail: float
    price: float
    mv: float
    dt: datetime.date


@dataclass
class AssetView:
    """资产视图."""

    cash: float
    total: float
    market_value: float
    frozen_cash: float
    principal: float
    dt: datetime.date


@dataclass
class OrderView:
    """订单视图."""

    order_id: str
    asset: str
    side: str
    shares: float
    price: float
    status: str
    tm: datetime.datetime
    filled: float = 0.0
    error: str = ""


@dataclass
class OrderAck:
    """下单响应."""

    order_id: str | None
    status: str
    trades: list[TradeView] = field(default_factory=list)
    message: str = ""


@dataclass
class CancelAck:
    """撤单响应."""

    success: bool
    message: str = ""


class BrokerPort(Protocol):
    """交易端口."""

    async def submit(self, request: OrderRequest) -> OrderAck:
        """提交订单."""
        ...

    async def cancel(self, order_id: str) -> CancelAck:
        """撤销订单."""
        ...

    async def cancel_all(self, side: OrderSide | None = None) -> int:
        """撤销全部订单."""
        ...

    def query_positions(self) -> list[PositionView]:
        """查询持仓."""
        ...

    def query_assets(self) -> AssetView | None:
        """查询资产."""
        ...

    def query_orders(self, status: str | None = None) -> list[OrderView]:
        """查询订单."""
        ...

    def query_trades(self, order_id: str | None = None) -> list[TradeView]:
        """查询成交."""
        ...
