"""基于正式 BrokerPort 的兼容句柄。"""

import datetime
from typing import Any

from quantide.core.enums import BidType, BrokerKind, OrderSide, OrderStatus
from quantide.core.ports import BrokerPort
from quantide.data.sqlite import Asset, Order, Position


class PortBackedBroker:
    """将正式 BrokerPort 暴露为 UI/过渡层可消费的 broker 句柄。"""

    def __init__(
        self,
        port: BrokerPort,
        portfolio_id: str,
        kind: BrokerKind | str,
        portfolio_name: str = "",
        status: bool = True,
        is_connected: bool | None = None,
        legacy: Any | None = None,
    ):
        self._port = port
        self._portfolio_id = portfolio_id
        self._kind = kind if isinstance(kind, BrokerKind) else BrokerKind(kind)
        self._portfolio_name = portfolio_name or portfolio_id
        self._status = status
        self._is_connected = status if is_connected is None else is_connected
        self._legacy = legacy

    def __getattr__(self, name: str) -> Any:
        if self._legacy is not None:
            return getattr(self._legacy, name)
        raise AttributeError(name)

    @property
    def port(self) -> BrokerPort:
        """返回正式 broker port。"""
        return self._port

    @property
    def portfolio_id(self) -> str:
        return self._portfolio_id

    @property
    def portfolio_name(self) -> str:
        return self._portfolio_name

    @property
    def kind(self) -> BrokerKind:
        return self._kind

    @property
    def status(self) -> bool:
        return self._status

    @property
    def is_connected(self) -> bool:
        return self._is_connected

    @property
    def asset(self) -> Asset:
        view = self._port.query_assets()
        if view is not None:
            return Asset(
                portfolio_id=self._portfolio_id,
                dt=view.dt,
                principal=view.principal,
                cash=view.cash,
                frozen_cash=view.frozen_cash,
                market_value=view.market_value,
                total=view.total,
            )
        principal = float(getattr(self._legacy, "principal", 0.0) or 0.0)
        return Asset(
            portfolio_id=self._portfolio_id,
            dt=datetime.date.today(),
            principal=principal,
            cash=0.0,
            frozen_cash=0.0,
            market_value=0.0,
            total=principal,
        )

    @property
    def cash(self) -> float:
        return float(self.asset.cash)

    @property
    def principal(self) -> float:
        return float(self.asset.principal)

    @property
    def total_assets(self) -> float:
        return float(self.asset.total)

    @property
    def positions(self) -> dict[str, Position]:
        result: dict[str, Position] = {}
        for view in self._port.query_positions():
            result[view.asset] = Position(
                portfolio_id=self._portfolio_id,
                dt=view.dt,
                asset=view.asset,
                shares=view.shares,
                avail=view.avail,
                price=view.price,
                profit=0.0,
                mv=view.mv,
            )
        return result

    @property
    def orders(self) -> list[Order]:
        return [self._to_order(row) for row in self._port.query_orders()]

    def record(
        self,
        key: str,
        value: float,
        dt: datetime.datetime | None = None,
        extra: dict | None = None,
    ) -> None:
        self._port.record(key, value, dt=dt, extra=extra)

    async def buy(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ):
        return await self._port.buy(
            asset=asset,
            shares=shares,
            price=price,
            order_time=order_time,
            timeout=timeout,
            **kwargs,
        )

    async def buy_percent(self, asset: str, percent: float, price: float = 0, order_time: datetime.datetime | None = None, timeout: float = 0.5, **kwargs):
        return await self._port.buy_percent(asset=asset, percent=percent, price=price, order_time=order_time, timeout=timeout, **kwargs)

    async def buy_amount(self, asset: str, amount: int | float, price: float = 0, order_time: datetime.datetime | None = None, timeout: float = 0.5, **kwargs):
        return await self._port.buy_amount(asset=asset, amount=amount, price=price, order_time=order_time, timeout=timeout, **kwargs)

    async def sell(self, asset: str, shares: int | float, price: float = 0, order_time: datetime.datetime | None = None, timeout: float = 0.5, **kwargs):
        return await self._port.sell(asset=asset, shares=shares, price=price, order_time=order_time, timeout=timeout, **kwargs)

    async def sell_percent(self, asset: str, percent: float, price: float = 0, order_time: datetime.datetime | None = None, timeout: float = 0.5, **kwargs):
        return await self._port.sell_percent(asset=asset, percent=percent, price=price, order_time=order_time, timeout=timeout, **kwargs)

    async def sell_amount(self, asset: str, amount: int | float, price: float = 0, order_time: datetime.datetime | None = None, timeout: float = 0.5, **kwargs):
        return await self._port.sell_amount(asset=asset, amount=amount, price=price, order_time=order_time, timeout=timeout, **kwargs)

    async def trade_target_pct(self, asset: str, target_pct: float, price: float = 0, order_time: datetime.datetime | None = None, timeout: float = 0.5, **kwargs):
        return await self._port.trade_target_pct(asset=asset, target_pct=target_pct, price=price, order_time=order_time, timeout=timeout, **kwargs)

    async def cancel_order(self, qt_oid: str):
        return await self._port.cancel(qt_oid)

    async def cancel_all_orders(self, side: OrderSide | None = None):
        return await self._port.cancel_all(side=side)

    def get_position(self, asset: str, date: datetime.date | None = None) -> Position | None:
        _ = date
        return self.positions.get(asset)

    def _to_order(self, row: Any) -> Order:
        return Order(
            portfolio_id=self._portfolio_id,
            asset=str(row.asset),
            side=self._to_side(row.side),
            shares=float(row.shares),
            bid_type=BidType.UNKNOWN,
            tm=row.tm,
            price=float(row.price),
            filled=float(row.filled),
            qtoid=str(row.order_id),
            status=self._to_status(row.status),
            error=str(getattr(row, "error", "") or ""),
        )

    def _to_side(self, value: Any) -> OrderSide:
        if isinstance(value, OrderSide):
            return value
        if isinstance(value, int):
            try:
                return OrderSide(value)
            except ValueError:
                return OrderSide.UNKNOWN
        text = str(value).strip().lower()
        if text in {"buy", "1", "买入"}:
            return OrderSide.BUY
        if text in {"sell", "-1", "卖出"}:
            return OrderSide.SELL
        return OrderSide.UNKNOWN

    def _to_status(self, value: Any) -> OrderStatus:
        if isinstance(value, OrderStatus):
            return value
        if isinstance(value, int):
            try:
                return OrderStatus(value)
            except ValueError:
                return OrderStatus.UNKNOWN
        text = str(value).strip().upper()
        aliases = {
            "SUBMITTED": OrderStatus.REPORTED,
            "REJECTED": OrderStatus.JUNK,
            "CANCELED": OrderStatus.CANCELED,
            "CANCELLED": OrderStatus.CANCELED,
            "PARTIAL": OrderStatus.PART_SUCC,
            "FILLED": OrderStatus.SUCCEEDED,
            "SUCCEEDED": OrderStatus.SUCCEEDED,
            "REPORTED": OrderStatus.REPORTED,
        }
        return aliases.get(text, OrderStatus.UNKNOWN)