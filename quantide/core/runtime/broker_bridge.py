"""旧 Broker 到新端口的桥接适配器."""

import datetime
from typing import Any

from quantide.core.enums import OrderSide, OrderStatus
from quantide.core.ports import (
    AssetView,
    BrokerPort,
    CancelAck,
    ExecutionResult,
    OrderAck,
    OrderRequest,
    OrderView,
    PositionView,
    TradeView,
)
from quantide.data.sqlite import Position, Trade, db
from quantide.service.base_broker import Broker


class LegacyBrokerPortAdapter(BrokerPort):
    """将现有 Broker 适配为 BrokerPort."""

    def __init__(self, broker: Broker, portfolio_id: str | None = None):
        """初始化桥接器.

        Args:
            broker: 旧 Broker 实例。
            portfolio_id: 账户 ID，为空时尝试从 broker 获取。
        """
        self._broker = broker
        self._portfolio_id = portfolio_id or getattr(broker, "portfolio_id", "default")

    def record(
        self,
        key: str,
        value: float,
        dt: datetime.datetime | None = None,
        extra: dict | None = None,
    ) -> None:
        """记录策略运行数据."""
        self._broker.record(key, value, dt=dt, extra=extra)

    async def submit(self, request: OrderRequest) -> OrderAck:
        """提交订单."""
        try:
            result = await self._dispatch_submit(request)
            trades = [
                self._to_trade_view(item)
                for item in (result.trades or [])
                if item is not None
            ]
            return OrderAck(order_id=result.qt_oid, status="submitted", trades=trades)
        except Exception as exc:
            return OrderAck(order_id=None, status="rejected", message=str(exc))

    async def buy(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> ExecutionResult:
        """按股数买入."""
        result = await self._broker.buy(
            asset=asset,
            shares=shares,
            price=price,
            order_time=order_time,
            timeout=timeout,
            **kwargs,
        )
        return self._to_execution_result(result)

    async def buy_percent(
        self,
        asset: str,
        percent: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> ExecutionResult:
        """按比例买入."""
        result = await self._broker.buy_percent(
            asset=asset,
            percent=percent,
            price=price,
            order_time=order_time,
            timeout=timeout,
            **kwargs,
        )
        return self._to_execution_result(result)

    async def buy_amount(
        self,
        asset: str,
        amount: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> ExecutionResult:
        """按金额买入."""
        result = await self._broker.buy_amount(
            asset=asset,
            amount=amount,
            price=price,
            order_time=order_time,
            timeout=timeout,
            **kwargs,
        )
        return self._to_execution_result(result)

    async def sell(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> ExecutionResult:
        """按股数卖出."""
        result = await self._broker.sell(
            asset=asset,
            shares=shares,
            price=price,
            order_time=order_time,
            timeout=timeout,
            **kwargs,
        )
        return self._to_execution_result(result)

    async def sell_percent(
        self,
        asset: str,
        percent: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> ExecutionResult:
        """按比例卖出."""
        result = await self._broker.sell_percent(
            asset=asset,
            percent=percent,
            price=price,
            order_time=order_time,
            timeout=timeout,
            **kwargs,
        )
        return self._to_execution_result(result)

    async def sell_amount(
        self,
        asset: str,
        amount: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> ExecutionResult:
        """按金额卖出."""
        result = await self._broker.sell_amount(
            asset=asset,
            amount=amount,
            price=price,
            order_time=order_time,
            timeout=timeout,
            **kwargs,
        )
        return self._to_execution_result(result)

    async def trade_target_pct(
        self,
        asset: str,
        target_pct: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> ExecutionResult:
        """调整目标仓位占比."""
        result = await self._broker.trade_target_pct(
            asset=asset,
            target_pct=target_pct,
            price=price,
            order_time=order_time,
            timeout=timeout,
            **kwargs,
        )
        return self._to_execution_result(result)

    async def cancel(self, order_id: str) -> CancelAck:
        """撤销订单."""
        try:
            await self._broker.cancel_order(order_id)
            return CancelAck(success=True)
        except Exception as exc:
            return CancelAck(success=False, message=str(exc))

    async def cancel_all(self, side: OrderSide | None = None) -> int:
        """撤销全部订单."""
        await self._broker.cancel_all_orders(side=side)
        return 0

    def query_positions(self) -> list[PositionView]:
        """查询持仓."""
        raw = self._broker.positions
        if isinstance(raw, dict):
            values = list(raw.values())
        else:
            values = list(raw)
        result: list[PositionView] = []
        for pos in values:
            if isinstance(pos, Position):
                result.append(
                    PositionView(
                        asset=pos.asset,
                        shares=float(pos.shares),
                        avail=float(pos.avail),
                        price=float(pos.price),
                        mv=float(pos.mv),
                        dt=pos.dt,
                    )
                )
        return result

    def query_assets(self) -> AssetView | None:
        """查询资产."""
        asset = db.get_asset(portfolio_id=self._portfolio_id)
        if asset is None:
            return None
        return AssetView(
            cash=float(asset.cash),
            total=float(asset.total),
            market_value=float(asset.market_value),
            frozen_cash=float(asset.frozen_cash),
            principal=float(asset.principal),
            dt=asset.dt,
        )

    def query_orders(self, status: str | None = None) -> list[OrderView]:
        """查询订单."""
        df = db.get_orders(portfolio_id=self._portfolio_id)
        if df.is_empty():
            return []
        rows = df.to_dicts()
        if status:
            rows = [row for row in rows if self._status_matches(row.get("status"), status)]
        result: list[OrderView] = []
        for row in rows:
            result.append(
                OrderView(
                    order_id=str(row.get("qtoid") or ""),
                    asset=str(row.get("asset") or ""),
                    side=str(row.get("side") or ""),
                    shares=float(row.get("shares") or 0),
                    price=float(row.get("price") or 0),
                    status=str(row.get("status") or ""),
                    tm=self._to_datetime(row.get("tm")),
                    filled=float(row.get("filled") or 0),
                    error=str(row.get("error") or ""),
                )
            )
        return result

    def query_trades(self, order_id: str | None = None) -> list[TradeView]:
        """查询成交."""
        if order_id:
            df = db.query_trade(qtoid=order_id)
            if df is None or df.is_empty():
                return []
            rows = df.to_dicts()
        else:
            df = db.get_trades(portfolio_id=self._portfolio_id)
            if df.is_empty():
                return []
            rows = df.to_dicts()
        return [self._to_trade_view(row) for row in rows]

    async def _dispatch_submit(self, request: OrderRequest):
        """路由下单调用."""
        if request.style == "shares":
            if request.side == OrderSide.BUY:
                return await self._broker.buy(
                    asset=request.asset,
                    shares=request.value,
                    price=request.price,
                    order_time=request.order_time,
                    timeout=request.timeout,
                    **request.extra,
                )
            return await self._broker.sell(
                asset=request.asset,
                shares=request.value,
                price=request.price,
                order_time=request.order_time,
                timeout=request.timeout,
                **request.extra,
            )
        if request.style == "amount":
            if request.side == OrderSide.BUY:
                return await self._broker.buy_amount(
                    asset=request.asset,
                    amount=request.value,
                    price=request.price,
                    order_time=request.order_time,
                    timeout=request.timeout,
                    **request.extra,
                )
            return await self._broker.sell_amount(
                asset=request.asset,
                amount=request.value,
                price=request.price,
                order_time=request.order_time,
                timeout=request.timeout,
                **request.extra,
            )
        if request.style == "percent":
            if request.side == OrderSide.BUY:
                return await self._broker.buy_percent(
                    asset=request.asset,
                    percent=request.value,
                    price=request.price,
                    order_time=request.order_time,
                    timeout=request.timeout,
                    **request.extra,
                )
            return await self._broker.sell_percent(
                asset=request.asset,
                percent=request.value,
                price=request.price,
                order_time=request.order_time,
                timeout=request.timeout,
                **request.extra,
            )
        return await self._broker.trade_target_pct(
            asset=request.asset,
            target_pct=request.value,
            price=request.price,
            order_time=request.order_time,
            timeout=request.timeout,
        )

    def _status_matches(self, raw_status: Any, expected: str) -> bool:
        """判断状态是否匹配."""
        text = str(expected).strip().upper()
        if text.isdigit():
            return str(raw_status) == text
        if isinstance(raw_status, int):
            try:
                return OrderStatus(raw_status).name == text
            except ValueError:
                return False
        return str(raw_status).upper() == text

    def _to_trade_view(self, trade: Trade | dict[str, Any]) -> TradeView:
        """转换成交视图."""
        if isinstance(trade, Trade):
            return TradeView(
                trade_id=str(trade.tid),
                order_id=str(trade.qtoid),
                asset=trade.asset,
                side=str(trade.side),
                shares=float(trade.shares),
                price=float(trade.price),
                amount=float(trade.amount),
                tm=trade.tm,
            )
        return TradeView(
            trade_id=str(trade.get("tid") or ""),
            order_id=str(trade.get("qtoid") or ""),
            asset=str(trade.get("asset") or ""),
            side=str(trade.get("side") or ""),
            shares=float(trade.get("shares") or 0),
            price=float(trade.get("price") or 0),
            amount=float(trade.get("amount") or 0),
            tm=self._to_datetime(trade.get("tm")),
        )

    def _to_execution_result(self, result: Any) -> ExecutionResult:
        """将旧 TradeResult 转换为正式返回类型."""
        trades = [
            self._to_trade_view(item)
            for item in (getattr(result, "trades", None) or [])
            if item is not None
        ]
        return ExecutionResult(
            order_id=getattr(result, "qt_oid", None),
            trades=trades,
            status="submitted",
            message="",
        )

    def _to_datetime(self, value: Any) -> datetime.datetime:
        """转换为 datetime."""
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            return datetime.datetime.combine(value, datetime.time())
        if isinstance(value, str):
            return datetime.datetime.fromisoformat(value)
        return datetime.datetime.now()
