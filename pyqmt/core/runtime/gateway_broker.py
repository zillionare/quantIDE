"""qmt-gateway 交易端口适配器."""

import datetime
from uuid import uuid4
from typing import Any, Dict

from pyqmt.core.enums import BrokerKind, OrderSide
from pyqmt.core.ports import (
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
from pyqmt.core.runtime.gateway_client import GatewayClient
from pyqmt.data.sqlite import Asset, Position, Trade
from pyqmt.service.base_broker import Broker, TradeResult


class GatewayBrokerWrapper(Broker):
    """将 GatewayBrokerAdapter 包装为旧版的 Broker 接口，以便 UI 使用。"""

    def __init__(self, adapter: "GatewayBrokerAdapter", portfolio_id: str = "gateway"):
        self._adapter = adapter
        self._portfolio_id = portfolio_id
        self._portfolio_name = "实盘网关"
        self._kind = BrokerKind.QMT

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
        return True

    @property
    def is_connected(self) -> bool:
        return True

    @property
    def asset(self) -> Asset:
        view = self._adapter.query_assets()
        if not view:
            return Asset(
                portfolio_id=self._portfolio_id,
                dt=datetime.date.today(),
                principal=0,
                cash=0,
                frozen_cash=0,
                market_value=0,
                total=0,
            )
        return Asset(
            portfolio_id=self._portfolio_id,
            dt=view.dt or datetime.date.today(),
            principal=view.principal,
            cash=view.cash,
            frozen_cash=view.frozen_cash,
            market_value=view.market_value,
            total=view.total,
        )

    @property
    def cash(self) -> float:
        return self.asset.cash

    @property
    def positions(self) -> Dict[str, Position]:
        """返回当前持仓."""
        views = self._adapter.query_positions()
        res = {}
        for v in views:
            res[v.asset] = Position(
                portfolio_id=self._portfolio_id,
                dt=datetime.date.today(),
                asset=v.asset,
                shares=v.shares,
                avail=v.avail,
                price=v.price,
                profit=0,
                mv=v.mv,
            )
        return res

    def record(
        self,
        key: str,
        value: float,
        dt: datetime.datetime | None = None,
        extra: dict | None = None,
    ) -> None:
        """记录策略运行数据.

        Gateway 兼容层当前仅用于 UI 交易与查询，不持久化策略指标。
        """

    async def buy(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> TradeResult:
        """按股数买入."""
        return await self._submit_legacy_order(
            asset=asset,
            side=OrderSide.BUY,
            value=shares,
            style="shares",
            price=price,
            order_time=order_time,
            timeout=timeout,
            extra=kwargs,
        )

    async def buy_percent(
        self,
        asset: str,
        percent: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> TradeResult:
        """按资金比例买入."""
        return await self._submit_legacy_order(
            asset=asset,
            side=OrderSide.BUY,
            value=percent,
            style="percent",
            price=price,
            order_time=order_time,
            timeout=timeout,
            extra=kwargs,
        )

    async def buy_amount(
        self,
        asset: str,
        amount: int | float,
        price: int | float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> TradeResult:
        """按金额买入."""
        return await self._submit_legacy_order(
            asset=asset,
            side=OrderSide.BUY,
            value=amount,
            style="amount",
            price=float(price),
            order_time=order_time,
            timeout=timeout,
            extra=kwargs,
        )

    async def sell(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> TradeResult:
        """按股数卖出."""
        return await self._submit_legacy_order(
            asset=asset,
            side=OrderSide.SELL,
            value=shares,
            style="shares",
            price=price,
            order_time=order_time,
            timeout=timeout,
            extra=kwargs,
        )

    async def sell_percent(
        self,
        asset: str,
        percent: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> TradeResult:
        """按持仓比例卖出."""
        return await self._submit_legacy_order(
            asset=asset,
            side=OrderSide.SELL,
            value=percent,
            style="percent",
            price=price,
            order_time=order_time,
            timeout=timeout,
            extra=kwargs,
        )

    async def sell_amount(
        self,
        asset: str,
        amount: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
        **kwargs,
    ) -> TradeResult:
        """按金额卖出."""
        return await self._submit_legacy_order(
            asset=asset,
            side=OrderSide.SELL,
            value=amount,
            style="amount",
            price=price,
            order_time=order_time,
            timeout=timeout,
            extra=kwargs,
        )

    async def cancel_order(self, qt_oid: str):
        """撤销指定订单."""
        await self._adapter.cancel(qt_oid)

    async def cancel_all_orders(self, side: OrderSide | None = None):
        """撤销全部未完成订单."""
        await self._adapter.cancel_all(side=side)

    async def trade_target_pct(
        self,
        asset: str,
        target_pct: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """将仓位调整到目标占比."""
        current_mv = 0.0
        for position in self.positions.values():
            if position.asset == asset:
                current_mv = float(position.mv)
                break
        total_asset = float(self.asset.total)
        if total_asset <= 0:
            return TradeResult.empty()
        target_mv = total_asset * target_pct
        side = OrderSide.BUY if target_mv >= current_mv else OrderSide.SELL
        return await self._submit_legacy_order(
            asset=asset,
            side=side,
            value=target_pct,
            style="target_pct",
            price=price,
            order_time=order_time,
            timeout=timeout,
            extra={},
        )

    async def _submit_legacy_order(
        self,
        asset: str,
        side: OrderSide,
        value: int | float,
        style: str,
        price: float,
        order_time: datetime.datetime | None,
        timeout: float,
        extra: dict[str, Any],
    ) -> TradeResult:
        """将旧版 Broker 调用委托到统一交易端口."""
        request = OrderRequest(
            asset=asset,
            side=side,
            value=float(value),
            style=style,
            price=price,
            order_time=order_time,
            timeout=timeout,
            extra=extra,
        )
        ack = await self._adapter.submit(request)
        if ack.order_id is None:
            return TradeResult.empty()
        trades = [
            Trade(
                self._portfolio_id,
                str(item.trade_id),
                str(item.order_id),
                "",
                str(item.asset),
                float(item.shares),
                float(item.price),
                float(item.amount),
                item.tm,
                OrderSide.BUY if side == OrderSide.BUY else OrderSide.SELL,
                "",
            )
            for item in (ack.trades or [])
        ]
        return TradeResult(str(ack.order_id), trades)


class GatewayBrokerAdapter(BrokerPort):
    """基于 qmt-gateway REST 的交易适配器."""

    def __init__(self, client: GatewayClient):
        """初始化适配器.

        Args:
            client: gateway 客户端。
        """
        self._client = client

    def record(
        self,
        key: str,
        value: float,
        dt: datetime.datetime | None = None,
        extra: dict | None = None,
    ) -> None:
        """记录策略运行数据.

        gateway 当前仅提供交易与查询能力，不在此端口持久化指标。
        """

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
        return await self._submit_execution(
            asset=asset,
            side=OrderSide.BUY,
            value=shares,
            style="shares",
            price=price,
            order_time=order_time,
            timeout=timeout,
            extra=kwargs,
        )

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
        return await self._submit_execution(
            asset=asset,
            side=OrderSide.BUY,
            value=percent,
            style="percent",
            price=price,
            order_time=order_time,
            timeout=timeout,
            extra=kwargs,
        )

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
        return await self._submit_execution(
            asset=asset,
            side=OrderSide.BUY,
            value=amount,
            style="amount",
            price=price,
            order_time=order_time,
            timeout=timeout,
            extra=kwargs,
        )

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
        return await self._submit_execution(
            asset=asset,
            side=OrderSide.SELL,
            value=shares,
            style="shares",
            price=price,
            order_time=order_time,
            timeout=timeout,
            extra=kwargs,
        )

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
        return await self._submit_execution(
            asset=asset,
            side=OrderSide.SELL,
            value=percent,
            style="percent",
            price=price,
            order_time=order_time,
            timeout=timeout,
            extra=kwargs,
        )

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
        return await self._submit_execution(
            asset=asset,
            side=OrderSide.SELL,
            value=amount,
            style="amount",
            price=price,
            order_time=order_time,
            timeout=timeout,
            extra=kwargs,
        )

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
        asset_view = self.query_assets()
        if asset_view is None or asset_view.total <= 0:
            return ExecutionResult.empty()
        current_mv = 0.0
        for position in self.query_positions():
            if position.asset == asset:
                current_mv = float(position.mv)
                break
        target_mv = float(asset_view.total) * target_pct
        side = OrderSide.BUY if target_mv >= current_mv else OrderSide.SELL
        return await self._submit_execution(
            asset=asset,
            side=side,
            value=target_pct,
            style="target_pct",
            price=price,
            order_time=order_time,
            timeout=timeout,
            extra=kwargs,
        )

    async def submit(self, request: OrderRequest) -> OrderAck:
        """提交订单."""
        shares = self._resolve_shares(request)
        qtoid = str(request.extra.get("qtoid") or uuid4())
        strategy_id = str(request.extra.get("strategy_id") or "")
        if shares <= 0:
            return OrderAck(order_id=None, status="rejected", message="invalid shares")
        if request.side == OrderSide.BUY:
            payload = {
                "symbol": request.asset,
                "price": request.price,
                "shares": int(shares),
                "strategy_id": strategy_id,
                "qtoid": qtoid,
            }
            result = self._client.post_form("/api/trade/buy", payload) or {}
        else:
            payload = {
                "symbol": request.asset,
                "price": request.price,
                "shares": int(shares),
                "strategy_id": strategy_id,
                "qtoid": qtoid,
            }
            result = self._client.post_form("/api/trade/sell", payload) or {}
        if result.get("success"):
            return OrderAck(
                order_id=str(result.get("qtoid") or qtoid),
                status="submitted",
                message="ok",
            )
        return OrderAck(
            order_id=None,
            status="rejected",
            message=str(result.get("error") or "gateway submit failed"),
        )

    async def cancel(self, order_id: str) -> CancelAck:
        """撤销订单."""
        result = self._client.post_form("/api/trade/cancel", {"qtoid": order_id}) or {}
        return CancelAck(
            success=bool(result.get("success", False)),
            message=str(result.get("error") or ""),
        )

    async def _submit_execution(
        self,
        asset: str,
        side: OrderSide,
        value: int | float,
        style: str,
        price: float,
        order_time: datetime.datetime | None,
        timeout: float,
        extra: dict[str, Any],
    ) -> ExecutionResult:
        """提交高阶交易请求并返回正式结果对象."""
        ack = await self.submit(
            OrderRequest(
                asset=asset,
                side=side,
                value=float(value),
                style=style,
                price=price,
                order_time=order_time,
                timeout=timeout,
                extra=extra,
            )
        )
        return ExecutionResult(
            order_id=ack.order_id,
            trades=list(ack.trades or []),
            status=ack.status,
            message=ack.message,
        )

    async def cancel_all(self, side: OrderSide | None = None) -> int:
        """撤销全部订单."""
        orders = self.query_orders()
        count = 0
        for order in orders:
            if side and not self._side_matches(order.side, side):
                continue
            if order.order_id:
                ack = await self.cancel(order.order_id)
                if ack.success:
                    count += 1
        return count

    def query_positions(self) -> list[PositionView]:
        """查询持仓."""
        rows = self._client.get_json("/api/trade/positions") or []
        result: list[PositionView] = []
        today = datetime.date.today()
        for row in rows:
            result.append(
                PositionView(
                    asset=str(row.get("symbol") or ""),
                    shares=float(row.get("shares") or 0),
                    avail=float(row.get("avail") or 0),
                    price=float(row.get("cost") or 0),
                    mv=float(row.get("market_value") or 0),
                    dt=today,
                )
            )
        return result

    def query_assets(self) -> AssetView | None:
        """查询资产."""
        row = self._client.get_json("/api/trade/asset") or {}
        if not row:
            return None
        today = datetime.date.today()
        return AssetView(
            cash=float(row.get("cash") or 0),
            total=float(row.get("total") or 0),
            market_value=float(row.get("market_value") or 0),
            frozen_cash=float(row.get("frozen_cash") or 0),
            principal=float(row.get("principal") or 0),
            dt=today,
        )

    def query_orders(self, status: str | None = None) -> list[OrderView]:
        """查询订单."""
        params = {"status": status} if status else None
        rows = self._client.get_json("/api/trade/orders", params=params) or []
        result: list[OrderView] = []
        for row in rows:
            result.append(
                OrderView(
                    order_id=str(row.get("qtoid") or ""),
                    asset=str(row.get("symbol") or ""),
                    side=str(row.get("side") or ""),
                    shares=float(row.get("shares") or 0),
                    price=float(row.get("price") or 0),
                    status=str(row.get("status") or ""),
                    tm=self._parse_time_text(str(row.get("time") or "")),
                    filled=float(row.get("filled") or 0),
                    error="",
                )
            )
        return result

    def query_trades(self, order_id: str | None = None) -> list[TradeView]:
        """查询成交."""
        rows = self._client.get_json("/api/trade/trades") or []
        result: list[TradeView] = []
        for idx, row in enumerate(rows):
            trade_id = str(row.get("tid") or f"gw-{idx}")
            result.append(
                TradeView(
                    trade_id=trade_id,
                    order_id=str(row.get("qtoid") or order_id or ""),
                    asset=str(row.get("symbol") or ""),
                    side=str(row.get("side") or ""),
                    shares=float(row.get("shares") or 0),
                    price=float(row.get("price") or 0),
                    amount=float(row.get("amount") or 0),
                    tm=self._parse_time_text(str(row.get("time") or "")),
                )
            )
        return result

    def _resolve_shares(self, request: OrderRequest) -> int:
        """将统一下单请求转换为股数."""
        if request.style == "shares":
            return int(request.value // 100 * 100)
        if request.price <= 0:
            return 0
        if request.style == "amount":
            return int((request.value / request.price) // 100 * 100)
        if request.style == "percent":
            asset = self.query_assets()
            if asset is None:
                return 0
            amount = asset.total * request.value
            return int((amount / request.price) // 100 * 100)
        if request.style == "target_pct":
            asset = self.query_assets()
            if asset is None:
                return 0
            target_value = asset.total * request.value
            current_value = 0.0
            for position in self.query_positions():
                if position.asset == request.asset:
                    current_value = position.shares * request.price
                    break
            delta = target_value - current_value
            if request.side == OrderSide.BUY:
                if delta <= 0:
                    return 0
                return int((delta / request.price) // 100 * 100)
            if delta >= 0:
                return 0
            return int(((-delta) / request.price) // 100 * 100)
        return 0

    def _side_matches(self, text: str, side: OrderSide) -> bool:
        """判断订单方向是否匹配."""
        raw = str(text).strip().lower()
        if side == OrderSide.BUY:
            return raw in {"buy", "1", "买入"}
        if side == OrderSide.SELL:
            return raw in {"sell", "-1", "卖出"}
        return False

    def _parse_time_text(self, text: str) -> datetime.datetime:
        """解析时间字符串."""
        if not text:
            return datetime.datetime.now()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%H:%M:%S"):
            try:
                parsed = datetime.datetime.strptime(text, fmt)
                if fmt == "%H:%M:%S":
                    today = datetime.date.today()
                    return datetime.datetime.combine(today, parsed.time())
                return parsed
            except ValueError:
                continue
        return datetime.datetime.now()
