"""qmt-gateway 交易端口适配器."""

import datetime

from pyqmt.core.enums import OrderSide
from pyqmt.core.ports import (
    AssetView,
    BrokerPort,
    CancelAck,
    OrderAck,
    OrderRequest,
    OrderView,
    PositionView,
    TradeView,
)
from pyqmt.core.runtime.gateway_client import GatewayClient


class GatewayBrokerAdapter(BrokerPort):
    """基于 qmt-gateway REST 的交易适配器."""

    def __init__(self, client: GatewayClient):
        """初始化适配器.

        Args:
            client: gateway 客户端。
        """
        self._client = client

    async def submit(self, request: OrderRequest) -> OrderAck:
        """提交订单."""
        shares = self._resolve_shares(request)
        if shares <= 0:
            return OrderAck(order_id=None, status="rejected", message="invalid shares")
        if request.side == OrderSide.BUY:
            payload = {
                "symbol": request.asset,
                "price": request.price,
                "shares": int(shares),
                "strategy_id": str(request.extra.get("strategy_id") or ""),
            }
            result = self._client.post_form("/api/trade/buy", payload) or {}
        else:
            payload = {
                "symbol": request.asset,
                "price": request.price,
                "shares": int(shares),
                "strategy_id": str(request.extra.get("strategy_id") or ""),
            }
            result = self._client.post_form("/api/trade/sell", payload) or {}
        if result.get("success"):
            return OrderAck(
                order_id=str(result.get("order_id") or ""),
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
        result = self._client.post_form("/api/trade/cancel", {"order_id": order_id}) or {}
        return CancelAck(
            success=bool(result.get("success", False)),
            message=str(result.get("error") or ""),
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
                    order_id=str(order_id or ""),
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
