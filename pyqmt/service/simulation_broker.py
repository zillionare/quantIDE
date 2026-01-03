import asyncio
import datetime
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import polars as pl

from pyqmt.core.enums import BidType, OrderSide, OrderStatus
from pyqmt.core.errors import TradeError, TradeErrors
from pyqmt.core.message import msg_hub
from pyqmt.data.sqlite import Order, Position, Trade, db, new_uuid_id
from pyqmt.service.abstract_broker import AbstractBroker


@dataclass(frozen=True)
class QuoteEvent:
    asset: str
    last_price: float
    volume: float | None = None
    low: float | None = None
    high: float | None = None
    up_limit: float | None = None
    down_limit: float | None = None
    tm: datetime.datetime | None = None


class SimulationBroker(AbstractBroker):
    def __init__(self, cfg: Any):
        super().__init__()
        self._cfg = cfg
        self._init_account(principal=float(getattr(cfg, "principal", 1_000_000)))
        self._positions: dict[str, Position] = {}
        self._pending: dict[str, list[str]] = defaultdict(list)
        self._quotes: dict[str, QuoteEvent] = {}
        self._task: asyncio.Task | None = None

    def _remove_pending(self, asset: str, qtoid: str) -> None:
        q = self._pending.get(asset)
        if not q:
            return
        try:
            q.remove(qtoid)
        except ValueError:
            return
        if len(q) == 0:
            self._pending.pop(asset, None)

    def _market_value(self) -> float:
        mv = 0.0
        for asset, pos in self._positions.items():
            q = self._quotes.get(asset)
            px = float(q.last_price) if q is not None else float(pos.price)
            mv += float(pos.shares) * px
        return float(mv)

    def _sync_asset(self, tm: datetime.datetime | None) -> None:
        dt = (tm or datetime.datetime.now()).date()
        mv = self._market_value()
        total = float(self._cash) + mv
        self.on_sync_asset(total_asset=total, cash=float(self._cash), frozen_cash=0.0, market_value=mv, dt=dt)

    def on_day_open(self, date: datetime.date) -> dict:
        prev = date - datetime.timedelta(days=1)
        # 刷新 T+1 可用量
        for asset, pos in list(self._positions.items()):
            self._positions[asset] = Position(
                dt=date,
                asset=asset,
                shares=float(pos.shares),
                avail=float(pos.shares),
                price=float(pos.price),
                profit=0.0,
                mv=float(pos.price) * float(pos.shares),
            )
        if self._positions:
            db.upsert_positions(list(self._positions.values()))
        # 结转上一交易日快照
        self._sync_asset(datetime.datetime.combine(prev, datetime.time(15, 0, 0)))
        return {"status": "ok", "date": date.isoformat()}

    def on_day_close(self, date: datetime.date) -> dict:
        # 生成当日快照
        self._sync_asset(datetime.datetime.combine(date, datetime.time(15, 0, 0)))
        # 将未完成订单统一废单
        for asset, q in list(self._pending.items()):
            for qtoid in list(q):
                order = db.get_order(qtoid)
                if order is None:
                    continue
                if order.status in (OrderStatus.SUCCEEDED, OrderStatus.PART_SUCC):
                    continue
                db.update_order(qtoid, status=OrderStatus.JUNK, status_msg="expired at close")
                self._remove_pending(asset, qtoid)
                self.awake(qtoid, None)
        return {"status": "ok", "date": date.isoformat()}

    def _ensure_task(self) -> None:
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            try:
                event = msg_hub.get("md:quote:1s", timeout=1)
            except Exception:
                await asyncio.sleep(0)
                continue
            if isinstance(event, dict):
                qe = QuoteEvent(
                    asset=str(event.get("asset")),
                    last_price=float(event.get("lastPrice") or event.get("last_price")),
                    volume=event.get("volume"),
                    low=event.get("low"),
                    high=event.get("high"),
                    up_limit=event.get("up_limit"),
                    down_limit=event.get("down_limit"),
                    tm=event.get("tm"),
                )
            else:
                qe = event
            if isinstance(qe, QuoteEvent):
                self.on_quote(qe)
            await asyncio.sleep(0)

    def on_quote(self, event: QuoteEvent) -> None:
        self._quotes[event.asset] = event
        pending = list(self._pending.get(event.asset, []))
        for qtoid in pending:
            order = db.get_order(qtoid)
            if order is None:
                self._remove_pending(event.asset, qtoid)
                self.awake(qtoid, None)
                continue
            if order.asset != event.asset:
                self._remove_pending(event.asset, qtoid)
                self._pending[order.asset].append(qtoid)
                continue
            if event.tm and order.tm and event.tm < order.tm:
                continue
            px = float(event.last_price)
            try:
                self._check_limit_strict(order.side, px, event.up_limit, event.down_limit)
                if order.bid_type == BidType.FIXED and order.price is not None:
                    price = float(order.price)
                    if abs(price - px) > 1e-8:
                        continue
                    px = price
            except TradeError as e:
                db.update_order(order.qtoid, status=OrderStatus.JUNK, status_msg=str(e))
                self._remove_pending(event.asset, qtoid)
                self.awake(qtoid, None)
                continue

            filled = int(float(getattr(order, "filled", 0.0) or 0.0))
            total = int(float(order.shares))
            remaining = max(0, total - filled)
            if remaining <= 0:
                db.update_order(order.qtoid, status=OrderStatus.SUCCEEDED)
                self._remove_pending(event.asset, qtoid)
                self.awake(qtoid, True)
                continue

            available = int(float(event.volume)) if event.volume is not None else remaining
            available = max(0, (available // 100) * 100)
            if available <= 0:
                continue

            shares = min(remaining, available)
            shares = (shares // 100) * 100
            if shares <= 0:
                continue

            amount = px * shares
            fee = self._calc_fee(amount)

            if order.side == OrderSide.BUY:
                cash_cap = self._max_affordable_buy_shares(px)
                shares = min(shares, int(cash_cap))
                shares = (shares // 100) * 100
                if shares <= 0:
                    continue
                self._cash -= amount + fee
                prev = self._positions.get(order.asset)
                if prev is None:
                    pos = Position(
                        dt=(event.tm or datetime.datetime.now()).date(),
                        asset=order.asset,
                        shares=float(shares),
                        avail=0.0,
                        price=float(px),
                        profit=0.0,
                        mv=float(px) * float(shares),
                    )
                else:
                    new_shares = float(prev.shares) + float(shares)
                    new_avg = (float(prev.price) * float(prev.shares) + float(amount)) / new_shares
                    pos = Position(
                        dt=(event.tm or datetime.datetime.now()).date(),
                        asset=order.asset,
                        shares=new_shares,
                        avail=float(prev.avail),
                        price=float(new_avg),
                        profit=0.0,
                        mv=new_shares * float(px),
                    )
                self._positions[order.asset] = pos
                db.upsert_positions(pos)
            else:
                pos = self._positions.get(order.asset)
                if pos is None or float(pos.avail) <= 0:
                    db.update_order(order.qtoid, status=OrderStatus.JUNK, status_msg="no position")
                    self._remove_pending(event.asset, qtoid)
                    self.awake(qtoid, None)
                    continue
                shares = min(shares, int(float(pos.avail)))
                shares = (shares // 100) * 100
                if shares <= 0:
                    continue
                self._cash += amount - fee
                remain = float(pos.shares) - float(shares)
                avail = float(pos.avail) - float(shares)
                if remain <= 0:
                    self._positions.pop(order.asset, None)
                else:
                    self._positions[order.asset] = Position(
                        dt=(event.tm or datetime.datetime.now()).date(),
                        asset=order.asset,
                        shares=remain,
                        avail=max(0.0, avail),
                        price=float(pos.price),
                        profit=0.0,
                        mv=remain * float(px),
                    )
                    db.upsert_positions(self._positions[order.asset])

            trade = Trade(
                tid=new_uuid_id(),
                qtoid=order.qtoid,
                foid=order.foid or order.qtoid,
                asset=order.asset,
                shares=float(shares),
                price=float(px),
                amount=float(amount),
                tm=event.tm or datetime.datetime.now(),
                side=order.side,
                cid=order.cid or "",
                fee=float(fee),
            )
            db.insert_trades(trade)
            new_filled = filled + int(shares)
            status = OrderStatus.SUCCEEDED if new_filled >= total else OrderStatus.PART_SUCC
            db.update_order(order.qtoid, filled=float(new_filled), status=status)
            self._sync_asset(event.tm)
            if status == OrderStatus.SUCCEEDED:
                self._remove_pending(event.asset, qtoid)
                self.awake(qtoid, True)

    async def buy(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        bid_time: datetime.datetime | None = None,
        strategy: str = "",
        timeout: float = 0.5,
    ) -> pl.DataFrame | None:
        _ = bid_time
        self._ensure_task()
        s = self._normalize_buy_shares(shares)
        bid_type = BidType.MARKET if price == 0 else BidType.FIXED
        order = Order(
            asset=asset,
            price=float(price),
            shares=s,
            side=OrderSide.BUY,
            bid_type=bid_type,
            tm=datetime.datetime.now(),
            strategy=strategy,
        )
        qtoid = db.insert_order(order)
        self._pending[asset].append(qtoid)
        done, _ = await self.wait(qtoid, timeout)
        if not done:
            return None
        return db.query_trade(qtoid=qtoid)

    async def buy_percent(
        self,
        asset: str,
        percent: float,
        bid_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> pl.DataFrame | None:
        raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "unsupported")

    async def buy_amount(
        self,
        asset: str,
        amount: int | float,
        price: int | float | None = None,
        bid_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> list[Trade]:
        raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "unsupported")

    async def sell(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        bid_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> list[Trade]:
        _ = bid_time
        self._ensure_task()
        pos = self._positions.get(asset)
        if pos is None:
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "no position")
        s = self._normalize_sell_shares(pos.avail, shares)
        bid_type = BidType.MARKET if price == 0 else BidType.FIXED
        order = Order(
            asset=asset,
            price=float(price),
            shares=s,
            side=OrderSide.SELL,
            bid_type=bid_type,
            tm=datetime.datetime.now(),
        )
        qtoid = db.insert_order(order)
        self._pending[asset].append(qtoid)
        done, _ = await self.wait(qtoid, timeout)
        if not done:
            return []
        trades = db.query_trade(qtoid=qtoid)
        if trades is None:
            return []
        return [Trade(**r) for r in trades.to_dicts()]

    async def sell_percent(
        self,
        asset: str,
        percent: float,
        bid_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> list[Trade]:
        raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "unsupported")

    async def sell_amount(
        self,
        asset: str,
        amount: int | float,
        price: int | float | None = None,
        bid_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> list[Trade]:
        raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "unsupported")

    def cancel_order(self, order_id: str):
        raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "unsupported")

    def cancel_all_orders(self, side: OrderSide | None = None):
        raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "unsupported")

    def trade_target_pct(
        self,
        asset: str,
        price: float,
        target_pct: float,
        bid_type: BidType = BidType.MARKET,
    ) -> list[Trade]:
        raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "unsupported")
