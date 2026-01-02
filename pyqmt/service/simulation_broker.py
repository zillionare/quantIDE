import asyncio
import datetime
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
    up_limit: float | None = None
    down_limit: float | None = None
    tm: datetime.datetime | None = None


class SimulationBroker(AbstractBroker):
    def __init__(self, cfg: Any):
        super().__init__()
        self._cfg = cfg
        self._init_account(principal=float(getattr(cfg, "principal", 1_000_000)))
        self._positions: dict[str, Position] = {}
        self._pending: list[str] = []
        self._quotes: dict[str, QuoteEvent] = {}
        self._task: asyncio.Task | None = None

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
        pending = list(self._pending)
        for qtoid in pending:
            order = db.get_order(qtoid)
            if order is None or order.asset != event.asset:
                continue
            px = float(event.last_price)
            try:
                self._check_limit_strict(order.side, px, event.up_limit, event.down_limit)
            except TradeError as e:
                db.update_order(order.qtoid, status=OrderStatus.JUNK, status_msg=str(e))
                self._pending.remove(qtoid)
                self.awake(qtoid, None)
                continue

            if order.side == OrderSide.BUY:
                shares = self._normalize_buy_shares(order.shares)
                shares = min(shares, self._max_affordable_buy_shares(px))
                if shares <= 0:
                    db.update_order(order.qtoid, status=OrderStatus.JUNK, status_msg="insufficient cash")
                    self._pending.remove(qtoid)
                    self.awake(qtoid, None)
                    continue
                if int(order.shares) != int(shares):
                    db.update_order(order.qtoid, shares=int(shares))
                amount = px * shares
                fee = self._calc_fee(amount)
                self._cash -= amount + fee
                prev = self._positions.get(order.asset)
                if prev is None:
                    pos = Position(
                        dt=(event.tm or datetime.datetime.now()).date(),
                        asset=order.asset,
                        shares=float(shares),
                        avail=0.0,
                        price=px,
                        profit=0.0,
                        mv=px * shares,
                    )
                else:
                    pos = Position(
                        dt=(event.tm or datetime.datetime.now()).date(),
                        asset=order.asset,
                        shares=float(prev.shares) + float(shares),
                        avail=float(prev.avail),
                        price=float(prev.price),
                        profit=0.0,
                        mv=(float(prev.shares) + float(shares)) * px,
                    )
                self._positions[order.asset] = pos
                db.upsert_positions(pos)
            else:
                pos = self._positions.get(order.asset)
                if pos is None or pos.avail <= 0:
                    db.update_order(order.qtoid, status=OrderStatus.JUNK, status_msg="no position")
                    self._pending.remove(qtoid)
                    self.awake(qtoid, None)
                    continue
                shares = self._normalize_sell_shares(pos.avail, order.shares)
                if int(order.shares) != int(shares):
                    db.update_order(order.qtoid, shares=int(shares))
                amount = px * shares
                fee = self._calc_fee(amount)
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
                        mv=remain * px,
                    )
                    db.upsert_positions(self._positions[order.asset])

            trade = Trade(
                tid=new_uuid_id(),
                qtoid=order.qtoid,
                foid=order.foid or order.qtoid,
                asset=order.asset,
                shares=float(shares),
                price=px,
                amount=px * float(shares),
                tm=event.tm or datetime.datetime.now(),
                side=order.side,
                cid=order.cid or "",
                fee=float(fee),
            )
            db.insert_trades(trade)
            db.update_order(order.qtoid, status=OrderStatus.SUCCEEDED)
            self._sync_asset(event.tm)
            self._pending.remove(qtoid)
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
        self._pending.append(qtoid)
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
        self._pending.append(qtoid)
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
