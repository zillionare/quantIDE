import asyncio
import datetime
from dataclasses import dataclass
from typing import Any

import numpy as np
import polars as pl

from pyqmt.core.enums import BidType, OrderSide, OrderStatus
from pyqmt.core.errors import TradeError, TradeErrors
from pyqmt.data.models.daily_bars import daily_bars
from pyqmt.data.sqlite import Asset, Order, Position, Trade, db, new_uuid_id
from pyqmt.service.abstract_broker import AbstractBroker


@dataclass(frozen=True)
class _PendingOrder:
    qtoid: str
    dt: datetime.date


class BacktestBroker(AbstractBroker):
    def __init__(self, cfg: Any):
        super().__init__()
        self._cfg = cfg

        self.account_name: str = "backtest"
        self.token: str = ""
        self.bt_start: datetime.date = datetime.date.today()
        self.bt_end: datetime.date = datetime.date.today()
        self._bt_stopped: bool = True

        self._task: asyncio.Task | None = None
        self._pending: list[_PendingOrder] = []
        self._positions: dict[str, Position] = {}
        self._assets: np.ndarray = np.array([], dtype=self._assets_dtype())
        self._last_day: datetime.date | None = None
        self._daily_match_price: str = "close"

    @staticmethod
    def _assets_dtype() -> np.dtype:
        return np.dtype(
            [
                ("date", "datetime64[D]"),
                ("principal", "f8"),
                ("cash", "f8"),
                ("frozen_cash", "f8"),
                ("market_value", "f8"),
                ("total", "f8"),
            ]
        )

    def start_backtest(self, params: dict | None = None) -> dict:
        params = params or {}
        principal = float(params.get("principal", 1_000_000))
        commission = float(params.get("commission", 0.001))
        name = str(params.get("name", "backtest"))
        token = str(params.get("token", ""))
        match_price = str(params.get("match_price", "close")).lower()
        if match_price not in ("open", "close"):
            match_price = "close"
        self._daily_match_price = match_price

        start_s = params.get("start")
        end_s = params.get("end")
        if start_s:
            self.bt_start = datetime.date.fromisoformat(str(start_s))
        else:
            self.bt_start = datetime.date.today()
        if end_s:
            self.bt_end = datetime.date.fromisoformat(str(end_s))
        else:
            self.bt_end = self.bt_start

        self.account_name = name
        self.token = token

        self._positions = {}
        self._pending = []
        self._assets = np.array([], dtype=self._assets_dtype())
        self._last_day = None

        self._init_account(principal=principal, commission_rate=commission, account=name)
        self._bt_stopped = False

        if self._task and not self._task.done():
            self._task.cancel()
        self._task = asyncio.create_task(self._run())

        return {
            "account_name": self.account_name,
            "token": self.token,
            "account_start_date": self.bt_start.isoformat(),
            "principal": principal,
        }

    def stop_backtest(self) -> dict:
        self._bt_stopped = True
        if self._task and not self._task.done():
            self._task.cancel()
        self._task = None
        return {"status": "stopped"}

    def list_accounts(self) -> list[dict]:
        return [{"name": self.account_name, "token": self.token}]

    def _get_bar(self, dt: datetime.date, asset: str) -> dict | None:
        df = daily_bars.get_bars_in_range(dt, dt, assets=[asset], adjust=None, eager_mode=True)
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        if df.height == 0:
            return None
        row = df.row(0, named=True)
        if isinstance(row["date"], datetime.datetime):
            row["date"] = row["date"].date()
        return row

    def _refresh_t1(self, dt: datetime.date) -> None:
        for asset, pos in list(self._positions.items()):
            self._positions[asset] = Position(
                dt=dt,
                asset=asset,
                shares=pos.shares,
                avail=pos.shares,
                price=pos.price,
                profit=pos.profit,
                mv=pos.mv,
            )

    def _update_asset_eod(self, dt: datetime.date) -> None:
        mv = 0.0
        for asset, pos in self._positions.items():
            bar = self._get_bar(dt, asset)
            if bar is None:
                continue
            close = float(bar["close"])
            mv += float(pos.shares) * close

        total = float(self._cash) + mv
        self.on_sync_asset(
            total_asset=total,
            cash=float(self._cash),
            frozen_cash=0.0,
            market_value=float(mv),
            dt=dt,
        )
        record = np.array(
            [
                (
                    np.datetime64(dt),
                    float(self._principal),
                    float(self._cash),
                    0.0,
                    float(mv),
                    float(total),
                )
            ],
            dtype=self._assets_dtype(),
        )
        self._assets = np.concatenate([self._assets, record]) if self._assets.size else record

    def _can_trade_on_day(self, dt: datetime.date) -> bool:
        return self.bt_start <= dt <= self.bt_end

    def _check_daily_limit_relaxed(self, side: OrderSide, bar: dict) -> None:
        up_limit = bar.get("up_limit")
        down_limit = bar.get("down_limit")
        if up_limit is None or down_limit is None:
            return
        if side == OrderSide.BUY and float(bar["low"]) >= float(up_limit):
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "涨停板不可买入")
        if side == OrderSide.SELL and float(bar["high"]) <= float(down_limit):
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "跌停板不可卖出")

    def _match_price(self, order: Order, bar: dict) -> float | None:
        if order.bid_type == BidType.MARKET or not order.price:
            return float(bar[self._daily_match_price])
        price = float(order.price)
        if float(bar["low"]) <= price <= float(bar["high"]):
            return price
        return None

    def _ensure_cash_for_buy(self, price: float, shares: int) -> int:
        amount = price * shares
        fee = self._calc_fee(amount)
        if self._cash >= amount + fee:
            return shares
        if price <= 0:
            return 0
        max_shares = int(self._cash / (price * (1.0 + float(self._commission))) // 100 * 100)
        return max(0, max_shares)

    def _apply_buy(self, dt: datetime.date, order: Order, price: float, shares: int) -> Trade:
        amount = float(price) * float(shares)
        fee = self._calc_fee(amount)
        self._cash -= amount + fee

        prev = self._positions.get(order.asset)
        if prev is None:
            new_pos = Position(
                dt=dt,
                asset=order.asset,
                shares=float(shares),
                avail=0.0,
                price=float(price),
                profit=0.0,
                mv=float(shares) * float(price),
            )
        else:
            new_shares = float(prev.shares) + float(shares)
            new_avg = (float(prev.price) * float(prev.shares) + amount) / new_shares
            new_pos = Position(
                dt=dt,
                asset=order.asset,
                shares=new_shares,
                avail=float(prev.avail),
                price=float(new_avg),
                profit=0.0,
                mv=new_shares * float(price),
            )
        self._positions[order.asset] = new_pos
        db.upsert_positions(new_pos)

        trade = Trade(
            tid=new_uuid_id(),
            qtoid=order.qtoid,
            foid=order.foid or order.qtoid,
            asset=order.asset,
            shares=shares,
            price=float(price),
            amount=float(amount),
            tm=datetime.datetime.combine(dt, datetime.time(9, 30, 0))
            if self._daily_match_price == "open"
            else datetime.datetime.combine(dt, datetime.time(15, 0, 0)),
            side=OrderSide.BUY,
            cid=order.cid or "",
            fee=float(fee),
        )
        db.insert_trades(trade)
        db.update_order(order.qtoid, status=OrderStatus.SUCCEEDED)
        return trade

    def _apply_sell(self, dt: datetime.date, order: Order, price: float, shares: int) -> Trade:
        amount = float(price) * float(shares)
        fee = self._calc_fee(amount)
        self._cash += amount - fee

        prev = self._positions.get(order.asset)
        assert prev is not None
        new_shares = float(prev.shares) - float(shares)
        new_avail = float(prev.avail) - float(shares)
        if new_shares <= 0:
            self._positions.pop(order.asset, None)
            db.upsert_positions(
                Position(
                    dt=dt,
                    asset=order.asset,
                    shares=0.0,
                    avail=0.0,
                    price=float(prev.price),
                    profit=0.0,
                    mv=0.0,
                )
            )
        else:
            self._positions[order.asset] = Position(
                dt=dt,
                asset=order.asset,
                shares=new_shares,
                avail=max(0.0, new_avail),
                price=float(prev.price),
                profit=0.0,
                mv=new_shares * float(price),
            )
            db.upsert_positions(self._positions[order.asset])

        trade = Trade(
            tid=new_uuid_id(),
            qtoid=order.qtoid,
            foid=order.foid or order.qtoid,
            asset=order.asset,
            shares=shares,
            price=float(price),
            amount=float(amount),
            tm=datetime.datetime.combine(dt, datetime.time(9, 30, 0))
            if self._daily_match_price == "open"
            else datetime.datetime.combine(dt, datetime.time(15, 0, 0)),
            side=OrderSide.SELL,
            cid=order.cid or "",
            fee=float(fee),
        )
        db.insert_trades(trade)
        db.update_order(order.qtoid, status=OrderStatus.SUCCEEDED)
        return trade

    async def _run(self) -> None:
        dt = self.bt_start
        while not self._bt_stopped and dt <= self.bt_end:
            if self._last_day is None or dt != self._last_day:
                self._refresh_t1(dt)
                self._last_day = dt

            pending_today = [p for p in self._pending if p.dt <= dt]
            rest = [p for p in self._pending if p.dt > dt]
            self._pending = rest

            for item in pending_today:
                order = db.get_order(item.qtoid)
                if order is None:
                    self.awake(item.qtoid, None)
                    continue
                if not self._can_trade_on_day(dt):
                    db.update_order(order.qtoid, status=OrderStatus.JUNK, status_msg="out of range")
                    self.awake(order.qtoid, None)
                    continue

                bar = self._get_bar(dt, order.asset)
                if bar is None:
                    db.update_order(order.qtoid, status=OrderStatus.JUNK, status_msg="no bar")
                    self.awake(order.qtoid, None)
                    continue

                try:
                    self._check_daily_limit_relaxed(order.side, bar)
                    px = self._match_price(order, bar)
                    if px is None:
                        self._pending.append(_PendingOrder(qtoid=order.qtoid, dt=dt + datetime.timedelta(days=1)))
                        continue

                    if order.side == OrderSide.BUY:
                        shares = self._normalize_buy_shares(order.shares)
                        shares = self._ensure_cash_for_buy(px, shares)
                        if shares <= 0:
                            db.update_order(order.qtoid, status=OrderStatus.JUNK, status_msg="insufficient cash")
                            self.awake(order.qtoid, None)
                            continue
                        if shares != int(order.shares):
                            db.update_order(order.qtoid, shares=shares)
                        self._apply_buy(dt, order, px, shares)
                        self.awake(order.qtoid, True)
                    else:
                        pos = self._positions.get(order.asset)
                        if pos is None or pos.avail <= 0:
                            db.update_order(order.qtoid, status=OrderStatus.JUNK, status_msg="no position")
                            self.awake(order.qtoid, None)
                            continue
                        shares = self._normalize_sell_shares(pos.avail, order.shares)
                        shares = min(int(pos.avail), shares)
                        if shares <= 0:
                            db.update_order(order.qtoid, status=OrderStatus.JUNK, status_msg="not available")
                            self.awake(order.qtoid, None)
                            continue
                        if shares != int(order.shares):
                            db.update_order(order.qtoid, shares=shares)
                        self._apply_sell(dt, order, px, shares)
                        self.awake(order.qtoid, True)
                except TradeError as e:
                    db.update_order(order.qtoid, status=OrderStatus.JUNK, status_msg=str(e))
                    self.awake(order.qtoid, None)

            self._update_asset_eod(dt)
            dt = dt + datetime.timedelta(days=1)
            await asyncio.sleep(0)

        self._bt_stopped = True

    async def buy(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        bid_time: datetime.datetime | None = None,
        strategy: str = "",
        timeout: float = 0.5,
    ) -> pl.DataFrame | None:
        if bid_time is None:
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "bid_time must be provided")
        if self._bt_stopped:
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "backtest not started")

        s = self._normalize_buy_shares(shares)
        bid_type = BidType.MARKET if price == 0 else BidType.FIXED
        order = Order(
            asset=asset,
            price=float(price),
            shares=s,
            side=OrderSide.BUY,
            bid_type=bid_type,
            tm=bid_time,
            strategy=strategy,
        )
        qtoid = db.insert_order(order)
        self._pending.append(_PendingOrder(qtoid=qtoid, dt=bid_time.date()))

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
        if bid_time is None:
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "bid_time must be provided")
        if self._bt_stopped:
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "backtest not started")

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
            tm=bid_time,
        )
        qtoid = db.insert_order(order)
        self._pending.append(_PendingOrder(qtoid=qtoid, dt=bid_time.date()))

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

    def get_position(self, asset: str, date: datetime.date | None = None) -> dict:
        pos = self._positions.get(asset)
        if pos is None:
            return {"asset": asset, "shares": 0.0, "avail": 0.0}
        return {"asset": asset, "shares": float(pos.shares), "avail": float(pos.avail)}

    def get_account_info(self, asset: str | None = None, date: datetime.date | None = None) -> bytes:
        dt = date or (self._last_day or self.bt_start)
        a = db.get_asset(dt) or self.asset
        positions = [
            (k, float(p.shares), float(p.avail), float(p.price), float(p.mv))
            for k, p in self._positions.items()
        ]
        result = {
            "name": self.account_name,
            "principal": float(self._principal),
            "assets": float(a.total),
            "start": self.bt_start,
            "last_trade": dt,
            "end": self.bt_end,
            "available": float(a.cash),
            "market_value": float(a.market_value),
            "positions": positions,
        }
        return pl.DataFrame([result]).to_pandas().to_json().encode("utf-8")

    async def metrics(
        self, start: datetime.date | None = None, end: datetime.date | None = None, baseline: str | None = None
    ) -> dict:
        _ = baseline
        start = start or self.bt_start
        end = end or self.bt_end
        assets = db.assets_all()
        if assets is None:
            return {}
        df = assets.filter((pl.col("dt") >= start) & (pl.col("dt") <= end))
        if df.height == 0:
            return {}
        total0 = float(df["total"][0])
        totaln = float(df["total"][-1])
        return {"start": start, "end": end, "return": (totaln - total0) / total0 if total0 else 0.0}

    def bills(self) -> dict:
        return {
            "trades": None if db.trades_all() is None else db.trades_all().to_dicts(),
            "positions": [
                {"asset": a, "shares": float(p.shares), "avail": float(p.avail)}
                for a, p in self._positions.items()
            ],
            "assets": self._assets.tolist(),
        }
