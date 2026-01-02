import asyncio
import datetime
import time
from typing import Any

from loguru import logger

from pyqmt.core.enums import OrderSide
from pyqmt.core.errors import TradeError, TradeErrors
from pyqmt.data.sqlite import Asset, db
from pyqmt.service.base_broker import Broker


class AbstractBroker(Broker):
    """抽象 Broker 类。"""

    def __init__(self):
        super().__init__()
        self._asset: Asset | None = None

        # 超时等待队列，用来实现带超时的交易
        self._pending_txs: dict[Any, Any] = {}

    @property
    def asset(self) -> Asset:
        """账户资产信息"""
        assert self._asset is not None, "资产信息未初始化"
        return self._asset

    def on_sync_asset(
        self,
        total_asset: float,
        cash: float,
        frozen_cash: float,
        market_value: float,
        dt: datetime.date | None = None,
    ) -> None:
        """更新账户资产信息"""
        dt = dt or datetime.date.today()
        self._asset = Asset(
            dt=dt,
            principal=self._principal,
            cash=cash,
            frozen_cash=frozen_cash,
            market_value=market_value,
            total=total_asset,
        )

        current = db.get_asset(dt)
        if current is None:
            db.insert_asset(self._asset)
        else:
            db.update_asset(
                dt,
                cash=cash,
                frozen_cash=frozen_cash,
                market_value=market_value,
                total=total_asset,
            )

    def _init_account(
        self,
        principal: float,
        commission_rate: float = 0.001,
        account: str = "",
        cash: float | None = None,
    ) -> None:
        self._principal = principal
        self._commission = commission_rate
        self._account = account
        if cash is None:
            cash = principal
        self._cash = float(cash)
        self.on_sync_asset(
            total_asset=self._cash, cash=self._cash, frozen_cash=0, market_value=0
        )

    def _calc_fee(self, amount: float) -> float:
        if amount <= 0:
            return 0.0
        return float(amount) * float(self._commission)

    def _max_affordable_buy_shares(self, price: float) -> int:
        if price <= 0:
            return 0
        max_shares = int(self._cash / (float(price) * (1.0 + float(self._commission))))
        return max(0, (max_shares // 100) * 100)

    def _check_limit_strict(
        self,
        side: OrderSide,
        price: float,
        up_limit: float | None,
        down_limit: float | None,
    ) -> None:
        if side == OrderSide.BUY and up_limit is not None and float(price) >= float(up_limit):
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "涨停板不可买入")
        if side == OrderSide.SELL and down_limit is not None and float(price) <= float(down_limit):
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "跌停板不可卖出")

    def _normalize_buy_shares(self, shares: int | float) -> int:
        s = int(shares)
        s = (s // 100) * 100
        if s <= 0:
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "无效的参数: shares <= 0")
        return s

    def _normalize_sell_shares(self, position_shares: float, shares: int | float) -> int:
        s = int(shares)
        if s <= 0:
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "无效的参数: shares <= 0")
        if s >= int(position_shares):
            return int(position_shares)
        return (s // 100) * 100

    async def wait(self, event_id: Any, timeout: float) -> tuple[Any, float]:
        """事件等待机制

        Args:
            event_id: 事件，全局唯一，通过它来获取绑定的 context
            timeout: 超时时间，单位秒。超时撮合不成功，返回空列表

        Returns:
            result: 事件结果。如果超时，则为 None
            remaining_time: 剩余时间，单位秒。如果超时，则为 0
        """
        if event_id in self._pending_txs:
            logger.warning("duplicate event_id: {}, overwritten.", event_id)

        _future = asyncio.Future()
        self._pending_txs[event_id] = _future

        try:
            t0 = time.perf_counter()
            result = await asyncio.wait_for(_future, timeout=timeout)
            return result, time.perf_counter() - t0
        except asyncio.TimeoutError:
            return None, 0

    def awake(self, event_id: Any, result: Any) -> None:
        """事件触发机制

        Args:
            event_id: 事件，全局唯一，通过它来获取绑定的 context
            result: 事件结果
        """
        if event_id in self._pending_txs:
            _future = self._pending_txs.pop(event_id)
            _future.set_result(result)
        else:
            logger.warning("event_id not found: {}", event_id)
            raise ValueError(f"event_id not found: {event_id}")
