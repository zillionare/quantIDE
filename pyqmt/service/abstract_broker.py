import asyncio
import datetime
import threading
import time
from typing import Any

import pandas as pd
import polars as pl
import quantstats as qs
from loguru import logger

from pyqmt.core.enums import BrokerKind
from pyqmt.core.errors import InsufficientPosition, NonMultipleOfLotSize
from pyqmt.data.sqlite import Position, db
from pyqmt.service.base_broker import Broker


class AbstractBroker(Broker):
    """抽象 Broker 类。只实现超时控制功能，策略的 metrics 计算等功能"""

    def __init__(
        self,
        portfolio_id: str = "default",
        principal: float = 1_000_000,
        commission: float = 1e-4,
        kind: BrokerKind = BrokerKind.BACKTEST,
        portfolio_name: str = "",
        info: str = "",
        start: datetime.date | None = None,
        end: datetime.date | None = None,
    ):
        self._cash: float = principal
        self._portfolio_id: str = portfolio_id
        self._principal: float = principal
        self._commission: float = commission
        self._kind: BrokerKind = kind
        self._portfolio_name: str = portfolio_name
        self._info: str = info
        self._start: datetime.date | None = start
        self._end: datetime.date | None = end

        # 超时等待队列，用来实现带超时的交易
        self._pending_txs: dict[Any, Any] = {}
        # 缓存尚未被wait的早期结果
        self._early_results: dict[Any, Any] = {}
        self._lock = threading.RLock()

    def as_date(self, dt: datetime.date | datetime.datetime) -> datetime.date:
        """将 datetime 转换为 date"""
        if isinstance(dt, datetime.datetime):
            return dt.date()
        return dt

    @property
    def portfolio_name(self) -> str:
        """portfolio 对应的账户名称，是一个适合人类阅读的名字。一般应与 portfolio一一对应，不建议重复"""
        return self._portfolio_name

    @property
    def kind(self) -> BrokerKind:
        """broker 类型"""
        return self._kind

    @property
    def info(self) -> str:
        """账户、策略的说明信息"""
        return self._info

    @property
    def portfolio_id(self) -> str:
        return self._portfolio_id

    def _validate_sell_shares(
        self, pos: Position, shares: float
    ) ->None:
        """卖出时，如果是清仓，则不限制卖出数量；否则必须以100的整数倍为单位"""
        # 1. 基础检查：没有可用持仓
        if pos.avail == 0:
            raise InsufficientPosition(pos.asset, shares)

        # 2. 清仓判断：
        # 条件：请求卖出的数量接近可用持仓量，且可用持仓量接近总持仓量（即全仓可卖）
        # 允许微小误差
        is_clearance = (abs(pos.shares - pos.avail) < 1e-7) and (abs(shares - pos.avail) < 1e-7)

        if is_clearance:
            return

        # 3. 数量检查：非清仓情况下，卖出量不能超过可用量
        if shares > pos.avail:
             raise InsufficientPosition(pos.asset, shares)

        # 4. 整手检查
        if shares % 100 != 0 or shares == 0:
            raise NonMultipleOfLotSize(pos.asset, shares)

    async def wait(self, event_id: Any, timeout: float) -> tuple[Any, float]:
        """事件等待机制

        Args:
            event_id: 事件，全局唯一，通过它来获取绑定的 context
            timeout: 超时时间，单位秒。超时撮合不成功，返回空列表

        Returns:
            result: 事件结果。如果超时，则为 None
            remaining_time: 剩余时间，单位秒。如果超时，则为 0
        """
        with self._lock:
            # Check if result already arrived
            if event_id in self._early_results:
                return self._early_results.pop(event_id), 0.0

            if event_id in self._pending_txs:
                logger.warning("duplicate event_id: {}, overwritten.", event_id)

            _future = asyncio.get_running_loop().create_future()
            self._pending_txs[event_id] = _future

        try:
            t0 = time.perf_counter()
            result = await asyncio.wait_for(_future, timeout=timeout)
            return result, time.perf_counter() - t0
        except asyncio.TimeoutError:
            with self._lock:
                self._pending_txs.pop(event_id, None)
            return None, 0

    def awake(self, event_id: Any, result: Any) -> None:
        """事件触发机制。线程安全。

        Args:
            event_id: 事件，全局唯一，通过它来获取绑定的 context
            result: 事件结果
        """
        with self._lock:
            if event_id in self._pending_txs:
                future = self._pending_txs.pop(event_id)
                if not future.done():
                    loop = future.get_loop()
                    if not loop.is_closed():
                        loop.call_soon_threadsafe(future.set_result, result)
            else:
                # Store for later
                self._early_results[event_id] = result
