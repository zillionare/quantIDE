import asyncio
import datetime
import time
from typing import Any

import pandas as pd
import polars as pl
import quantstats as qs
from loguru import logger

from pyqmt.core.enums import BrokerKind, OrderSide
from pyqmt.core.errors import TradeError, TradeErrors
from pyqmt.data.sqlite import db
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

    def _normalize_buy_shares(self, shares: float) -> float:
        """买入时，必须以100股为单位

        Args:
            shares: 建议买入量

        Returns:
            float: 实际允许买入量
        """
        s = (shares // 100) * 100
        if s <= 0:
            raise TradeError(
                TradeErrors.ERROR_BAD_PARAMS, f"无效的参数: shares不足一手:{shares}"
            )
        return s

    def _normalize_sell_shares(
        self, position_shares: float, shares: int | float
    ) -> float | int:
        """卖出时，如果是清仓，则不限制卖出数量；否则必须以100的整数倍为单位"""
        s = shares
        if s <= 0:
            raise TradeError(TradeErrors.ERROR_BAD_PARAMS, "无效的参数: shares <= 0")
        if s >= position_shares:  # 清仓卖出时，允许不足一手，允许小数
            s = position_shares
        else:
            s = (s // 100) * 100

        if s <= 0:
            raise TradeError(
                TradeErrors.ERROR_BAD_PARAMS,
                f"无效的参数: 可卖资金不足一手或为0: {position_shares}",
            )
        return s

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
