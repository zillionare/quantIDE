from abc import abstractmethod
from typing import Any
import asyncio
import datetime
import time
from dataclasses import asdict

from pyqmt.service.base_broker import Broker
from pyqmt.dal.tradedb import db
from pyqmt.core.enums import OrderSide, BidType
from pyqmt.models import OrderModel, TradeModel, PositionModel, AssetModel
from loguru import logger


class AbstractBroker(Broker):
    """抽象 Broker 类。"""

    def __init__(self):
        self._cash: float = 0
        self._asset: AssetModel | None = None
        self._principal: float = 0
        self._commission: float = 0
        self._account: str = ""

        # 超时等待队列，用来实现带超时的交易
        self._pending_txs: dict[Any, Any] = {}

    @property
    def asset(self) -> AssetModel:
        """账户资产信息"""
        assert self._asset is not None, "资产信息未初始化"
        return self._asset

    def on_sync_asset(
        self, total_asset: float, cash: float, frozen_cash: float, market_value: float
    ) -> None:
        """更新账户资产信息"""
        dt = datetime.date.today()
        self._asset = AssetModel(
            dt,
            principal=self._principal,
            cash=cash,
            frozen_cash=frozen_cash,
            market_value=market_value,
            total=total_asset,
        )

        db.update_asset(
            dt,
            cash=cash,
            frozen_cash=frozen_cash,
            market_value=market_value,
            total=total_asset,
        )

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
