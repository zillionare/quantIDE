"""Broker 接口类。

本接口类定义了交易代理的基本功能接口。
"""

import datetime
from abc import ABCMeta, abstractmethod

import polars as pl

from pyqmt.core.enums import BidType, OrderSide
from pyqmt.data.sqlite import Trade


class Broker(metaclass=ABCMeta):
    def __init__(self):
        self._cash: float = 0
        self._principal: float = 0
        self._commission: float = 0
        self._account: str = ""

    @abstractmethod
    async def buy(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        bid_time: datetime.datetime | None = None,
        strategy: str = "",
        timeout: float = 0.5,
    ) -> pl.DataFrame | None:
        """买入指令

        如果传入价格为0, 则为市价买入。

        Args:
            asset: 资产代码, "symbol.SZ"风格
            shares: 委托数量
            price: 委托价格
            bit_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。如果超时未成交(含部成），返回空列表
        """
        ...

    @abstractmethod
    async def buy_percent(
        self,
        asset: str,
        percent: float,
        bid_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> pl.DataFrame|None:
        """买入指令按比例买入

        Args:
            asset: 资产代码, "symbol.SZ"风格
            percent: 买入比例，0-1之间的浮点数
            bid_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。如果超时未成交(含部成），返回空列表
        """
        ...

    @abstractmethod
    async def buy_amount(
        self,
        asset: str,
        amount: int | float,
        price: int | float | None = None,
        bid_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> list[Trade]:
        """买入指令按金额买入

        Args:
            asset: 资产代码, "symbol.SZ"风格
            amount: 买入金额
            price: 如果委托价格为 None，则以市价买入
            bid_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。如果超时未成交(含部成），返回空列表
        """
        ...

    @abstractmethod
    async def sell(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        bid_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> list[Trade]:
        """卖出指令

        如果传入价格为0, 则为市价卖出。

        Args:
            asset: 资产代码, "symbol.SZ"风格
            shares: 委托数量
            price: 委托价格
            bit_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交数据。如果超时未成交(含部成），返回空列表
        """
        ...

    @abstractmethod
    async def sell_percent(
        self,
        asset: str,
        percent: float,
        bid_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> list[Trade]:
        """卖出指令按比例卖出

        Args:
            asset: 资产代码, "symbol.SZ"风格
            percent: 卖出比例，0-1之间的浮点数
            bid_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。如果超时未成交(含部成），返回空列表
        """
        ...

    @abstractmethod
    async def sell_amount(
        self,
        asset: str,
        amount: int | float,
        price: int | float | None = None,
        bid_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> list[Trade]:
        """卖出指令按金额卖出

        因为取整（手）的关系，实际卖出金额将可能超过约定金额，以保证回笼足够的现金。

        Args:
            asset: 资产代码, "symbol.SZ"风格
            amount: 卖出金额
            price: 如果委托价格为 None，则以市价卖出
            bid_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。如果超时未成交(含部成），返回空列表
        """
        ...

    @abstractmethod
    def cancel_order(self, order_id: str):
        """取消订单，用于实盘

        取消指定订单。如果订单不存在或已成交，不做任何操作。

        Args:
            order_id: 订单 ID
        """
        ...

    @abstractmethod
    def cancel_all_orders(self, side: OrderSide | None = None):
        """取消所有订单，用于实盘

        取消所有未成交订单。如果所有订单已成交，不做任何操作。

        Args:
            side: 订单方向，默认为 None，取消所有订单
        """
        ...

    @abstractmethod
    def trade_target_pct(
        self,
        asset: str,
        price: float,
        target_pct: float,
        bid_type: BidType = BidType.MARKET,
    ) -> list[Trade]:
        """将`asset`的仓位调整到占比`target_pct`

        如果当前仓位大于 target_pct，则卖出；
        如果当前仓位小于 target_pct，则买入，直到现金用尽；在这种情况下，最终`asset`的仓位会小于约定的`target_pct`。

        !!! warning:
            受交易手数取整和手续费影响，最终仓位可能会小于等于约定仓位。

        Args:
            asset: 资产代码, "symbol.SZ"风格
            price: 委托价格
            target_pct: 目标仓位占比，0-1之间的浮点数
            bid_type: 委托类型，市价或限价
        """
        ...
