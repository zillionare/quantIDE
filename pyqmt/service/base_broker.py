"""Broker 接口类。

本接口类定义了交易代理的基本功能接口。
"""

import datetime
from abc import ABCMeta, abstractmethod

import polars as pl

from pyqmt.core.enums import BidType, OrderSide
from pyqmt.data.sqlite import Trade


class TradeResult:
    """成交结果类。

    成交结果将包含系统创建的订单 id，以便客户端查询。如果在 timeout 时间内成交（或者部成），则trades 属性将包含所有对应的成交记录。

    在 trade_target_pct 时，有可能不需要调仓，此时将返回 qt_oid 为 None。这不应该被当成错误。
    """

    def __init__(self, qt_oid: str|None, trades: list[Trade]|None = None):
        self.trades = trades
        self.qt_oid = qt_oid

    @classmethod
    def empty(cls) -> "TradeResult":
        """返回空的成交结果"""
        return cls(None, [])


class Broker(metaclass=ABCMeta):
    """交易代理接口类。

    本接口类定义了交易代理的基本功能接口。
    """

    @abstractmethod
    async def buy(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """买入指令

        如果传入价格为 0, 则为市价买入。

        Args:
            asset: 资产代码，"symbol.SZ"风格
            shares: 委托数量

            price: 委托价格
            bit_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。
        """

    @abstractmethod
    async def buy_percent(
        self,
        asset: str,
        percent: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """按当前持有的现金的比例买入

        实际执行的结果可能与计划略有出入，因为买入时需要按 100 股为单位取整。

        Args:
            asset: 资产代码，"symbol.SZ"风格
            percent: 买入比例，0-1 之间的浮点数
            price: 订单价格，默认为 0，表示市价
            order_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。如果超时未成交（含部成），返回空列表
        """
        ...

    @abstractmethod
    async def buy_amount(
        self,
        asset: str,
        amount: int | float,
        price: int | float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """买入指令按金额买入

        Args:
            asset: 资产代码，"symbol.SZ"风格
            amount: 买入金额
            price: 如果委托价格为 None，则以市价买入
            order_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。如果超时未成交（含部成），返回空列表
        """
        ...

    @abstractmethod
    async def sell(
        self,
        asset: str,
        shares: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """卖出指令

        如果传入价格为 0, 则为市价卖出。

        Args:
            asset: 资产代码，"symbol.SZ"风格
            shares: 委托数量
            price: 委托价格
            bit_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交数据。如果超时未成交（含部成），返回空列表
        """
        ...

    @abstractmethod
    async def sell_percent(
        self,
        asset: str,
        percent: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """卖出指令按比例卖出

        Args:
            asset: 资产代码，"symbol.SZ"风格
            percent: 卖出比例，0-1 之间的浮点数
            price: 委托价格
            order_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。如果超时未成交（含部成），返回空列表
        """
        ...

    @abstractmethod
    async def sell_amount(
        self,
        asset: str,
        amount: int | float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5,
    ) -> TradeResult:
        """卖出指令按金额卖出

        因为取整（手）的关系，实际卖出金额将可能超过约定金额，以保证回笼足够的现金。

        Args:
            asset: 资产代码，"symbol.SZ"风格
            amount: 卖出金额
            price: 如果委托价格为 0，则以市价卖出
            order_time: 下单时间，实盘时可省略传入，测试时必须传入
            timeout: 超时时间，单位秒。超时撮合不成功，返回 None

        Returns:
            成交结果。如果超时未成交（含部成），返回空列表
        """
        ...

    @abstractmethod
    async def cancel_order(self, qt_oid: str):
        """取消订单，用于实盘

        取消指定订单。如果订单不存在或已成交，不做任何操作。

        Args:
            qt_oid: Quantide 订单 ID，是一个 uuid4 惟一值
        """
        ...

    @abstractmethod
    async def cancel_all_orders(self, side: OrderSide | None = None):
        """取消所有订单，用于实盘

        取消所有未成交订单。如果所有订单已成交，不做任何操作。

        Args:
            side: 订单方向，默认为 None，取消所有订单
        """
        ...

    @abstractmethod
    async def trade_target_pct(
        self,
        asset: str,
        target_pct: float,
        price: float = 0,
        order_time: datetime.datetime | None = None,
        timeout: float = 0.5
    ) -> TradeResult:
        """将`asset`的仓位调整到总体市值占比的`target_pct`

        如果当前仓位与总市值之比大于 target_pct，则卖出；
        如果当前仓位与总市值之比小于 target_pct，则买入，直到现金用尽；在这种情况下，最终`asset`的仓位会小于约定的`target_pct`。

        调仓步骤：
        1. 计算当前总市值
        2. 计算目标仓位市值 = 总市值 * target_pct
        3. 对比当前持仓市值与目标持仓市值，计算需要买卖的数量，执行交易

        !!! warning:
            受交易手数取整和手续费影响，最终仓位可能与目标仓位不完全一致。

        Args:
            asset: 资产代码，"symbol.SZ"风格
            price: 委托价格
            target_pct: 目标仓位占比，0-1 之间的浮点数
            order_time: 下单时间，实盘时可省略传入，测试时必须传入
        """
        ...
