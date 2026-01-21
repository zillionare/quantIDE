import datetime
from abc import ABC, abstractmethod

import polars as pl


class DataFeed(ABC):
    """用以回测、模拟盘获取用以撮合的价格数据。

    实现类需要根据情况，选择实现其中的接口。比如, simulation broker 就不需要实现get_bars 接口
    """





    @abstractmethod
    def get_trade_price_limits(
        self, asset: str, dt: datetime.date
    ) -> tuple[float, float]:
        """获取指定资产在指定日期的涨跌停限价。

        Args:
            asset: 资产代码
            dt: 日期

        Returns:
            tuple[float, float]: (跌停价, 涨停价)
        """
        ...

    @abstractmethod
    def get_price_for_match(
        self, asset: str, tm: datetime.datetime
    ) -> pl.DataFrame | None:
        """获取用于撮合的行情数据。

        对于日线，返回当日行情数据。
        对于分钟线，返回当前时间到收盘的所有分钟线数据。

        返回的 DataFrame, 无论是日线还是分钟线，都包含 open, close, high, low, volume, up_limit, down_limit 这几个字段。如果缺少其中之一，则返回 None

        Args:
            asset: 资产代码
            tm: 开始时间（通常为报单时间）

        Returns:
            pl.DataFrame: 包含从 tm 到收盘的行情数据。在没有数据时返回 None，而不是空 DataFrame
        """
        ...

    @abstractmethod
    def get_close_factor(
        self, assets: list[str], start: datetime.date, end: datetime.date
    ) -> pl.DataFrame:
        """获取指定日期范围内的收盘价和复权因子，用以计算市值和除权。

        Args:
            assets: 资产列表
            start: 开始日期
            end: 结束日期

        Returns:
            pl.DataFrame: 包含字段 [dt, asset, close, factor]
        """
        ...
