import datetime
from abc import ABC, abstractmethod

import polars as pl


class Bars:
    @abstractmethod
    def get_bars_in_range(
        self,
        start: datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime | None = None,
        assets: list[str] | None = None,
        adjust: str | None = "qfq",
        eager_mode: bool = True,
    ) -> pl.DataFrame | pl.LazyFrame: ...

    @abstractmethod
    def get_bars(
        self,
        n: int,
        end: datetime.date | datetime.datetime | None = None,
        assets: list[str] | None = None,
        adjust: str | None = "qfq",
        eager_mode: bool = True,
    ) -> pl.DataFrame | pl.LazyFrame:
        """获取最近 n 个周期的行情数据

        Args:
            n (int): 最近 n 个交易日
            end (datetime.date | datetime.datetime | None, optional): 结束日期/时间，默认为 None，表示获取缓存中最后一个周期。 Defaults to None.
            assets (list[str] | None, optional): 获取指定股票的行情数据，默认为 None，表示获取所有股票。 Defaults to None.
        """
        ...

    @abstractmethod
    def get_price(
        self,
        asset: str,
        date: datetime.date | datetime.datetime,
        adjust: str | None = "qfq",
    ):
        """返回`date`日（时间）的`asset`的收盘价、涨跌停价

        Args:
            asset (str): _description_
            date (datetime.date | datetime.datetime): _description_
            adjust (str | None, optional): _description_. Defaults to "qfq".
        """
        ...
