"""指数行情数据模型

使用 Parquet 文件存储指数行情数据，按年分区。
与个股行情不同，指数行情没有复权因子和涨跌停价格。
"""

import datetime
from pathlib import Path

import pandas as pd
import polars as pl
from loguru import logger

from pyqmt.core.singleton import singleton
from pyqmt.data.models.bars import Bars
from pyqmt.data.models.calendar import Calendar
from pyqmt.data.stores.index_bars import IndexBarsStore


@singleton
class IndexBars(Bars):
    """指数行情数据管理类

    使用 Parquet 文件存储，按年分区。
    字段包括：symbol, date, open, high, low, close, volume, amount
    """

    def __init__(self):
        self._store: IndexBarsStore | None = None
        self._calendar: Calendar | None = None

    @property
    def store(self) -> IndexBarsStore:
        """获取存储实例"""
        if self._store is None:
            raise RuntimeError("IndexBars store 未初始化")
        return self._store

    def connect(self, store_path: str | Path, calendar: Calendar) -> None:
        """连接存储

        Args:
            store_path: 存储路径
            calendar: 日历对象
        """
        if self._store is not None:
            logger.warning("重加载 IndexBars store")

        self._calendar = calendar
        self._store = IndexBarsStore(store_path, calendar)

    def load(self, store_path: str | Path, calendar: Calendar) -> "IndexBars":
        """加载存储（兼容旧接口）

        Args:
            store_path: 存储路径
            calendar: 日历对象

        Returns:
            IndexBars: 自身实例
        """
        self.connect(store_path, calendar)
        return self

    def __getattr__(self, name: str):
        """代理到 store 的属性"""
        if name in ("start", "end", "total_dates", "size", "last_update_time"):
            return getattr(self.store, name)
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def get_bars_in_range(
        self,
        start: datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime | None = None,
        symbols: list[str] | None = None,
        eager_mode: bool = True,
    ) -> pl.DataFrame | pl.LazyFrame:
        """获取指定日期范围内的指数行情数据

        Args:
            start: 开始日期
            end: 结束日期，默认为 None，表示获取到最后一个交易日
            symbols: 指数代码列表，默认为 None 表示获取所有指数
            eager_mode: 是否立即执行，默认为 True

        Returns:
            行情数据 DataFrame 或 LazyFrame
        """
        return self.store.get(symbols, start, end, eager_mode=eager_mode)

    def get_bars(
        self,
        n: int,
        end: datetime.date | datetime.datetime | None = None,
        symbols: list[str] | None = None,
        eager_mode: bool = True,
    ) -> pl.DataFrame | pl.LazyFrame:
        """获取最近 n 个交易日的行情数据

        Args:
            n: 最近 n 个交易日
            end: 结束日期，默认为 None 表示获取到最后一个交易日
            symbols: 指数代码列表，默认为 None 表示获取所有指数
            eager_mode: 是否立即执行，默认为 True

        Returns:
            行情数据 DataFrame 或 LazyFrame
        """
        assert self._calendar is not None

        if end is None:
            end = self.end
        end_date = self._calendar.floor(end)
        start_date = self._calendar.shift(end_date, -n + 1)

        return self.get_bars_in_range(start_date, end_date, symbols, eager_mode=eager_mode)

    def get_price(
        self,
        symbol: str,
        date: datetime.date | datetime.datetime,
    ) -> tuple[float, float, float, float, float, float]:
        """获取指定日期指数的行情数据

        Args:
            symbol: 指数代码
            date: 日期

        Returns:
            (open, high, low, close, volume, amount) 元组
        """
        df = self.store.get([symbol], date, date, eager_mode=True)
        if df.is_empty():
            return (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

        row = df.row(0, named=True)
        return (
            float(row["open"]),
            float(row["high"]),
            float(row["low"]),
            float(row["close"]),
            float(row["volume"]),
            float(row["amount"]),
        )

    def append_data(
        self,
        data: pl.DataFrame | pd.DataFrame,
    ) -> None:
        """追加数据到存储

        Args:
            data: 要追加的数据，必须包含 symbol, date, open, high, low, close, volume, amount 列
        """
        self.store.append_data(data)


# 全局实例
index_bars = IndexBars()
