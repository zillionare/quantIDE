"""标准化外部数据源端口."""

from __future__ import annotations

import datetime
from collections.abc import Callable, Iterable
from typing import Protocol

import pandas as pd


class DataFetcherPort(Protocol):
    """外部数据源适配器端口.

    后台同步、模型回填与存储层只依赖这个标准接口，
    不直接依赖具体数据源实现。
    """

    def fetch_calendar(self, epoch: datetime.date) -> pd.DataFrame:
        """获取交易日历."""
        ...

    def fetch_stock_list(self) -> pd.DataFrame | None:
        """获取股票列表."""
        ...

    def fetch_adjust_factor(
        self, dates: Iterable[datetime.date] | datetime.date
    ) -> tuple[pd.DataFrame, list[list]]:
        """获取复权因子."""
        ...

    def fetch_bars(
        self, dates: Iterable[datetime.date] | datetime.date
    ) -> tuple[pd.DataFrame, list[list]]:
        """获取基础日线."""
        ...

    def fetch_limit_price(
        self, dates: Iterable[datetime.date] | datetime.date
    ) -> tuple[pd.DataFrame, list[list]]:
        """获取涨跌停价."""
        ...

    def fetch_st_info(
        self, dates: Iterable[datetime.date] | datetime.date
    ) -> tuple[pd.DataFrame, list[list]]:
        """获取 ST 信息."""
        ...

    def fetch_bars_ext(
        self,
        dates: Iterable[datetime.date] | datetime.date,
        phase_callback: Callable[[str], None] | None = None,
    ) -> tuple[pd.DataFrame, list[list]]:
        """获取标准化扩展日线数据."""
        ...