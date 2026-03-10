"""指数行情存储类

使用 Parquet 文件存储指数行情数据，按年分区。
"""

import datetime
from pathlib import Path
from typing import Literal

import polars as pl

from pyqmt.data.fetchers.tushare_ext import fetch_index_bars
from pyqmt.data.models.calendar import Calendar
from pyqmt.data.stores.base import ParquetStorage


class IndexBarsStore(ParquetStorage):
    """指数行情存储类

    使用 Parquet 文件存储，按年分区。
    字段：symbol, date, open, high, low, close, volume, amount
    """

    def __init__(self, path: str | Path, calendar: Calendar):
        """初始化指数行情存储

        Args:
            path: 存储路径（目录）
            calendar: 日历对象
        """
        path = Path(path).expanduser()
        if path.suffix == ".parquet":
            partition_by = None
        else:
            partition_by = "year"

        super().__init__(
            "IndexBars",
            path,
            calendar,
            fetch_index_bars,
            error_handler=None,
            partition_by=partition_by,
        )

    def get(
        self,
        symbols: list[str] | None = None,
        start: datetime.date | datetime.datetime | None = None,
        end: datetime.date | datetime.datetime | None = None,
        eager_mode: bool = True,
    ) -> pl.DataFrame | pl.LazyFrame:
        """获取指数行情数据

        Args:
            symbols: 指数代码列表，None 表示所有指数
            start: 开始日期
            end: 结束日期
            eager_mode: 是否立即执行

        Returns:
            行情数据 DataFrame 或 LazyFrame
        """
        # 使用父类的 get_with_fetch 方法
        return self.get_with_fetch(symbols, start, end, eager_mode=eager_mode)

    def rec_counts_per_date(
        self, start: datetime.date | None = None, end: datetime.date | None = None
    ) -> dict[datetime.date, int]:
        """获取每个交易日期的记录数量统计

        Args:
            start: 开始日期，如果为None则从最早日期开始统计
            end: 结束日期，如果为None则统计到最晚日期

        Returns:
            一个字典，键为交易日期，值为该日期的记录数量
        """
        lazy = self._scan_store(keep_partition_col=False)
        lazy = lazy.with_columns(pl.col("date").cast(pl.Date))

        if start is not None:
            lazy = lazy.filter(pl.col("date") >= start)
        if end is not None:
            lazy = lazy.filter(pl.col("date") <= end)
        df = lazy.group_by("date").agg(pl.len().alias("n")).collect()
        dates = df["date"].to_list()
        counts = df["n"].to_list()
        result: dict[datetime.date, int] = {}
        for d, c in zip(dates, counts):
            result[d] = int(c)
        return result
