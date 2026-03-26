"""指数行情存储类.

读取本地已落盘数据属于正常能力；基于 xtdata 的抓取入口仅保留为
兼容/离线工具路径。
"""

import datetime
from pathlib import Path
from typing import Callable

import polars as pl

from pyqmt.data.models.calendar import Calendar
from pyqmt.data.stores.base import ParquetStorage


_REMOVED_MESSAGE = "指数抓取功能已从 pyqmt 主体移除。"


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
            None,
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
        lf = self._scan_store(keep_partition_col=False)
        if lf is None:
            return pl.DataFrame() if eager_mode else pl.LazyFrame()

        # 过滤日期
        if start is not None:
            lf = lf.filter(pl.col("date") >= start)
        if end is not None:
            lf = lf.filter(pl.col("date") <= end)

        # 过滤指数
        if symbols is not None:
            lf = lf.filter(pl.col("sector_id").is_in(symbols))

        if eager_mode:
            return lf.collect()
        return lf

    def fetch(
        self,
        symbol: str,
        start: datetime.date,
        end: datetime.date,
        progress_callback: Callable[[int, int, str], None] | None = None,
    ) -> int:
        """获取并保存指数行情数据

        Args:
            symbol: 指数代码
            start: 开始日期
            end: 结束日期
            progress_callback: 进度回调函数 (current, total, message)

        Returns:
            获取的记录数
        """
        raise RuntimeError(_REMOVED_MESSAGE)

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
        if lazy is None:
            return {}

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
