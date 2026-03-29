import datetime
from pathlib import Path
from typing import Literal

import polars as pl

from quantide.data.fetchers.tushare import fetch_bars_ext
from quantide.data.models.calendar import Calendar
from quantide.data.stores.base import ParquetStorage


class DailyBarsStore(ParquetStorage):
    """日线行情存储、更新"""

    def __init__(self, path: str | Path, calendar: Calendar):
        path = Path(path).expanduser()
        if path.suffix == ".parquet":
            partition_by = None
        else:
            partition_by = "year"

        super().__init__(
            "DailyBars",
            path,
            calendar,
            fetch_bars_ext,
            error_handler=None,
            partition_by=partition_by,
        )

    def rec_counts_per_date(
        self, start: datetime.date | None = None, end: datetime.date | None = None
    ) -> dict[datetime.date, int]:
        """
        获取每个交易日期的记录数量统计

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
