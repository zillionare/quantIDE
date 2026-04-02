import datetime
from pathlib import Path
from typing import Literal

import polars as pl

from quantide.core.ports import DataFetcherPort
from quantide.data.fetchers.registry import get_data_fetcher
from quantide.data.models.calendar import Calendar
from quantide.data.stores.base import ParquetStorage, _as_date_bound


class DailyBarsStore(ParquetStorage):
    """日线行情存储、更新"""

    def __init__(
        self,
        path: str | Path,
        calendar: Calendar,
        data_fetcher: DataFetcherPort | None = None,
    ):
        path = Path(path).expanduser()
        if path.suffix == ".parquet":
            partition_by = None
        else:
            partition_by = "year"

        self._data_fetcher = data_fetcher or get_data_fetcher()

        super().__init__(
            "DailyBars",
            path,
            calendar,
            self._fetch_bars_ext,
            error_handler=None,
            partition_by=partition_by,
        )

    def _fetch_bars_ext(
        self,
        dates: list[datetime.date] | datetime.date,
        phase_callback=None,
    ):
        return self._data_fetcher.fetch_bars_ext(dates, phase_callback=phase_callback)

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
            lazy = lazy.filter(pl.col("date").dt.strftime("%F") >= start.isoformat())
        if end is not None:
            lazy = lazy.filter(pl.col("date").dt.strftime("%F") <= end.isoformat())
        df = lazy.group_by("date").agg(pl.len().alias("n")).collect()
        dates = df["date"].to_list()
        counts = df["n"].to_list()
        result: dict[datetime.date, int] = {}
        for d, c in zip(dates, counts):
            result[d] = int(c)
        return result
