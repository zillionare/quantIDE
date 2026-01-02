import datetime

import polars as pl
from loguru import logger

from pyqmt.config import cfg
from pyqmt.core.singleton import singleton
from pyqmt.core.enums import FrameType
from pyqmt.data.helper import hfq_adjustment, qfq_adjustment
from pyqmt.data.models.calendar import Calendar
from pyqmt.data.stores.bars import DailyBarsStore


@singleton
class DailyBars:
    def __init__(self):
        self._store: DailyBarsStore | None = None
        self._calendar: Calendar | None = None

    @property
    def store(self) -> DailyBarsStore:
        if self._store is None:
            raise RuntimeError("daily bars store 未初始化")
        return self._store

    def connect(self, store_path: str, calendar_store_path: str) -> None:
        if self._store is not None:
            logger.warning("重加载 daily bars store")

        self._calendar = Calendar().load(calendar_store_path)
        self._store = DailyBarsStore(store_path, self._calendar)

    def __getattr__(self, name: str):
        if name in ("start", "end", "total_dates", "size", "last_update_time"):
            return getattr(self.store, name)

    def get_bars_in_range(
        self,
        start: datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime | None = None,
        assets: list[str] | None = None,
        adjust: str | None = "qfq",
        eager_mode: bool = True,
    ) -> pl.DataFrame | pl.LazyFrame:
        """获取指定日期范围内的日线数据。

        参数：
            assets: 需要获取的股票列表
            start: 开始日期/时间
            end: 结束日期/时间，默认为 None，表示获取缓存中最后一个交易日
        """
        lf = self.store.get(assets, start, end, eager_mode=False)
        if adjust == "qfq":
            lf = qfq_adjustment(lf, eager_mode=False)
        elif adjust == "hfq":
            lf = hfq_adjustment(lf, eager_mode=False)

        if eager_mode:
            return lf.collect()

        return lf

    def get_bars(
        self,
        n: int,
        end: datetime.date | datetime.datetime | None = None,
        assets: list[str] | None = None,
        adjust: str | None = "qfq",
        eager_mode: bool = True,
    ) -> pl.DataFrame | pl.LazyFrame:
        """获取最近 n 个交易日的行情数据

        Args:
            n (int): 最近 n 个交易日
            end (datetime.date | datetime.datetime | None, optional): 结束日期/时间，默认为 None，表示获取缓存中最后一个交易日。 Defaults to None.
            assets (list[str] | None, optional): 获取指定股票的行情数据，默认为 None，表示获取所有股票。 Defaults to None.
        """
        assert self._calendar is not None

        if end is None:
            end = datetime.datetime.now(tz=cfg.TIMEZONE)
        end_date = self._calendar.floor(end, FrameType.DAY)
        start_date = self._calendar.shift(end_date, -n + 1, FrameType.DAY)

        return self.get_bars_in_range(
            start_date, end_date, assets, adjust=adjust, eager_mode=eager_mode
        )

daily_bars = DailyBars()
