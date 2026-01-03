import datetime
from dataclasses import dataclass
from typing import Iterable, Sequence

import polars as pl

from pyqmt.core.message import msg_hub
from pyqmt.data.models.daily_bars import daily_bars


class TimeProvider:
    def now(self) -> datetime.datetime:
        return datetime.datetime.now()

    def advance_to(self, dt: datetime.datetime) -> None:
        pass


class SimClock(TimeProvider):
    def __init__(self):
        self._now = datetime.datetime.combine(datetime.date.today(), datetime.time(9, 30, 0))

    def now(self) -> datetime.datetime:
        return self._now

    def advance_to(self, dt: datetime.datetime) -> None:
        self._now = dt


@dataclass(frozen=True)
class QuoteEvent:
    asset: str
    last_price: float
    up_limit: float | None = None
    down_limit: float | None = None
    low: float | None = None
    high: float | None = None
    volume: float | None = None
    tm: datetime.datetime | None = None


class ParquetFeed:
    def __init__(self, clock: TimeProvider | None = None):
        self._clock = clock or TimeProvider()

    def publish_open_close_for_date(self, date: datetime.date, assets: Sequence[str]) -> None:
        df = daily_bars.get_bars_in_range(date, date, assets=list(assets), adjust=None, eager_mode=True)
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        if df.height == 0:
            return
        for row in df.iter_rows(named=True):
            asset = str(row["asset"])
            open_px = float(row["open"])
            close_px = float(row["close"])
            low = float(row["low"])
            high = float(row["high"])
            up = row.get("up_limit")
            down = row.get("down_limit")
            vol = row.get("volume")
            tm_open = max(
                self._clock.now(),
                datetime.datetime.combine(date, datetime.time(9, 30, 0)),
            )
            tm_close = datetime.datetime.combine(date, datetime.time(15, 0, 0))
            msg_hub.publish(
                "md:bar:1d",
                QuoteEvent(
                    asset=asset,
                    last_price=open_px,
                    up_limit=up,
                    down_limit=down,
                    low=low,
                    high=high,
                    volume=vol,
                    tm=tm_open,
                ),
            )
            msg_hub.publish(
                "md:bar:1d",
                QuoteEvent(
                    asset=asset,
                    last_price=close_px,
                    up_limit=up,
                    down_limit=down,
                    low=low,
                    high=high,
                    volume=vol,
                    tm=tm_close,
                ),
            )


class RedisStreamFeed:
    def __init__(self):
        pass

    def forward(self, events: Iterable[dict]) -> None:
        for e in events:
            msg_hub.publish("md:quote:1s", e)
