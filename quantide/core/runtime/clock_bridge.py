"""时钟端口适配器."""

import datetime
from collections.abc import Iterable

from quantide.core.enums import FrameType
from quantide.core.ports import ClockPort
from quantide.data.models.calendar import calendar


class SystemClockAdapter(ClockPort):
    """系统时钟适配器."""

    def now(self) -> datetime.datetime:
        """返回当前系统时间."""
        return datetime.datetime.now()

    def set_now(self, tm: datetime.datetime) -> None:
        """设置当前时间."""
        raise RuntimeError("system clock does not support set_now")

    def iter_frames(
        self,
        start: datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime,
        frame_type: FrameType,
    ) -> Iterable[datetime.date | datetime.datetime]:
        """按周期遍历时间帧."""
        return calendar.get_frames(start, end, frame_type)


class BacktestClockAdapter(ClockPort):
    """回测时钟适配器."""

    def __init__(self):
        """初始化回测时钟."""
        self._now: datetime.datetime | None = None

    def now(self) -> datetime.datetime:
        """返回当前时钟时间."""
        return self._now or datetime.datetime.now()

    def set_now(self, tm: datetime.datetime) -> None:
        """设置当前时间."""
        self._now = tm

    def iter_frames(
        self,
        start: datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime,
        frame_type: FrameType,
    ) -> Iterable[datetime.date | datetime.datetime]:
        """按周期遍历时间帧."""
        return calendar.get_frames(start, end, frame_type)
