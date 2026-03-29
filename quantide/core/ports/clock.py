"""时钟端口抽象."""

import datetime
from collections.abc import Iterable
from typing import Protocol

from quantide.core.enums import FrameType


class ClockPort(Protocol):
    """时钟端口."""

    def now(self) -> datetime.datetime:
        """返回当前时间."""
        ...

    def set_now(self, tm: datetime.datetime) -> None:
        """设置当前时间."""
        ...

    def iter_frames(
        self,
        start: datetime.date | datetime.datetime,
        end: datetime.date | datetime.datetime,
        frame_type: FrameType,
    ) -> Iterable[datetime.date | datetime.datetime]:
        """遍历时间帧."""
        ...
