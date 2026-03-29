import datetime
from dataclasses import dataclass


@dataclass
class FetcherError:
    """数据获取中的错误信息"""

    module: str
    date: datetime.date | None
    msg: str
