import datetime
from dataclasses import dataclass


@dataclass
class Bar:
    """
    Bar data
    """

    asset: str
    date: datetime.date
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float
    turnover: float
    adjust: float
    st: bool
    up_limit: float
    down_limit: float

    @classmethod
    def fields(cls) -> list[str]:
        """返回固定数据字段名"""
        return [
            "date",
            "asset",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "turnover",
            "adjust",
            "st",
            "up_limit",
            "down_limit",
        ]
