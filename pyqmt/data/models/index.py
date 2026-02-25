"""指数数据模型"""

import datetime
from dataclasses import dataclass, field
from typing import ClassVar

from pyqmt.data.models.base import Entity


@dataclass
class Index(Entity):
    """指数模型

    Attributes:
        symbol: 指数代码，如 '000001.SH'
        name: 指数名称
        index_type: 类型：'market'(市场指数) / 'industry'(行业指数) / 'concept'(概念指数)
        category: 分类：如 '上证系列' / '深证系列' / '中证系列'
        publisher: 发布机构
        base_date: 基准日期
        base_point: 基准点数
        list_date: 上市日期
        description: 描述
        updated_at: 更新时间
    """

    __table_name__ = "indices"
    __pk__ = "symbol"
    __indexes__ = (["index_type", "category"], False)

    symbol: str
    name: str
    index_type: str = "market"  # 'market', 'industry', 'concept'
    category: str = ""
    publisher: str = ""
    base_date: datetime.date | None = None
    base_point: float = 0.0
    list_date: datetime.date | None = None
    description: str = ""
    updated_at: datetime.datetime = field(default_factory=datetime.datetime.now)

    def __post_init__(self):
        if isinstance(self.base_date, str):
            self.base_date = datetime.date.fromisoformat(self.base_date)
        if isinstance(self.list_date, str):
            self.list_date = datetime.date.fromisoformat(self.list_date)
        if isinstance(self.updated_at, str):
            self.updated_at = datetime.datetime.fromisoformat(self.updated_at)


@dataclass
class IndexBar(Entity):
    """指数行情数据模型

    Attributes:
        symbol: 指数代码
        dt: 交易日期
        open: 开盘价
        high: 最高价
        low: 最低价
        close: 收盘价
        volume: 成交量（股）
        amount: 成交额（元）
    """

    __table_name__ = "index_bars"
    __pk__ = ["symbol", "dt"]
    __indexes__ = (["symbol", "dt"], False)
    __foreign_keys__ = [("symbol", "indices", "symbol")]

    symbol: str
    dt: datetime.date
    open: float
    high: float
    low: float
    close: float
    volume: int
    amount: float

    def __post_init__(self):
        if isinstance(self.dt, str):
            self.dt = datetime.date.fromisoformat(self.dt)
