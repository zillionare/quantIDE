"""板块数据模型"""

import datetime
from dataclasses import dataclass, field
from typing import ClassVar

from pyqmt.data.models.base import Entity


@dataclass
class Sector(Entity):
    """板块模型

    Attributes:
        id: 板块代码（用户自定义或tushare代码）
        name: 板块名称
        sector_type: 类型：'custom'(用户自定义) / 'industry'(行业) / 'concept'(概念)
        source: 来源：'user' / 'tushare'
        description: 描述
        created_at: 创建时间
        updated_at: 更新时间
    """

    __table_name__ = "sectors"
    __pk__ = "id"
    __indexes__ = (["sector_type"], False)

    id: str
    name: str
    sector_type: str  # 'custom', 'industry', 'concept'
    source: str = "user"  # 'user', 'tushare'
    description: str = ""
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime = field(default_factory=datetime.datetime.now)

    def __post_init__(self):
        if isinstance(self.created_at, str):
            self.created_at = datetime.datetime.fromisoformat(self.created_at)
        if isinstance(self.updated_at, str):
            self.updated_at = datetime.datetime.fromisoformat(self.updated_at)


@dataclass
class SectorStock(Entity):
    """板块成分股模型

    Attributes:
        sector_id: 板块ID
        symbol: 股票代码，如 '000001.SZ'
        name: 股票名称（缓存）
        weight: 权重（可选，用于加权计算）
        added_at: 添加时间
    """

    __table_name__ = "sector_stocks"
    __pk__ = ["sector_id", "symbol"]
    __indexes__ = (["sector_id"], False)
    __foreign_keys__ = [("sector_id", "sectors", "id")]

    sector_id: str
    symbol: str
    name: str = ""
    weight: float = 0.0
    added_at: datetime.datetime = field(default_factory=datetime.datetime.now)

    def __post_init__(self):
        if isinstance(self.added_at, str):
            self.added_at = datetime.datetime.fromisoformat(self.added_at)


@dataclass
class SectorBar(Entity):
    """板块行情数据模型

    Attributes:
        sector_id: 板块ID
        dt: 交易日期
        open: 开盘价
        high: 最高价
        low: 最低价
        close: 收盘价
        volume: 成交量（股）
        amount: 成交额（元）
    """

    __table_name__ = "sector_bars"
    __pk__ = ["sector_id", "dt"]
    __indexes__ = (["dt"], False)
    __foreign_keys__ = [("sector_id", "sectors", "id")]

    sector_id: str
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
