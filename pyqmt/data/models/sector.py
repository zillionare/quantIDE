"""板块数据模型"""

import datetime
from dataclasses import dataclass, field

from pyqmt.data.models.base import Entity


@dataclass
class Sector(Entity):
    """板块模型，带日期版本控制

    Attributes:
        id: 板块代码（如 'SW1电力设备'）
        name: 板块名称
        sector_type: 类型：'concept'(概念) / 'etf' / 'convertible'(转债) / 'sw1' / 'sw2' / 'index'(指数)
        source: 来源：'qmt'
        trade_date: 数据日期，用于PIT（Point In Time）查询
        description: 描述
        created_at: 创建时间
    """

    __table_name__ = "sectors"
    __pk__ = ["id", "trade_date"]
    __indexes__ = (["sector_type", "trade_date"], False)

    id: str
    name: str
    sector_type: str  # 'concept', 'etf', 'convertible', 'sw1', 'sw2', 'index'
    source: str = "qmt"
    trade_date: datetime.date = field(default_factory=datetime.date.today)
    description: str = ""
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime = field(default_factory=datetime.datetime.now)

    def __post_init__(self):
        if isinstance(self.trade_date, str):
            self.trade_date = datetime.date.fromisoformat(self.trade_date)
        if isinstance(self.created_at, str):
            self.created_at = datetime.datetime.fromisoformat(self.created_at)
        if isinstance(self.updated_at, str):
            self.updated_at = datetime.datetime.fromisoformat(self.updated_at)


@dataclass
class SectorConstituent(Entity):
    """板块成分股模型，带日期版本控制

    Attributes:
        sector_id: 板块ID
        trade_date: 数据日期
        symbol: 股票代码，如 '000001.SZ'
        name: 股票名称（缓存）
        weight: 权重（可选，用于加权计算）
        created_at: 创建时间
    """

    __table_name__ = "sector_constituents"
    __pk__ = ["sector_id", "trade_date", "symbol"]
    __indexes__ = (["sector_id", "trade_date", "symbol"], False)

    sector_id: str
    trade_date: datetime.date
    symbol: str
    name: str = ""
    weight: float = 0.0
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime = field(default_factory=datetime.datetime.now)

    def __post_init__(self):
        if isinstance(self.trade_date, str):
            self.trade_date = datetime.date.fromisoformat(self.trade_date)
        if isinstance(self.created_at, str):
            self.created_at = datetime.datetime.fromisoformat(self.created_at)
        if isinstance(self.updated_at, str):
            self.updated_at = datetime.datetime.fromisoformat(self.updated_at)


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
