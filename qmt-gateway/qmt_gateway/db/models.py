"""数据模型定义

定义所有数据库表对应的数据模型，使用 dataclass 简化定义。
"""

import datetime
import uuid
from dataclasses import dataclass, field, fields
from typing import Any, ClassVar


def new_uuid_id() -> str:
    """生成新的 UUID 字符串"""
    return str(uuid.uuid4())


@dataclass
class Entity:
    """基础实体类

    所有数据模型都继承此类，提供通用的数据库操作方法。
    """

    __table_name__: ClassVar[str] = ""
    __pk__: ClassVar[str | list[str]] = "id"
    __indexes__: ClassVar[tuple] = ([], False)
    __foreign_keys__: ClassVar[list] = []

    def to_dict(self) -> dict[str, Any]:
        """转换为字典"""
        result = {}
        for f in fields(self):
            value = getattr(self, f.name)
            if isinstance(value, datetime.datetime):
                result[f.name] = value.isoformat()
            elif isinstance(value, datetime.date):
                result[f.name] = value.strftime("%Y-%m-%d")
            else:
                result[f.name] = value
        return result

    @classmethod
    def to_db_schema(cls) -> dict[str, str]:
        """转换为数据库表结构定义"""
        schema = {}
        type_map = {
            str: "TEXT",
            int: "INTEGER",
            float: "FLOAT",
            bool: "INTEGER",
            datetime.datetime: "TEXT",
            datetime.date: "TEXT",
        }

        for f in fields(cls):
            origin = f.type
            # 处理 Optional 类型
            if hasattr(origin, "__origin__") and origin.__origin__ is not None:
                args = getattr(origin, "__args__", ())
                if len(args) > 0:
                    origin = args[0]

            # 获取基本类型
            if hasattr(origin, "__origin__"):
                origin = origin.__origin__

            schema[f.name] = type_map.get(origin, "TEXT")

        return schema

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Entity":
        """从字典创建实例"""
        valid_fields = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in data.items() if k in valid_fields}
        return cls(**filtered_data)


@dataclass
class User(Entity):
    """用户表

    存储管理员账号信息。
    """

    __table_name__ = "users"
    __pk__ = "id"
    __indexes__ = (["username"], True)

    username: str
    password_hash: str
    is_admin: bool = True
    auto_login: bool = False
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    id: str = field(default_factory=new_uuid_id)

    def __post_init__(self):
        if isinstance(self.created_at, str):
            self.created_at = datetime.datetime.fromisoformat(self.created_at)
        if isinstance(self.updated_at, str):
            self.updated_at = datetime.datetime.fromisoformat(self.updated_at)


@dataclass
class Settings(Entity):
    """系统设置表

    单例表设计，只存储一条记录（id=1）。
    存储所有系统配置，包括服务器设置、QMT配置等。
    """

    __table_name__ = "settings"
    __pk__ = "id"
    __indexes__ = ([], False)

    id: int = 1

    # 服务器设置
    server_port: int = 8130
    log_path: str = "~/.qmt-gateway/log"
    log_rotation: str = "10 MB"
    log_retention: int = 10

    # QMT 配置
    qmt_account_id: str = ""
    qmt_account_type: str = "live"
    qmt_path: str = ""
    xtquant_path: str = ""

    # 数据配置
    data_start_date: datetime.date | None = None
    data_home: str = "~/.qmt-gateway/data"

    # 初始化状态
    init_completed: bool = False
    init_step: int = 0
    init_started_at: datetime.datetime | None = None
    init_completed_at: datetime.datetime | None = None

    # 元数据
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime = field(default_factory=datetime.datetime.now)

    def __post_init__(self):
        if isinstance(self.created_at, str):
            self.created_at = datetime.datetime.fromisoformat(self.created_at)
        if isinstance(self.updated_at, str):
            self.updated_at = datetime.datetime.fromisoformat(self.updated_at)
        if isinstance(self.init_started_at, str):
            self.init_started_at = datetime.datetime.fromisoformat(self.init_started_at)
        if isinstance(self.init_completed_at, str):
            self.init_completed_at = datetime.datetime.fromisoformat(self.init_completed_at)
        if isinstance(self.data_start_date, str):
            self.data_start_date = datetime.datetime.strptime(self.data_start_date, "%Y-%m-%d").date()


@dataclass
class Sector(Entity):
    """板块表

    存储板块列表信息，带日期版本控制。
    """

    __table_name__ = "sectors"
    __pk__ = ["id", "trade_date"]
    __indexes__ = (["sector_type", "trade_date"], False)

    id: str
    name: str
    sector_type: str
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
    """板块成分股表

    存储板块成分股信息，带日期版本控制。
    """

    __table_name__ = "sector_constituents"
    __pk__ = ["sector_id", "trade_date", "symbol"]
    __indexes__ = (["sector_id", "trade_date"], False)

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
    """板块行情表

    存储板块历史行情数据。
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


@dataclass
class SyncLog(Entity):
    """同步日志表

    记录数据同步操作的日志。
    """

    __table_name__ = "sync_logs"
    __pk__ = "id"
    __indexes__ = (["sync_type", "created_at"], False)

    sync_type: str
    status: str
    message: str = ""
    details: str = ""
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    id: str = field(default_factory=new_uuid_id)

    def __post_init__(self):
        if isinstance(self.created_at, str):
            self.created_at = datetime.datetime.fromisoformat(self.created_at)


@dataclass
class Order(Entity):
    """订单表

    存储交易订单信息。
    """

    __table_name__ = "orders"
    __pk__ = "qtoid"
    __indexes__ = (["qtoid", "tm"], True)

    # 必需字段（无默认值）
    asset: str
    side: str
    shares: float
    # 可选字段（有默认值）
    qtoid: str = field(default_factory=new_uuid_id)
    price: float = 0.0
    bid_type: str = "limit"
    tm: datetime.datetime = field(default_factory=datetime.datetime.now)
    filled: float = 0.0
    foid: str = ""
    status: str = "unreported"
    status_msg: str = ""
    error: str = ""

    def __post_init__(self):
        if isinstance(self.tm, str):
            self.tm = datetime.datetime.fromisoformat(self.tm)


@dataclass
class Trade(Entity):
    """成交表

    存储交易成交信息。
    """

    __table_name__ = "trades"
    __pk__ = "tid"
    __indexes__ = (["qtoid", "tm"], False)
    __foreign_keys__ = [("qtoid", "orders", "qtoid")]

    tid: str
    qtoid: str
    asset: str
    shares: float
    price: float
    amount: float
    tm: datetime.datetime
    side: str
    fee: float = 0.0

    def __post_init__(self):
        if isinstance(self.tm, str):
            self.tm = datetime.datetime.fromisoformat(self.tm)


@dataclass
class Position(Entity):
    """持仓表

    存储账户持仓信息。
    """

    __table_name__ = "positions"
    __pk__ = ["asset", "dt"]
    __indexes__ = (["asset"], False)

    asset: str
    dt: datetime.date
    shares: float
    avail: float
    price: float
    mv: float

    def __post_init__(self):
        if isinstance(self.dt, str):
            self.dt = datetime.date.fromisoformat(self.dt)


@dataclass
class Asset(Entity):
    """资金表

    存储账户资金信息。
    """

    __table_name__ = "assets"
    __pk__ = "dt"
    __indexes__ = (["dt"], True)

    dt: datetime.date
    principal: float
    cash: float
    frozen_cash: float
    market_value: float
    total: float

    def __post_init__(self):
        if isinstance(self.dt, str):
            self.dt = datetime.date.fromisoformat(self.dt)
