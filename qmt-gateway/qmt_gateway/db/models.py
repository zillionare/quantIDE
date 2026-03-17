"""数据库模型

与主体工程 pyqmt 保持一致，支持 portfolio（投资组合）概念。
"""

import datetime
import types
import uuid
from dataclasses import dataclass, field, fields
from enum import Enum
from typing import ClassVar, List, Union, get_args, get_origin

from qmt_gateway.core.enums import BidType, BrokerKind, OrderSide, OrderStatus


def new_uuid_id() -> str:
    """生成新的 UUID"""
    return str(uuid.uuid4())


@dataclass
class Entity:
    """模型基类，提供数据库操作方法"""

    __table_name__: ClassVar[str]
    __pk__: ClassVar[Union[str, List[str]]]
    __indexes__: ClassVar[tuple[List[str], bool]] = ([], False)

    @classmethod
    def to_db_schema(cls) -> dict:
        """类方法：解析当前 dataclass 为 sqlite-utils 兼容的 schema 字典"""
        schema = {}

        for f in fields(cls):
            if f.type is uuid.UUID:
                schema[f.name] = str
            elif f.type in (str, int, float, bool):
                schema[f.name] = f.type
            # 处理所有联合类型（Union[A, B] 和 A | B 语法）
            elif (
                hasattr(f.type, "__origin__") and get_origin(f.type) is Union
            ) or isinstance(f.type, types.UnionType):
                # 提取非 None 的类型
                args = get_args(f.type)
                non_none_types = [t for t in args if t is not type(None)]
                if non_none_types:
                    base_type = non_none_types[0]
                    schema[f.name] = (
                        base_type if base_type in (str, int, float, bool) else str
                    )
                else:
                    schema[f.name] = str
            elif isinstance(f.type, type) and issubclass(f.type, Enum):
                schema[f.name] = int
            else:
                schema[f.name] = str
        return schema


@dataclass
class User(Entity):
    """用户模型"""

    __table_name__ = "users"
    __pk__ = "id"

    username: str
    password_hash: str
    is_admin: bool = False
    auto_login: bool = False
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    id: str = field(default_factory=new_uuid_id)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "username": self.username,
            "password_hash": self.password_hash,
            "is_admin": int(self.is_admin),
            "auto_login": int(self.auto_login),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "User":
        return cls(
            id=data["id"],
            username=data["username"],
            password_hash=data["password_hash"],
            is_admin=bool(data.get("is_admin", 0)),
            auto_login=bool(data.get("auto_login", 0)),
            created_at=datetime.datetime.fromisoformat(data["created_at"]),
            updated_at=datetime.datetime.fromisoformat(data["updated_at"]),
        )


@dataclass
class Settings(Entity):
    """系统设置（单例表）"""

    __table_name__ = "settings"
    __pk__ = "id"

    server_port: int = 8130
    log_path: str = "logs"
    log_rotation: str = "00:00"
    log_retention: int = 30
    qmt_account_id: str = ""
    qmt_account_type: str = ""
    qmt_path: str = ""
    xtquant_path: str = ""
    data_start_date: str = "2024-01-01"
    data_home: str = "data"
    init_completed: bool = False
    init_step: int = 0
    init_started_at: datetime.datetime | None = None
    init_completed_at: datetime.datetime | None = None
    id: int = 1

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "server_port": self.server_port,
            "log_path": self.log_path,
            "log_rotation": self.log_rotation,
            "log_retention": self.log_retention,
            "qmt_account_id": self.qmt_account_id,
            "qmt_account_type": self.qmt_account_type,
            "qmt_path": self.qmt_path,
            "xtquant_path": self.xtquant_path,
            "data_start_date": self.data_start_date,
            "data_home": self.data_home,
            "init_completed": int(self.init_completed),
            "init_step": self.init_step,
            "init_started_at": self.init_started_at.isoformat() if self.init_started_at else None,
            "init_completed_at": self.init_completed_at.isoformat() if self.init_completed_at else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Settings":
        return cls(
            id=data.get("id", 1),
            server_port=data.get("server_port", 8130),
            log_path=data.get("log_path", "logs"),
            log_rotation=data.get("log_rotation", "00:00"),
            log_retention=data.get("log_retention", 30),
            qmt_account_id=data.get("qmt_account_id", ""),
            qmt_account_type=data.get("qmt_account_type", ""),
            qmt_path=data.get("qmt_path", ""),
            xtquant_path=data.get("xtquant_path", ""),
            data_start_date=data.get("data_start_date", "2024-01-01"),
            data_home=data.get("data_home", "data"),
            init_completed=bool(data.get("init_completed", 0)),
            init_step=data.get("init_step", 0),
            init_started_at=datetime.datetime.fromisoformat(data["init_started_at"]) if data.get("init_started_at") else None,
            init_completed_at=datetime.datetime.fromisoformat(data["init_completed_at"]) if data.get("init_completed_at") else None,
        )


@dataclass
class Order(Entity):
    """订单模型 - 与主体工程保持一致"""

    __table_name__ = "orders"
    __pk__ = "qtoid"
    __indexes__ = (["qtoid", "tm"], True)

    portfolio_id: str  # 投资组合ID
    asset: str  # 资产代码
    side: OrderSide  # 买卖方向
    shares: float  # 委托数量
    bid_type: BidType  # 委托类型
    tm: datetime.datetime = field(default_factory=datetime.datetime.now)  # 委托时间
    price: float = 0  # 委托价格
    filled: float = 0.0  # 已成交数量

    foid: str | None = None  # 代理(比如QMT)指定的 id
    cid: str | None = None  # 券商柜台合约 id
    status: OrderStatus = OrderStatus.UNREPORTED  # 委托状态
    status_msg: str = ""  # 委托状态描述

    qtoid: str = field(default_factory=new_uuid_id)  # 本委托 ID, pk
    error: str = ""  # 报单错误信息
    extra: str = ""  # 额外信息，json 格式
    strategy: str = ""  # 策略名称

    def __post_init__(self):
        if isinstance(self.tm, str):
            self.tm = datetime.datetime.fromisoformat(self.tm)
        if isinstance(self.status, int):
            self.status = OrderStatus(self.status)
        if isinstance(self.side, int):
            self.side = OrderSide(self.side)
        if isinstance(self.bid_type, int):
            self.bid_type = BidType(self.bid_type)

    def to_dict(self) -> dict:
        return {
            "qtoid": self.qtoid,
            "portfolio_id": self.portfolio_id,
            "asset": self.asset,
            "side": int(self.side),
            "shares": self.shares,
            "bid_type": int(self.bid_type),
            "tm": self.tm.isoformat(),
            "price": self.price,
            "filled": self.filled,
            "foid": self.foid,
            "cid": self.cid,
            "status": int(self.status),
            "status_msg": self.status_msg,
            "error": self.error,
            "extra": self.extra,
            "strategy": self.strategy,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Order":
        return cls(
            qtoid=data.get("qtoid", new_uuid_id()),
            portfolio_id=data["portfolio_id"],
            asset=data["asset"],
            side=OrderSide(data.get("side", 1)),
            shares=data.get("shares", 0),
            bid_type=BidType(data.get("bid_type", 0)),
            tm=datetime.datetime.fromisoformat(data["tm"]) if isinstance(data.get("tm"), str) else data.get("tm", datetime.datetime.now()),
            price=data.get("price", 0),
            filled=data.get("filled", 0),
            foid=data.get("foid"),
            cid=data.get("cid"),
            status=OrderStatus(data.get("status", 48)),
            status_msg=data.get("status_msg", ""),
            error=data.get("error", ""),
            extra=data.get("extra", ""),
            strategy=data.get("strategy", ""),
        )


@dataclass
class Trade(Entity):
    """成交模型 - 与主体工程保持一致"""

    __table_name__ = "trades"
    __pk__ = "tid"
    __indexes__ = (["tid", "tm"], True)
    __foreign_keys__ = [("qtoid", "orders", "qtoid")]

    portfolio_id: str
    tid: str  # 成交 id，pk
    qtoid: str  # 对应的 Order id
    foid: str  # 代理（比如qmt）给出的 order id
    asset: str  # 资产代码
    shares: float  # 成交数量
    price: float  # 成交价格
    amount: float  # 成交金额
    tm: datetime.datetime  # 成交时间
    side: OrderSide  # 成交方向

    cid: str = ""  # 柜台合同编号
    fee: float = 0  # 本笔交易手续费

    def __post_init__(self):
        if isinstance(self.tm, str):
            self.tm = datetime.datetime.fromisoformat(self.tm)
        if isinstance(self.side, int):
            self.side = OrderSide(self.side)

    def to_dict(self) -> dict:
        return {
            "tid": self.tid,
            "portfolio_id": self.portfolio_id,
            "qtoid": self.qtoid,
            "foid": self.foid,
            "asset": self.asset,
            "shares": self.shares,
            "price": self.price,
            "amount": self.amount,
            "tm": self.tm.isoformat(),
            "side": int(self.side),
            "cid": self.cid,
            "fee": self.fee,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Trade":
        return cls(
            tid=data["tid"],
            portfolio_id=data["portfolio_id"],
            qtoid=data["qtoid"],
            foid=data["foid"],
            asset=data["asset"],
            shares=data.get("shares", 0),
            price=data.get("price", 0),
            amount=data.get("amount", 0),
            tm=datetime.datetime.fromisoformat(data["tm"]) if isinstance(data.get("tm"), str) else data.get("tm", datetime.datetime.now()),
            side=OrderSide(data.get("side", 1)),
            cid=data.get("cid", ""),
            fee=data.get("fee", 0),
        )


@dataclass
class Position(Entity):
    """持仓模型 - 与主体工程保持一致"""

    __table_name__ = "positions"
    __pk__ = ["portfolio_id", "dt", "asset"]
    __indexes__ = (["portfolio_id", "asset", "dt"], False)

    portfolio_id: str
    dt: datetime.date
    asset: str
    shares: float
    avail: float  # 可用数量
    price: float  # 持仓成本
    profit: float  # 盈亏比
    mv: float  # 市值

    def __post_init__(self):
        if isinstance(self.dt, str):
            # 处理可能包含时间的ISO格式字符串
            if self.dt.find("T") != -1:
                self.dt = datetime.datetime.fromisoformat(self.dt).date()
            else:
                self.dt = datetime.datetime.strptime(self.dt, "%Y-%m-%d").date()
        elif isinstance(self.dt, datetime.datetime):
            self.dt = self.dt.date()

    def to_dict(self) -> dict:
        return {
            "portfolio_id": self.portfolio_id,
            "dt": self.dt.isoformat(),
            "asset": self.asset,
            "shares": self.shares,
            "avail": self.avail,
            "price": self.price,
            "profit": self.profit,
            "mv": self.mv,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Position":
        dt = data["dt"]
        if isinstance(dt, str):
            if dt.find("T") != -1:
                dt = datetime.datetime.fromisoformat(dt).date()
            else:
                dt = datetime.datetime.strptime(dt, "%Y-%m-%d").date()

        return cls(
            portfolio_id=data["portfolio_id"],
            dt=dt,
            asset=data["asset"],
            shares=data.get("shares", 0),
            avail=data.get("avail", 0),
            price=data.get("price", 0),
            profit=data.get("profit", 0),
            mv=data.get("mv", 0),
        )


@dataclass
class Asset(Entity):
    """资金模型 - 与主体工程保持一致"""

    __table_name__ = "assets"
    __pk__ = ["portfolio_id", "dt"]
    __indexes__ = (["portfolio_id", "dt"], True)

    portfolio_id: str
    dt: datetime.date
    principal: float  # 本金
    cash: float  # 现金
    frozen_cash: float  # 冻结资金
    market_value: float  # 市值
    total: float  # 总资产

    def __post_init__(self):
        if isinstance(self.dt, str):
            # 处理可能包含时间的ISO格式字符串
            if self.dt.find("T") != -1:
                self.dt = datetime.datetime.fromisoformat(self.dt).date()
            else:
                self.dt = datetime.datetime.strptime(self.dt, "%Y-%m-%d").date()
        elif isinstance(self.dt, datetime.datetime):
            self.dt = self.dt.date()

    def to_dict(self) -> dict:
        return {
            "portfolio_id": self.portfolio_id,
            "dt": self.dt.isoformat(),
            "principal": self.principal,
            "cash": self.cash,
            "frozen_cash": self.frozen_cash,
            "market_value": self.market_value,
            "total": self.total,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Asset":
        dt = data["dt"]
        if isinstance(dt, str):
            if dt.find("T") != -1:
                dt = datetime.datetime.fromisoformat(dt).date()
            else:
                dt = datetime.datetime.strptime(dt, "%Y-%m-%d").date()

        return cls(
            portfolio_id=data["portfolio_id"],
            dt=dt,
            principal=data.get("principal", 0),
            cash=data.get("cash", 0),
            frozen_cash=data.get("frozen_cash", 0),
            market_value=data.get("market_value", 0),
            total=data.get("total", 0),
        )


@dataclass
class Portfolio(Entity):
    """投资组合模型 - 与主体工程保持一致"""

    __table_name__ = "portfolios"
    __pk__ = "portfolio_id"
    __indexes__ = (["portfolio_id"], True)

    portfolio_id: str
    kind: BrokerKind
    start: datetime.date
    name: str = ""

    def __post_init__(self):
        if isinstance(self.start, str):
            if self.start.find("T") != -1:
                self.start = datetime.datetime.fromisoformat(self.start).date()
            else:
                self.start = datetime.datetime.strptime(self.start, "%Y-%m-%d").date()

    def to_dict(self) -> dict:
        return {
            "portfolio_id": self.portfolio_id,
            "kind": self.kind.value,
            "start": self.start.isoformat(),
            "name": self.name,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Portfolio":
        start = data["start"]
        if isinstance(start, str):
            if start.find("T") != -1:
                start = datetime.datetime.fromisoformat(start).date()
            else:
                start = datetime.datetime.strptime(start, "%Y-%m-%d").date()

        return cls(
            portfolio_id=data["portfolio_id"],
            kind=BrokerKind(data.get("kind", "qmt")),
            start=start,
            name=data.get("name", ""),
        )


@dataclass
class SyncLog(Entity):
    """同步日志"""

    __table_name__ = "sync_logs"
    __pk__ = "id"

    sync_type: str
    status: str
    message: str = ""
    details: str = ""
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    id: str = field(default_factory=new_uuid_id)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "sync_type": self.sync_type,
            "status": self.status,
            "message": self.message,
            "details": self.details,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "SyncLog":
        return cls(
            id=data.get("id", new_uuid_id()),
            sync_type=data["sync_type"],
            status=data["status"],
            message=data.get("message", ""),
            details=data.get("details", ""),
            created_at=datetime.datetime.fromisoformat(data["created_at"]) if isinstance(data.get("created_at"), str) else data.get("created_at", datetime.datetime.now()),
        )


@dataclass
class HistoryMinuteJob(Entity):
    """历史分钟线下载任务."""

    __table_name__ = "history_minute_jobs"
    __pk__ = "job_id"
    __indexes__ = (["trade_date", "status", "created_at"], False)

    job_id: str
    trade_date: datetime.date
    period: str
    universe: str
    status: str
    file_path: str
    file_name: str
    total_symbols: int = 0
    finished_symbols: int = 0
    rows: int = 0
    error: str = ""
    created_at: datetime.datetime = field(default_factory=datetime.datetime.now)
    updated_at: datetime.datetime = field(default_factory=datetime.datetime.now)

    def __post_init__(self):
        if isinstance(self.trade_date, str):
            if self.trade_date.find("T") != -1:
                self.trade_date = datetime.datetime.fromisoformat(
                    self.trade_date
                ).date()
            else:
                self.trade_date = datetime.datetime.strptime(
                    self.trade_date,
                    "%Y-%m-%d",
                ).date()
        if isinstance(self.created_at, str):
            self.created_at = datetime.datetime.fromisoformat(self.created_at)
        if isinstance(self.updated_at, str):
            self.updated_at = datetime.datetime.fromisoformat(self.updated_at)

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "trade_date": self.trade_date.isoformat(),
            "period": self.period,
            "universe": self.universe,
            "status": self.status,
            "file_path": self.file_path,
            "file_name": self.file_name,
            "total_symbols": self.total_symbols,
            "finished_symbols": self.finished_symbols,
            "rows": self.rows,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "HistoryMinuteJob":
        return cls(
            job_id=data["job_id"],
            trade_date=data["trade_date"],
            period=data["period"],
            universe=data["universe"],
            status=data.get("status", "pending"),
            file_path=data.get("file_path", ""),
            file_name=data.get("file_name", ""),
            total_symbols=int(data.get("total_symbols", 0)),
            finished_symbols=int(data.get("finished_symbols", 0)),
            rows=int(data.get("rows", 0)),
            error=data.get("error", ""),
            created_at=data.get("created_at", datetime.datetime.now()),
            updated_at=data.get("updated_at", datetime.datetime.now()),
        )


@dataclass
class Stock:
    """股票信息（内存中使用，不存数据库）"""

    symbol: str
    name: str
    pinyin: str = ""
    last_close: float = 0.0
    updated_at: datetime.datetime = field(default_factory=datetime.datetime.now)

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "name": self.name,
            "pinyin": self.pinyin,
            "last_close": self.last_close,
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Stock":
        return cls(
            symbol=data["symbol"],
            name=data["name"],
            pinyin=data.get("pinyin", ""),
            last_close=data.get("last_close", 0.0),
            updated_at=datetime.datetime.fromisoformat(data["updated_at"]) if isinstance(data.get("updated_at"), str) else datetime.datetime.now(),
        )
