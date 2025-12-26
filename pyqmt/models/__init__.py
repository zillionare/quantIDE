"""Modles 定义

本模块定义了常用的数据模型，比如 Order, Trade, Position, Asset 等。同时还提供了一些工具函数，比如将 dataclass 转换为 fastlite 兼容的 schema 字典。

## db_model

自动为 dataclass 注入表名、主键、索引等元数据。

## _dataclass_to_schema

sqlite_utils 赋予应用无须事先创建表结构的能力，但是，为了性能和数据类型精确性考虑，手动创建数据库表是更好的方式。

`_dataclass_to_schema` 自动将 dataclass 转换为数据库字段声明，从而可以用于创建数据库表结构（基于 sqlite_utils）。

Example:

```python
@db_model("orders", "qtoid", (["qtoid", "tm"], True), foreign_keys=[("from_col", "to_table", "to_col")])
@dataclass
class OrderModel:
    asset: str                              # 资产代码
    side: OrderSide
    shares: float|int                       # 委托数量。调用者需要保证符合交易要求
    price: float
    bid_type: BidType                       # 委托类型，比如限价单、市价单
    tm: datetime.datetime|None = None       # 下单时间

    foid: str|None = None                   # 代理(比如QMT)指定的 id，透传，一般用以查错
    cid: str|None = None                    # 券商柜台合约 id
    status: OrderStatus = OrderStatus.UNREPORTED # 委托状态，比如未报、待报、已报、部成等
    status_msg: str = ""                    # 委托状态描述，比如废单原因

    # 本委托 ID, pk
    qtoid: str = field(default_factory=lambda: "qtide-" + uuid.uuid4().hex[:16])
    strategy: str = ""                   # 策略名称

for model in [OrderModel, TradeModel, AssetModel]:
    table = model.__table_name__
    pk = model.__pk__

    t: su.db.Table = self[table]
    t.create(model.to_db_schema(), pk=pk)

    if model.__indexes__ is not None:
        indexes, is_unique = model.__indexes__
        t.create_index(indexes, unique=is_unique)
```

通过这样的封装，初始化数据库、创建表结构就变得非常简洁。
"""

from dataclasses import dataclass, field, fields
from typing import Type, TypeVar
from enum import IntEnum
import datetime
import uuid
import types
from pyqmt.core.enums import OrderSide, BidType, OrderStatus

T = TypeVar("T")


def new_uuid_id() -> str:
    return "qtide-" + uuid.uuid4().hex[:16]


def _dataclass_to_schema(cls) -> dict:
    """类方法：解析当前 dataclass 为 fastlite 兼容的 schema 字典"""
    schema = {}

    for f in fields(cls):
        if f.type is uuid.UUID:
            schema[f.name] = str
        elif f.type in (str, int, float, bool):
            schema[f.name] = f.type
        # 处理所有联合类型（Union[A, B] 和 A | B 语法）
        elif (
            hasattr(f.type, "__origin__") and f.type.__origin__ is Union
        ) or isinstance(f.type, types.UnionType):
            # 提取非 None 的类型
            non_none_types = [t for t in f.type.__args__ if t is not type(None)]
            if non_none_types:
                base_type = non_none_types[0]
                schema[f.name] = (
                    base_type if base_type in (str, int, float, bool) else str
                )
            else:
                schema[f.name] = str
        elif isinstance(f.type, type) and issubclass(f.type, IntEnum):
            schema[f.name] = int
        else:
            schema[f.name] = str
    return schema


def db_model(
    table_name: str,
    pk: str,
    indexes: tuple[list[str], bool] | None = None,
    foreign_keys: list | None = None,
):
    def wrapper(cls: Type[T]) -> Type[T]:
        setattr(cls, "__table_name__", table_name)
        setattr(cls, "__pk__", pk)
        setattr(cls, "__indexes__", indexes or [])  # 例如 (["qtoid", "foid"], True)
        setattr(
            cls, "__foreign_keys__", foreign_keys or []
        )  # 例如 [("qtoid", "orders", "qtoid")]

        if not hasattr(cls, "to_db_schema"):

            @classmethod
            def to_db_schema(cls_inner):
                return _dataclass_to_schema(cls_inner)

            cls.to_db_schema = to_db_schema
        return cls

    return wrapper


@db_model("orders", "qtoid", (["qtoid", "tm"], True))
@dataclass
class OrderModel:
    asset: str  # 资产代码
    side: OrderSide
    shares: float | int  # 委托数量。调用者需要保证符合交易要求
    price: float
    bid_type: BidType  # 委托类型，比如限价单、市价单
    tm: datetime.datetime | None = None  # 下单时间

    foid: str | None = None  # 代理(比如QMT)指定的 id，透传，一般用以查错
    cid: str | None = None  # 券商柜台合约 id
    status: OrderStatus = (
        OrderStatus.UNREPORTED
    )  # 委托状态，比如未报、待报、已报、部成等
    status_msg: str = ""  # 委托状态描述，比如废单原因

    # 本委托 ID, pk
    qtoid: str = field(default_factory=new_uuid_id)
    strategy: str = ""  # 策略名称
    error: str = ""  # 报单错误信息，包括错误码和错误信息,以:分隔

    def __post_init__(self):
        if isinstance(self.tm, str):
            self.tm = datetime.datetime.fromisoformat(self.tm)


@db_model(
    "trades", "tid", (["tid", "tm"], True), foreign_keys=[("qtoid", "orders", "qtoid")]
)
@dataclass
class TradeModel:
    tid: str  # 成交 id，pk。可使用代理（比如 qmt）返回值
    qtoid: str  # 对应的 Order id (quantide order id) - 外键引用 orders 表的 qtoid
    foid: str  # 代理（比如qmt）给出的 order id
    asset: str  # 资产代码
    shares: float | int  # 成交数量
    price: float  # 成交价格
    amount: float  # 成交金额 = 成交数量 * 成交价格
    tm: datetime.datetime  # 成交时间
    side: OrderSide  # 成交方向

    cid: str  # 柜台合同编号，应与同 qtoid 中的 cid 相一致

    fee: float = 0  # 本笔交易手续费

    def __post_init__(self):
        if isinstance(self.tm, str):
            self.tm = datetime.datetime.fromisoformat(self.tm)


@db_model("positions", "asset", (["asset", "dt"], True))
@dataclass
class PositionModel:
    dt: datetime.date
    asset: str
    shares: float
    avail: float  # 可用数量
    price: float  # 持仓价格
    profit: float  # 盈亏比
    mv: float  # 市值

    def __post_init__(self):
        if isinstance(self.dt, str):
            # 处理可能包含时间的ISO格式字符串
            if "T" in self.dt:
                self.dt = datetime.datetime.fromisoformat(self.dt).date()
            else:
                self.dt = datetime.datetime.strptime(self.dt, "%Y-%m-%d").date()
        elif isinstance(self.dt, datetime.datetime):
            self.dt = self.dt.date()


@db_model("assets", "dt", (["dt"], True))
@dataclass
class AssetModel:
    dt: datetime.date
    principal: float
    cash: float
    frozen_cash: float
    market_value: float
    total: float

    def __post_init__(self):
        if isinstance(self.dt, str):
            # 处理可能包含时间的ISO格式字符串
            if "T" in self.dt:
                self.dt = datetime.datetime.fromisoformat(self.dt).date()
            else:
                self.dt = datetime.datetime.strptime(self.dt, "%Y-%m-%d").date()
        elif isinstance(self.dt, datetime.datetime):
            self.dt = self.dt.date()
