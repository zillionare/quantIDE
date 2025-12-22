"""Modles 定义

本模块定义了常用的数据模型，比如 Order, Trade, Position, Asset 等。同时还提供了一些工具函数，比如将 dataclass 转换为 fastlite 兼容的 schema 字典。

## db_model

自动为 dataclass 注入表名、主键、索引等元数据。

## _dataclass_to_schema

sqlite_utils 赋予应用无须事先创建表结构的能力，但是，为了性能和数据类型精确性考虑，手动创建数据库表是更好的方式。

`_dataclass_to_schema` 自动将 dataclass 转换为数据库字段声明，从而可以用于创建数据库表结构（基于 sqlite_utils）。

Example:

```python
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
import datetime
import uuid
from pyqmt.core.enums import OrderSide, BidType, OrderStatus

T = TypeVar("T")

def _dataclass_to_schema(cls) -> dict:
    """类方法：解析当前 dataclass 为 fastlite 兼容的 schema 字典"""
    schema = {}

    for f in fields(cls):
        if f.type is uuid.UUID:
            schema[f.name] = str
        elif f.type in (str, int, float, bool):
            schema[f.name] = f.type
        elif getattr(f.type, "__origin__", None) is type(None) or (
            hasattr(f.type, "__args__") and type(None) in f.type.__args__
        ):
            base_type = [t for t in f.type.__args__ if t is not type(None)][0]
            schema[f.name] = base_type if base_type in (str, int, float, bool) else str
        else:
            schema[f.name] = str
    return schema

def db_model(table_name: str, pk: str, indexes:tuple[list[str], bool]):
    def wrapper(cls: Type[T])->Type[T]:
        setattr(cls, "__table_name__", table_name)
        setattr(cls, "__pk__", pk)
        setattr(cls, "__indexes__", indexes or []) # 例如 (["oid", "fid"], True)

        if not hasattr(cls, 'to_db_schema'):
            @classmethod
            def to_db_schema(cls_inner):
                return _dataclass_to_schema(cls_inner)
            
            cls.to_db_schema = to_db_schema
        return cls
    return wrapper


@db_model("orders", "oid", (["oid", "tm"], True))
@dataclass
class OrderModel:
    asset: str                              # 资产代码
    side: OrderSide
    shares: float|int                       # 委托数量。调用者需要保证符合交易要求
    price: float
    bid_type: BidType                       # 委托类型，比如限价单、市价单  
    tm: datetime.datetime|None = None       # 下单时间

    fid: str|None = None                    # 外部接口(比如QMT)指定的 id，透传，一般用以查错
    cid: str|None = None                    # 券商柜台合约 id
    status: OrderStatus = OrderStatus.UNREPORTED # 委托状态，比如未报、待报、已报、部成等
    status_msg: str = ""                    # 委托状态描述，比如废单原因

    # 本委托 ID, pk
    oid: str = field(default_factory=lambda: str(uuid.uuid4()))
    strategy: str = ""                   # 策略名称
    remark: str = ""                     # 备注

    @classmethod
    def to_db_schema(cls)->dict:
        schema = _dataclass_to_schema(cls)

        # 修正无法自动转换的类型
        schema["status"] = int
        schema["bid_type"] = int
        schema["fid"] = str
        return schema
    
    def __post_init__(self):
        if isinstance(self.status, int):
            self.status = OrderStatus(self.status)
        if isinstance(self.bid_type, int):
            self.bid_type = BidType(self.bid_type)
        if isinstance(self.side, int):
            self.side = OrderSide(self.side)


@db_model("trades", "tid", (["tid", "tm"], True))
@dataclass
class TradeModel:
    oid: str                            # 对应的 Order id      
    asset: str                          # 资产代码    
    shares: float|int                   # 成交数量
    price: float                        # 成交价格
    amount: float                       # 成交金额 = 成交数量 * 成交价格
    tm: datetime.datetime               # 成交时间
    side: OrderSide                     # 成交方向，在 qmt 中是order_type

    fid: str                            # 外部接口(比如QMT)指定的 traded_id
    cid: str                            # 柜台合同编号，应与同 oid 中的 cid 相一致

    fee: float = 0                      # 本笔交易手续费
    # 本次 fill(成交) ID
    tid: str = field(default_factory=lambda: str(uuid.uuid4())) 

    @classmethod
    def to_db_schema(cls)->dict:
        schema = _dataclass_to_schema(cls)

        # 修正无法自动转换的类型
        schema["side"] = int
        schema["tm"] = str
        return schema
    
    def __post_init__(self):
        if isinstance(self.side, int):
            self.side = OrderSide(self.side)
    
    


@db_model("positions", "asset", (["asset", "dt"], True))
@dataclass
class PositionModel:
    dt: datetime.date
    asset: str
    shares: float|int
    avail: float|int                      # 可用数量
    price: float                          # 持仓价格

    @classmethod
    def to_db_schema(cls)->dict:
        schema = _dataclass_to_schema(cls)

        # 修正无法自动转换的类型
        schema["dt"] = str
        return schema


@db_model("assets", "dt", (["dt"], True))
@dataclass
class AssetModel:
    dt: datetime.date
    principal: float
    cash: float
    frozen_cash: float
    market_value: float
    total: float

    @classmethod
    def to_db_schema(cls)->dict:
        schema = _dataclass_to_schema(cls)

        # 修正无法自动转换的类型
        schema["dt"] = str
        return schema

