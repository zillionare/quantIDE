"""sqlite 数据库封装类及ORM。开启 wal和多线程访问模式。初始化数据库表。

本模块定义了常用的数据模型，比如 Order, Trade, Position, Asset 等。同时还通过 Model class 实现了ORM。

## 01 Model class 及数据库表定义

sqlite_utils 赋予应用无须事先创建表结构的能力，但是，为了性能和数据类型精确性考虑，手动创建数据库表是更好的方式。

Model class 自动将 dataclass 转换为数据库字段(schema)声明，从而可以用于创建数据库表结构（基于 sqlite_utils）。与 sqlalchemy 不同之处在于，我们利用 dataclass 的字段类型注解来自动推导数据库字段类型，而无须使用额外的语法。

具体的Entity 在继承 Entity 之后，可根据需要改写__post_init__方法，以完成数据库类型与 python 类型的转换。

Example:

```python
db = SQLiteDB("path/to/sqlite.db")

order = OrderModel(...)
db["orders"].insert(order)
```

所有读写操作都代理给 sqlite_utils 库的 Database 对象。

## API惯例

get_表明通过主键查询
get_*_by_*表明通过某个字段查询
*_all 表明查询所有数据

## 并发和多线程安全

数据库启用了 wal 模式，支持多进程、多线程并发读写。在高并发情况下，可能遇到 busy timeout 错误，此时需要进行重试，暂未实现。

本方案实现了一个基于线程的连接池。每一个线程都有自己的数据库连接，因此不需要锁可以在多线程环境下并发执行。在使用时无须考虑申请和释放，直接使用 db 实例对象即可。

"""

import datetime
import sqlite3
import threading
import types
import uuid
from dataclasses import asdict, dataclass, field, fields
from enum import IntEnum
from pathlib import Path
from typing import ClassVar, List, Tuple, TypeVar, Union, get_args, get_origin

import polars as pl
import sqlite_utils as su

from pyqmt.core.enums import BidType, OrderSide, OrderStatus
from pyqmt.core.singleton import singleton

T = TypeVar("T")


def new_uuid_id() -> str:
    return "qtide-" + uuid.uuid4().hex[:16]


@dataclass
class Entity:
    """模型基类，提供数据库操作方法， 提供转换为数据库 schema 字典的方法"""

    __table_name__: ClassVar[str]
    __pk__: ClassVar[str]
    __indexes__: ClassVar[Tuple[List[str], bool]]


    @classmethod
    def to_db_schema(cls) -> dict:
        """类方法：解析当前 dataclass 为 fastlite 兼容的 schema 字典"""
        schema = {}

        for f in fields(cls):
            if f.type is uuid.UUID:
                schema[f.name] = str
            elif f.type in (str, int, float, bool):
                schema[f.name] = f.type
            # 处理所有联合类型（Union[A, B] 和 A | B 语法）
            elif (
                (hasattr(f.type, "__origin__") and get_origin(f.type) is Union) or isinstance(f.type, types.UnionType)
            ):
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
            elif isinstance(f.type, type) and issubclass(f.type, IntEnum):
                schema[f.name] = int
            else:
                schema[f.name] = str
        return schema

@dataclass
class Order(Entity):
    __table_name__ = "orders"
    __pk__ = "qtoid"
    __indexes__ = (["qtoid", "tm"], True)

    asset: str  # 资产代码
    side: OrderSide
    shares: float | int  # 委托数量。调用者需要保证符合交易要求
    price: float | None
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

@dataclass
class Trade(Entity):
    __table_name__ = "trades"
    __pk__ = "tid"
    __indexes__ = (["tid", "tm"], True)
    __foreign_keys__ = [("qtoid", "orders", "qtoid")]

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

@dataclass
class Position(Entity):
    __table_name__ = "positions"
    __pk__ = "asset"
    __indexes__ = (["asset", "dt"], True)

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
            if self.dt.find("T") != -1:
                self.dt = datetime.datetime.fromisoformat(self.dt).date()
            else:
                self.dt = datetime.datetime.strptime(self.dt, "%Y-%m-%d").date()
        elif isinstance(self.dt, datetime.datetime):
            self.dt = self.dt.date()


@dataclass
class Asset(Entity):
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
            # 处理可能包含时间的ISO格式字符串
            if self.dt.find("T") != -1:
                self.dt = datetime.datetime.fromisoformat(self.dt).date()
            else:
                self.dt = datetime.datetime.strptime(self.dt, "%Y-%m-%d").date()
        elif isinstance(self.dt, datetime.datetime):
            self.dt = self.dt.date()


@singleton
class SQLiteDB:
    def __init__(self):
        # 每个线程都有自己的数据库连接
        self._thread_local = threading.local()
        self.db_path: str = ""
        self._initialized = False

    def init(self, db_path: str|Path):
        next_path = str(Path(db_path).expanduser())
        if self._initialized and self.db_path == next_path:
            return
        if self._initialized and self.db_path != next_path:
            self._thread_local = threading.local()
            self._initialized = False

        # 初始化数据库连接
        self.db_path = next_path

        conn = sqlite3.connect(self.db_path)
        db = su.Database(conn)

        # 启用 WAL 模式提高并发读性能
        if db_path != ":memory:":
            db.enable_wal()

        # 初始化表结构
        self._init_tables(db)
        conn.commit()
        conn.close()
        self._initialized = True

    @property
    def db(self) -> su.Database:
        """获取当前线程的数据库连接"""
        if not self._initialized:
            raise RuntimeError(
                "SQLiteDB has not been initialized. Call init(db_path) first."
            )

        if not hasattr(self._thread_local, "conn"):
            conn = sqlite3.connect(self.db_path, check_same_thread=True)

            # 启用外键约束
            conn.execute("PRAGMA foreign_keys = ON")

            self._thread_local.conn = conn
            self._thread_local.db = su.Database(conn)

        return self._thread_local.db

    def _init_tables(self, db: su.Database):
        """初始化表结构

        在 sqlite_utils 中，创建表结构并非必须；但会导致sqlite-utils 无法准确判断类型。
        """
        for e in [Order, Trade, Asset, Position]:
            table = e.__table_name__
            pk = e.__pk__

            t: su.db.Table = db[table]  # type: ignore
            t.create(e.to_db_schema(), pk=pk, if_not_exists=True)

            # 创建索引
            if e.__indexes__ is not None:
                indexes, is_unique = e.__indexes__
                t.create_index(indexes, unique=is_unique, if_not_exists=True)

            # 创建外键约束
            if hasattr(e, "__foreign_keys__") and e.__foreign_keys__:
                for fk in e.__foreign_keys__:
                    if len(fk) == 3:  # (from_column, to_table, to_column)
                        from_col, to_table, to_col = fk
                        t.add_foreign_key(from_col, to_table, to_col, ignore=True)

    def __getitem__(self, table_name) -> su.db.Table:
        """代理获取表对象"""
        return self.db[table_name]  # type: ignore

    def __getattr__(self, name):
        """代理其他方法调用"""
        return getattr(self.db, name)

    def upsert_positions(self, position: Position | list[Position]):
        """保存（插入和更新）持仓信息。"""
        if isinstance(position, Position):
            positions = [position]
        else:
            positions = position

        self["positions"].upsert_all([asdict(pos) for pos in positions], pk=Position.__pk__)  # type: ignore

    def get_positions(self, dt: datetime.date) -> pl.DataFrame:
        """获取指定日期的持仓信息"""
        rows = self["positions"].rows_where("dt = ?", (dt,))
        df = pl.DataFrame(rows)
        return df.with_columns(pl.col("dt").cast(pl.Date))

    def positions_all(self) -> pl.DataFrame:
        """获取所有持仓信息"""
        rows = self["positions"].rows
        df = pl.DataFrame(rows)
        return df.with_columns(pl.col("dt").cast(pl.Date))

    def insert_order(self, order: Order) -> str:
        """增加委托单（未提交）

        Args:
            order: 订单

        Returns:
            订单ID, 用于后续查询和更新。该订单 ID 为内部 id，而柜台或者第三方的 id。
        """
        self["orders"].insert(asdict(order))  # type: ignore
        return order.qtoid

    def get_order_by_foid(self, foid: str | int) -> Order | None:
        """根据 foid 获取订单

        foid 是外部接口（比如 qmt 给出的订单 ID），而 qtoid 是本系统收到委托时创建的 id。
        Args:
            foid: 订单 id

        Returns:
            订单 id
        """
        rows = self["orders"].rows_where("foid = ?", (str(foid),), limit=1)
        orders = list(rows)
        if len(orders) == 0:
            return None
        else:
            return Order(**orders[0])

    def get_order(self, qtoid: str) -> Order | None:
        """根据 qtoid 获取订单

        Args:
            qtoid: 订单 id

        Returns:
            订单
        """
        rows = self["orders"].rows_where("qtoid = ?", (qtoid,), limit=1)
        orders = list(rows)
        if len(orders) == 0:
            return None
        else:
            return Order(**orders[0])

    def query_order_by_date(self, dt: datetime.date) -> pl.DataFrame|None:
        """根据日期查询订单

        Args:
            dt: 日期

        Returns:
            订单数据框
        """
        if isinstance(dt, datetime.datetime):
            dt = dt.date()

        rows = self["orders"].rows_where(
            "tm >= ? and tm < ?", (dt, dt + datetime.timedelta(days=1))
        )
        df = pl.DataFrame(rows)
        if len(df) == 0:
            return None

        return df.with_columns(pl.col("tm").cast(pl.Datetime))

    def orders_all(self) -> pl.DataFrame:
        """获取所有订单信息"""
        rows = self["orders"].rows
        df = pl.DataFrame(rows)
        return df.with_columns(pl.col("tm").cast(pl.Datetime))

    def update_order(self, qtoid: str, **updates) -> None:
        """更新订单信息

        Args:
            oid: 订单ID
            kwupdatesargs: 更新的字段
        """
        self["orders"].update(qtoid, updates)  # type: ignore

    def insert_trades(self, trades: list[Trade] | Trade) -> None:
        """保存成交信息

        Args:
            trade: 成交信息
        """
        if isinstance(trades, Trade):
            trades = [trades]
        else:
            trades = trades

        self["trades"].insert_all([asdict(trade) for trade in trades], ignore=True)  # type: ignore

    def get_trade(self, tid: str) -> Trade | None:
        """根据 tid 获取成交

        Args:
            tid: 成交 id

        Returns:
            成交
        """
        rows = self["trades"].rows_where("tid = ?", (tid,), limit=1)
        trades = list(rows)
        if len(trades) == 0:
            return None
        else:
            return Trade(**trades[0])

    def query_trade(
        self, qtoid: str | None = None, foid: str | None = None
    ) -> pl.DataFrame|None:
        """通过 qtoid, foid 或者 tid查询成交

            Args:
                qtoid: 查询指定 qtoid 的成交
                foid: 查询指定 foid 的成交

        Returns:
            成交数据框
        """
        filters = []
        params = {"qtoid": qtoid, "foid": foid}

        for param in params:
            if params[param]:
                filters.append(f"{param} = :{param}")

        if len(filters) == 0:
            return (pl.DataFrame(self["trades"].rows)
                    .with_columns(pl.col("tm").cast(pl.Datetime))
                    )

        where_clause = " OR ".join(filters)
        rows = self["trades"].rows_where(where_clause, params)
        df = pl.DataFrame(rows)
        if len(df) == 0:
            return None

        return df.with_columns(pl.col("tm").cast(pl.Datetime))

    def trades_all(self) -> pl.DataFrame|None:
        """获取所有成交信息"""
        rows = self["trades"].rows
        df = pl.DataFrame(rows)
        if len(df) == 0:
            return None

        return df.with_columns(pl.col("tm").cast(pl.Datetime))

    def get_asset(self, dt: datetime.date) -> Asset | None:
        """通过日期(主键）查询资产信息

        Args:
            dt: 查询日期

        Returns:
            AssetModel: 资产信息
        """
        if isinstance(dt, datetime.datetime):
            dt = dt.date()
        rows = self["assets"].rows_where("dt = ?", (dt,), limit=1)
        assets = list(rows)
        if len(assets) == 0:
            return None
        else:
            return Asset(**assets[0])

    def assets_all(self) -> pl.DataFrame|None:
        """获取所有资产信息

        Returns:
            list[AssetModel]: 资产信息列表
        """
        rows = self["assets"].rows
        df = pl.DataFrame(rows)
        if len(df) == 0:
            return None

        return df.with_columns(pl.col("dt").cast(pl.Date))

    def insert_asset(self, asset: Asset) -> None:
        """保存(更新)资产信息

        Args:
            asset: 资产信息
        """
        assert asset.principal is not None, "资产信息中本金不能为空"
        self["assets"].insert(asdict(asset))

    def update_asset(self, dt: datetime.date, **updates):
        """更新资产信息

        与 save_asset 不同，本方法允许单字段更新
        """
        if isinstance(dt, datetime.datetime):
            dt = dt.date()
        self["assets"].update(dt, updates)  # type: ignore


db: SQLiteDB = SQLiteDB()

__all__ = ["db"]
