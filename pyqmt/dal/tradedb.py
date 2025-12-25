"""sqlite 数据库封装类。开启 wal和多线程访问模式。初始化数据库表。

每一个线程都有自己的数据库连接，因此不需要锁可以在多线程环境下并发执行。

Example:

```python
db = TradeDB("path/to/trade.db")

db["orders"].insert(order)
```

所有读写操作都代理给 sqlite_utils 库的 Database 对象。
"""

import threading
from typing import Any
import datetime
import sqlite3
import sqlite_utils as su
from dataclasses import asdict
from pyqmt.core.enums import OrderSide
from pyqmt.core.singleton import singleton
from pyqmt.models import BidType, OrderModel, TradeModel, AssetModel, PositionModel


@singleton
class TradeDB:
    def __init__(self):
        # 每个线程都有自己的数据库连接
        self._thread_local = threading.local()
        self.db_path: str = ""
        self._initialized = False

    def init(self, db_path: str):
        if self._initialized:
            return

        # 初始化数据库连接
        self.db_path = db_path

        conn = sqlite3.connect(db_path)
        db = su.Database(conn)

        # 启用 WAL 模式提高并发读性能
        if db_path != ":memory:":
            db.enable_wal()

        # 初始化表结构
        self._init_tables(db)
        conn.close()
        self._initialized = True

    @property
    def db(self) -> su.Database:
        """获取当前线程的数据库连接"""
        if not self._initialized:
            raise RuntimeError(
                "TradeDB has not been initialized. Call init(db_path) first."
            )

        if not hasattr(self._thread_local, "conn"):
            conn = sqlite3.connect(self.db_path, check_same_thread=True)

            self._thread_local.conn = conn
            self._thread_local.db = su.Database(conn)

        return self._thread_local.db

    def _init_tables(self, db: su.Database):
        """初始化表结构

        在 sqlite_utils 中，创建表结构并非必须；但会导致sqlite-utils 无法准确判断类型。
        """
        for model in [OrderModel, TradeModel, AssetModel, PositionModel]:
            table = model.__table_name__
            pk = model.__pk__

            t: su.db.Table = db[table]  # type: ignore
            t.create(model.to_db_schema(), pk=pk)

            if model.__indexes__ is not None:
                indexes, is_unique = model.__indexes__
                t.create_index(indexes, unique=is_unique)

    def __getitem__(self, table_name) -> su.db.Table:
        """代理获取表对象"""
        return self.db[table_name]  # type: ignore

    def __getattr__(self, name):
        """代理其他方法调用"""
        return getattr(self.db, name)

    def get_positions(self, dt: datetime.date) -> list[PositionModel]:
        """获取指定日期的持仓信息"""
        rows = self.db["positions"].rows_where("dt = ?", (dt,))
        return [PositionModel(**row) for row in rows]

    def get_order_by_foid(self, foid: str | int) -> OrderModel | None:
        """根据 foid 获取订单

        foid 是外部接口（比如 qmt 给出的订单 ID），而 qtoid 是本系统收到委托时创建的 id。
        Args:
            foid: 订单 id

        Returns:
            订单 id
        """
        rows = self.db["orders"].rows_where("foid = ?", (str(foid),), limit=1)
        orders = list(rows)
        if len(orders) == 0:
            return None
        else:
            return OrderModel(**orders[0])

    def get_order(self, qtoid: str) -> OrderModel | None:
        """根据 qtoid 获取订单

        Args:
            qtoid: 订单 id

        Returns:
            订单
        """
        rows = self.db["orders"].rows_where("qtoid = ?", (qtoid,), limit=1)
        orders = list(rows)
        if len(orders) == 0:
            return None
        else:
            return OrderModel(**orders[0])

    def save_order(self, asset: str, price: int | float,
        shares: int | float,
        side: OrderSide,
        bid_type: BidType,
        strategy: str = "",
        bid_time: datetime.datetime | None = None,
        qtoid: str | None = None,
        foid: Any | None = None)->str:
        """保存委托单（未提交）

        Args:
            asset: 资产代码, "symbol.SZ"风格
            price: 委托价格
            shares: 委托数量
            strategy: 策略名称
            bid_time: 下单时间，实盘时可省略传入，测试

        Returns:
            订单ID, 用于后续查询和更新。该订单 ID 为内部 id，而柜台或者第三方的 id。
        """
        order = OrderModel(
            asset = asset,
            price = price,
            shares = shares,
            side = side,
            bid_type = bid_type,
            strategy = strategy,
            tm = bid_time or datetime.datetime.now(),
            foid = foid
        )

        if qtoid is not None:
            order.qtoid = qtoid

        db["orders"].insert(asdict(order)) # type: ignore
        return order.qtoid

    def update_order(self, qtoid: str, **updates)->None:
        """更新订单信息

        Args:
            oid: 订单ID
            kwupdatesargs: 更新的字段
        """
        self["orders"].update(qtoid, updates) # type: ignore

    def get_trade(self, tid: str) -> TradeModel | None:
        """根据 tid 获取成交

        Args:
            tid: 成交 id

        Returns:
            成交
        """
        rows = self.db["trades"].rows_where("tid = ?", (tid,), limit=1)
        trades = list(rows)
        if len(trades) == 0:
            return None
        else:
            return TradeModel(**trades[0])
    def query_trade(
        self, qtoid: str | None = None, foid: str | None = None
    ) -> list[TradeModel]:
        """通过 qtoid, foid 或者 tid查询成交

            Args:
                qtoid: 查询指定 qtoid 的成交
                foid: 查询指定 foid 的成交

        Returns:
            list[TradeModel]: 成交数据列表
        """
        filters = []
        params = {
            "qtoid": qtoid,
            "foid": foid
        }

        for param in params:
            if params[param]:
                filters.append(f"{param} = :{param}")

        if len(filters) == 0:
            return [TradeModel(**row) for row in db["trades"].rows]

        where_clause = " OR ".join(filters)
        rows = self["trades"].rows_where(where_clause, params)
        return [TradeModel(**row) for row in rows]
    
    def save_trades(self, trades: list[TradeModel]|TradeModel)->None:
        """保存成交信息

        Args:
            trade: 成交信息
        """
        if isinstance(trades, TradeModel):
            trades = [trades]
        else:
            trades = trades

        self["trades"].insert_all([asdict(trade) for trade in trades], ignore=True)


db = TradeDB()

__all__ = ["db"]
