"""sqlite 数据库封装类。开启 wal和多线程访问模式，提供写者锁。初始化数据库表。

Example:

```python
db = TradeDB("path/to/trade.db")

with db.write_lock:
    db["orders"].insert(order)
```

所有读写操作都代理给 sqlite_utils 库的 Database 对象。
"""
import threading
import sqlite3
import sqlite_utils as su
from dataclasses import asdict
from pyqmt.core.singleton import singleton
import datetime
from pyqmt.models import OrderModel, TradeModel, AssetModel, PositionModel

@singleton
class TradeDB:
    def __init__(self):
        # 创建全局写锁
        self._write_lock = threading.RLock()
    
    def init(self, db_path: str):
        # 初始化数据库连接
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.db = su.Database(self.conn)
        
        # 启用 WAL 模式提高并发读性能
        if db_path != ":memory:":
            self.db.enable_wal()
        
        # 初始化表结构
        self._init_tables()

    @property
    def write_lock(self):
        """获取写锁"""
        return self._write_lock
    
    def _init_tables(self):
        """初始化表结构
        
        在 sqlite_utils 中，创建表结构并非必须；但会导致sqlite-utils 无法准确判断类型。
        """
        with self._write_lock:
            for model in [OrderModel, TradeModel, AssetModel, PositionModel]:
                table = model.__table_name__
                pk = model.__pk__

                t: su.db.Table = self[table]
                t.create(model.to_db_schema(), pk=pk)

                if model.__indexes__ is not None:
                    indexes, is_unique = model.__indexes__
                    t.create_index(indexes, unique=is_unique)
    
    def __getitem__(self, table_name)->su.db.Table:
        """代理获取表对象"""
        return self.db[table_name] # type: ignore
    
    def __getattr__(self, name):
        """代理其他方法调用"""
        return getattr(self.db, name)
    
    def get_positions(self, dt: datetime.date) -> list[PositionModel]:
        """获取指定日期的持仓信息"""
        rows = self.db["positions"].rows_where("dt = ?", (dt, ))
        return [PositionModel(**row) for row in rows]
    
    def get_order_by_fid(self, fid: str)->OrderModel|None:
        """根据 fid 获取订单 id
        
        fid 是外部接口（比如 qmt 给出的订单 ID），而 oid 是本系统收到委托时创建的 id。
        Args:
            fid: 订单 id
        
        Returns:
            订单 id
        """
        rows = self.db["orders"].rows_where("fid = ?", (fid, ), limit=1)
        orders = list(rows)
        if len(orders) == 0:
            return None
        else:
            return OrderModel(**orders[0])
        
    def get_order(self, oid: str)->OrderModel|None:
        """根据 oid 获取订单
        
        Args:
            oid: 订单 id
        
        Returns:
            订单
        """
        rows = self.db["orders"].rows_where("oid = ?", (oid, ), limit=1)
        orders = list(rows)
        if len(orders) == 0:
            return None
        else:
            return OrderModel(**orders[0])
        
    def save_order(self, order: OrderModel):
        """保存订单"""
        with self._write_lock:
            self["orders"].upsert(asdict(order), pk = OrderModel.__pk__) # type: ignore

    def update_order(self, order: OrderModel):
        """更新订单状态
        
        我们将只改写 fid, cid, status, status_msg字段，其它字段保持不变
        """
        d = asdict(order)

        filtered = {
            "oid": d["oid"],
            "fid": d["fid"],
            "cid": d["cid"],
            "status": d["status"],
            "status_msg": d["status_msg"]
        }

        self["orders"].upsert(filtered, pk = OrderModel.__pk__) # type: ignore



db = TradeDB()

__all__ = ["db"]
