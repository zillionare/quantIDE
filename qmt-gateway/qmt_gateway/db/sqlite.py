"""SQLite 数据库封装类

提供线程安全的 SQLite 数据库访问，支持 WAL 模式和多线程并发读写。
"""

import sqlite3
import threading
from pathlib import Path
from typing import Any

import sqlite_utils as su
from loguru import logger

from qmt_gateway.db.models import (
    Asset,
    Order,
    Portfolio,
    Position,
    Settings,
    SyncLog,
    Trade,
    User,
)


class SQLiteDB:
    """SQLite 数据库单例类

    使用线程本地存储实现多线程安全，每个线程有自己的数据库连接。
    启用 WAL 模式提高并发性能。
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._thread_local = threading.local()
        self.db_path: str = ""
        self._initialized = False

    def init(self, db_path: str | Path):
        """初始化数据库

        Args:
            db_path: 数据库文件路径
        """
        next_path = str(Path(db_path).expanduser())
        if self._initialized and self.db_path == next_path and next_path != ":memory:":
            return

        # 强制重置连接
        self._thread_local = threading.local()
        self._initialized = False

        # 初始化数据库连接
        self.db_path = next_path

        # 确保目录存在
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

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
        logger.info(f"数据库初始化完成: {self.db_path}")

    @property
    def db(self) -> su.Database:
        """获取当前线程的数据库连接"""
        if not self._initialized:
            raise RuntimeError("数据库未初始化，请先调用 init(db_path)")

        if not hasattr(self._thread_local, "conn"):
            conn = sqlite3.connect(self.db_path, check_same_thread=True)
            # 启用外键约束
            conn.execute("PRAGMA foreign_keys = ON")
            self._thread_local.conn = conn
            self._thread_local.db = su.Database(conn)

        return self._thread_local.db

    def _init_tables(self, db: su.Database):
        """初始化表结构"""
        entities = [
            User,
            Settings,
            Portfolio,
            SyncLog,
            Order,
            Trade,
            Position,
            Asset,
        ]

        for e in entities:
            table = e.__table_name__
            pk = e.__pk__

            t: su.db.Table = db[table]  # type: ignore
            schema = e.to_db_schema()
            t.create(schema, pk=pk, if_not_exists=True)

            # 添加缺失的列
            for col, typ in schema.items():
                if col not in t.columns_dict:
                    t.add_column(col, typ)

            # 创建索引
            if e.__indexes__ is not None:
                indexes, is_unique = e.__indexes__
                if indexes:
                    t.create_index(indexes, unique=is_unique, if_not_exists=True)

            # 创建外键约束
            if hasattr(e, "__foreign_keys__") and e.__foreign_keys__:
                for fk in e.__foreign_keys__:
                    if len(fk) == 3:
                        from_col, to_table, to_col = fk
                        t.add_foreign_key(from_col, to_table, to_col, ignore=True)

        # 初始化默认设置
        self._init_default_settings(db)

    def _init_default_settings(self, db: su.Database):
        """初始化默认设置"""
        try:
            db["settings"].insert(Settings().to_dict(), pk="id", ignore=True)
        except Exception:
            pass

    def __getitem__(self, table_name) -> su.db.Table:
        """代理获取表对象"""
        return self.db[table_name]  # type: ignore

    def __getattr__(self, name):
        """代理其他方法调用"""
        return getattr(self.db, name)

    def get_settings(self) -> Settings:
        """获取系统设置"""
        try:
            row = self["settings"].get(1)
            return Settings.from_dict(dict(row))
        except Exception:
            return Settings()

    def save_settings(self, settings: Settings) -> None:
        """保存系统设置"""
        settings.updated_at = __import__("datetime").datetime.now()
        self["settings"].upsert(settings.to_dict(), pk="id")

    def get_user(self, username: str) -> User | None:
        """根据用户名获取用户"""
        rows = list(self["users"].rows_where("username = ?", (username,)))
        if len(rows) == 0:
            return None
        return User.from_dict(dict(rows[0]))

    def save_user(self, user: User) -> None:
        """保存用户信息"""
        user.updated_at = __import__("datetime").datetime.now()
        self["users"].upsert(user.to_dict(), pk="id")

    def get_order(self, qtoid: str) -> Order | None:
        """根据ID获取订单"""
        try:
            row = self["orders"].get(qtoid)
            return Order.from_dict(dict(row))
        except Exception:
            return None

    def get_order_by_foid(self, foid: str) -> Order | None:
        """根据代理订单ID获取订单"""
        rows = list(self["orders"].rows_where("foid = ?", (foid,)))
        if len(rows) == 0:
            return None
        return Order.from_dict(dict(rows[0]))

    def insert_order(self, order: Order) -> None:
        """插入订单"""
        self["orders"].insert(order.to_dict(), pk=Order.__pk__, ignore=True)

    def update_order(self, qtoid: str, **kwargs) -> None:
        """更新订单"""
        self["orders"].update(qtoid, kwargs)

    def get_orders(self, portfolio_id: str | None = None, status: Any = None, start: Any = None, end: Any = None) -> list[Order]:
        """获取订单列表"""
        where_clauses = []
        params = []

        if portfolio_id:
            where_clauses.append("portfolio_id = ?")
            params.append(portfolio_id)

        if status is not None:
            where_clauses.append("status = ?")
            params.append(int(status))

        if start:
            where_clauses.append("tm >= ?")
            params.append(start)

        if end:
            where_clauses.append("tm <= ?")
            params.append(end)

        where = " AND ".join(where_clauses) if where_clauses else None

        rows = self["orders"].rows_where(where, params)
        return [Order.from_dict(dict(row)) for row in rows]

    def get_trade(self, tid: str) -> Trade | None:
        """根据ID获取成交"""
        try:
            row = self["trades"].get(tid)
            return Trade.from_dict(dict(row))
        except Exception:
            return None

    def insert_trade(self, trade: Trade) -> None:
        """插入成交"""
        self["trades"].insert(trade.to_dict(), pk=Trade.__pk__, ignore=True)

    def get_trades(self, portfolio_id: str | None = None, start: Any = None, end: Any = None) -> list[Trade]:
        """获取成交列表"""
        where_clauses = []
        params = []

        if portfolio_id:
            where_clauses.append("portfolio_id = ?")
            params.append(portfolio_id)

        if start:
            where_clauses.append("tm >= ?")
            params.append(start)

        if end:
            where_clauses.append("tm <= ?")
            params.append(end)

        where = " AND ".join(where_clauses) if where_clauses else None

        rows = self["trades"].rows_where(where, params)
        return [Trade.from_dict(dict(row)) for row in rows]

    def get_trades_by_qtoid(self, qtoid: str) -> list[Trade]:
        """根据订单ID获取成交列表"""
        rows = self["trades"].rows_where("qtoid = ?", (qtoid,))
        return [Trade.from_dict(dict(row)) for row in rows]

    def get_position(self, portfolio_id: str, asset: str, dt: Any) -> Position | None:
        """获取持仓"""
        try:
            row = self["positions"].get((portfolio_id, dt, asset))
            return Position.from_dict(dict(row))
        except Exception:
            return None

    def get_positions(self, portfolio_id: str, dt: Any) -> list[Position]:
        """获取持仓列表"""
        rows = self["positions"].rows_where("portfolio_id = ? AND dt = ?", (portfolio_id, dt))
        return [Position.from_dict(dict(row)) for row in rows]

    def insert_position(self, position: Position) -> None:
        """插入持仓"""
        self["positions"].insert(position.to_dict(), pk=Position.__pk__, ignore=True)

    def get_asset(self, portfolio_id: str, dt: Any) -> Asset | None:
        """获取资金"""
        try:
            row = self["assets"].get((portfolio_id, dt))
            return Asset.from_dict(dict(row))
        except Exception:
            return None

    def insert_asset(self, asset: Asset) -> None:
        """插入资金"""
        self["assets"].insert(asset.to_dict(), pk=Asset.__pk__, ignore=True)

    def get_portfolio(self, portfolio_id: str) -> Portfolio | None:
        """获取投资组合"""
        try:
            row = self["portfolios"].get(portfolio_id)
            return Portfolio.from_dict(dict(row))
        except Exception:
            return None

    def insert_portfolio(self, portfolio: Portfolio) -> None:
        """插入投资组合"""
        self["portfolios"].insert(portfolio.to_dict(), pk=Portfolio.__pk__, ignore=True)


# 全局数据库实例
db = SQLiteDB()
