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
    Entity,
    Order,
    Position,
    Sector,
    SectorBar,
    SectorConstituent,
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
            Sector,
            SectorConstituent,
            SectorBar,
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

    def get_sector(self, sector_id: str) -> Sector | None:
        """根据ID获取板块"""
        try:
            row = self["sectors"].get(sector_id)
            return Sector.from_dict(dict(row))
        except Exception:
            return None

    def list_sectors(self, sector_type: str | None = None, trade_date: Any = None) -> list[Sector]:
        """获取板块列表"""
        where_clauses = []
        params = []

        if sector_type:
            where_clauses.append("sector_type = ?")
            params.append(sector_type)

        if trade_date:
            where_clauses.append("trade_date = ?")
            params.append(trade_date)

        where = " AND ".join(where_clauses) if where_clauses else None

        rows = self["sectors"].rows_where(where, params)
        return [Sector.from_dict(dict(row)) for row in rows]

    def get_sector_constituents(self, sector_id: str, trade_date: Any) -> list[SectorConstituent]:
        """获取板块成分股"""
        rows = self["sector_constituents"].rows_where(
            "sector_id = ? AND trade_date = ?",
            (sector_id, trade_date),
        )
        return [SectorConstituent.from_dict(dict(row)) for row in rows]

    def list_constituents(self, sector_id: str, trade_date: Any = None) -> list[SectorConstituent]:
        """获取板块成分股列表（简化版，使用最新日期）"""
        if trade_date is None:
            # 获取最新交易日
            row = self.db.execute(
                "SELECT MAX(trade_date) as max_date FROM sector_constituents WHERE sector_id = ?",
                (sector_id,)
            ).fetchone()
            if row and row["max_date"]:
                trade_date = row["max_date"]
            else:
                return []
        
        return self.get_sector_constituents(sector_id, trade_date)

    def insert_sectors(self, sectors: list[Sector]) -> None:
        """批量插入板块"""
        if not sectors:
            return
        self["sectors"].insert_all(
            [s.to_dict() for s in sectors],
            pk=Sector.__pk__,
            ignore=True,
        )

    def insert_constituents(self, constituents: list[SectorConstituent]) -> None:
        """批量插入成分股"""
        if not constituents:
            return
        self["sector_constituents"].insert_all(
            [c.to_dict() for c in constituents],
            pk=SectorConstituent.__pk__,
            ignore=True,
        )


# 全局数据库实例
db = SQLiteDB()
