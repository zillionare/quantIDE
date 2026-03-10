"""指数数据访问层

指数基本信息保存在 SQLite 的 indices 表中。
指数行情数据保存在 Parquet 文件中（通过 IndexBars）。
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import polars as pl
from loguru import logger

from pyqmt.data.models.index import Index, IndexBar
from pyqmt.data.sqlite import SQLiteDB

if TYPE_CHECKING:
    from pyqmt.data.models.index_bars import IndexBars


class IndexDAL:
    """指数数据访问层

    指数基本信息：SQLite (indices 表)
    指数行情数据：Parquet 文件 (通过 IndexBars)
    """

    def __init__(self, db: SQLiteDB, index_bars: "IndexBars" | None = None):
        """初始化指数数据访问层

        Args:
            db: SQLite 数据库实例
            index_bars: IndexBars 实例，用于访问行情数据
        """
        self.db = db
        self._index_bars = index_bars

    @property
    def index_bars(self) -> "IndexBars":
        """获取 IndexBars 实例"""
        if self._index_bars is None:
            raise RuntimeError("IndexBars 未初始化")
        return self._index_bars

    def set_index_bars(self, index_bars: "IndexBars") -> None:
        """设置 IndexBars 实例

        Args:
            index_bars: IndexBars 实例
        """
        self._index_bars = index_bars

    # ========== 指数基本信息操作（SQLite） ==========

    def create_index(self, index: Index) -> Index:
        """创建指数记录

        Args:
            index: 指数对象

        Returns:
            创建后的指数对象
        """
        index.updated_at = datetime.datetime.now()
        self.db["indices"].insert(index.to_dict(), pk=Index.__pk__)
        return index

    def get_index(self, symbol: str) -> Index | None:
        """获取指数

        Args:
            symbol: 指数代码

        Returns:
            指数对象，不存在则返回None
        """
        row = self.db["indices"].get(symbol)
        if row:
            return Index(**row)
        return None

    def list_indices(
        self, index_type: str | None = None, category: str | None = None
    ) -> list[Index]:
        """列出指数

        Args:
            index_type: 指数类型过滤：market/industry/concept
            category: 分类过滤

        Returns:
            指数列表
        """
        table = self.db["indices"]

        if index_type and category:
            rows = table.rows_where(
                "index_type = ? AND category = ?", (index_type, category)
            )
        elif index_type:
            rows = table.rows_where("index_type = ?", (index_type,))
        elif category:
            rows = table.rows_where("category = ?", (category,))
        else:
            rows = table.rows

        return [Index(**row) for row in rows]

    def update_index(self, index: Index) -> Index:
        """更新指数

        Args:
            index: 指数对象

        Returns:
            更新后的指数对象
        """
        index.updated_at = datetime.datetime.now()
        self.db["indices"].update(index.symbol, index.to_dict())
        return index

    def delete_index(self, symbol: str) -> bool:
        """删除指数

        Args:
            symbol: 指数代码

        Returns:
            是否删除成功
        """
        try:
            self.db["indices"].delete(symbol)
            return True
        except Exception as e:
            logger.error(f"删除指数失败: {e}")
            return False

    def upsert_indices(self, indices: list[Index]) -> int:
        """批量插入或更新指数

        Args:
            indices: 指数对象列表

        Returns:
            保存的记录数
        """
        if not indices:
            return 0

        now = datetime.datetime.now()
        for index in indices:
            index.updated_at = now

        self.db["indices"].upsert_all(
            [index.to_dict() for index in indices], pk=Index.__pk__
        )
        return len(indices)

    # ========== 指数行情数据操作（Parquet） ==========

    def save_index_bars(self, bars: list[IndexBar]) -> int:
        """保存指数行情数据

        Args:
            bars: 行情数据列表

        Returns:
            保存的记录数
        """
        if not bars:
            return 0

        # 转换为 DataFrame 并追加到 Parquet
        df = pl.DataFrame([
            {
                "symbol": bar.symbol,
                "date": bar.dt,
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "amount": bar.amount,
            }
            for bar in bars
        ])

        self.index_bars.append_data(df)
        return len(bars)

    def get_index_bars(
        self,
        symbol: str,
        start: datetime.date,
        end: datetime.date,
    ) -> pl.DataFrame:
        """获取指数行情数据

        Args:
            symbol: 指数代码
            start: 开始日期
            end: 结束日期

        Returns:
            行情数据 DataFrame
        """
        df = self.index_bars.get_bars_in_range(
            start=start,
            end=end,
            symbols=[symbol],
            eager_mode=True,
        )

        if df.is_empty():
            return pl.DataFrame(schema={
                "date": pl.Date,
                "symbol": pl.Utf8,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "amount": pl.Float64,
            })

        return df

    def get_index_bars_all(
        self,
        start: datetime.date,
        end: datetime.date,
    ) -> pl.DataFrame:
        """获取所有指数的行情数据

        Args:
            start: 开始日期
            end: 结束日期

        Returns:
            行情数据 DataFrame
        """
        return self.index_bars.get_bars_in_range(
            start=start,
            end=end,
            symbols=None,
            eager_mode=True,
        )
