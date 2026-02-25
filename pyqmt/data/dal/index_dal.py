"""指数数据访问层"""

import datetime

import polars as pl
from loguru import logger

from pyqmt.data.models.index import Index, IndexBar
from pyqmt.data.sqlite import SQLiteDB


class IndexDAL:
    """指数数据访问层"""

    def __init__(self, db: SQLiteDB):
        self.db = db

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

    def save_index_bars(self, bars: list[IndexBar]) -> int:
        """保存指数行情数据

        Args:
            bars: 行情数据列表

        Returns:
            保存的记录数
        """
        if not bars:
            return 0

        self.db["index_bars"].insert_all(
            [bar.to_dict() for bar in bars], pk=IndexBar.__pk__, replace=True
        )
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
            行情数据DataFrame
        """
        rows = self.db["index_bars"].rows_where(
            "symbol = ? AND dt >= ? AND dt <= ?",
            (symbol, start, end),
            order_by="dt",
        )

        if not rows:
            return pl.DataFrame()

        df = pl.DataFrame(rows)
        return df.with_columns(pl.col("dt").cast(pl.Date))
