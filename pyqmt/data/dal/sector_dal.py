"""板块数据访问层"""

import datetime
from pathlib import Path

import polars as pl
from loguru import logger

from pyqmt.data.models.sector import Sector, SectorBar, SectorConstituent
from pyqmt.data.sqlite import SQLiteDB


class SectorDAL:
    """板块数据访问层，支持PIT（Point In Time）查询"""

    def __init__(self, db: SQLiteDB):
        self.db = db

    def create_sector(self, sector: Sector) -> Sector:
        """创建板块

        Args:
            sector: 板块对象

        Returns:
            创建后的板块对象
        """
        self.db["sectors"].insert(sector.to_dict(), pk=Sector.__pk__)
        return sector

    def create_sectors_batch(self, sectors: list[Sector]) -> int:
        """批量创建板块

        Args:
            sectors: 板块对象列表

        Returns:
            创建的记录数
        """
        if not sectors:
            return 0

        self.db["sectors"].insert_all(
            [s.to_dict() for s in sectors],
            pk=Sector.__pk__,
            replace=True,
        )
        return len(sectors)

    def get_sector(self, sector_id: str, trade_date: datetime.date) -> Sector | None:
        """获取特定日期的板块

        Args:
            sector_id: 板块ID
            trade_date: 数据日期

        Returns:
            板块对象，不存在则返回None
        """
        row = self.db["sectors"].get((sector_id, trade_date))
        if row:
            return Sector(**row)
        return None

    def list_sectors(
        self,
        sector_type: str | None = None,
        trade_date: datetime.date | None = None,
    ) -> list[Sector]:
        """列出板块

        Args:
            sector_type: 板块类型过滤
            trade_date: 数据日期，默认为最新日期

        Returns:
            板块列表
        """
        table = self.db["sectors"]

        if trade_date is None:
            # 获取最新日期的板块
            if sector_type:
                rows = table.rows_where(
                    "sector_type = ?",
                    (sector_type,),
                )
            else:
                rows = table.rows
        else:
            if sector_type:
                rows = table.rows_where(
                    "sector_type = ? AND trade_date = ?",
                    (sector_type, trade_date),
                )
            else:
                rows = table.rows_where(
                    "trade_date = ?",
                    (trade_date,),
                )

        return [Sector(**row) for row in rows]

    def get_sectors_by_date(self, trade_date: datetime.date) -> pl.DataFrame:
        """获取指定日期的所有板块

        Args:
            trade_date: 数据日期

        Returns:
            板块DataFrame
        """
        rows = list(self.db["sectors"].rows_where(
            "trade_date = ?",
            (trade_date,),
        ))

        if not rows:
            return pl.DataFrame(schema={
                "id": pl.Utf8,
                "name": pl.Utf8,
                "sector_type": pl.Utf8,
                "source": pl.Utf8,
                "trade_date": pl.Date,
                "description": pl.Utf8,
            })

        return pl.DataFrame(rows)

    def delete_sectors_by_date(self, trade_date: datetime.date) -> int:
        """删除指定日期的板块数据

        Args:
            trade_date: 数据日期

        Returns:
            删除的记录数
        """
        try:
            cursor = self.db.execute(
                "DELETE FROM sectors WHERE trade_date = ?",
                (trade_date,),
            )
            return cursor.rowcount
        except Exception as e:
            logger.error(f"删除板块数据失败: {e}")
            return 0

    def add_constituent(self, constituent: SectorConstituent) -> bool:
        """添加板块成分股

        Args:
            constituent: 成分股对象

        Returns:
            是否添加成功
        """
        try:
            self.db["sector_constituents"].insert(
                constituent.to_dict(),
                pk=SectorConstituent.__pk__,
            )
            return True
        except Exception as e:
            logger.error(f"添加板块成分股失败: {e}")
            return False

    def add_constituents_batch(self, constituents: list[SectorConstituent]) -> int:
        """批量添加板块成分股

        Args:
            constituents: 成分股对象列表

        Returns:
            添加的记录数
        """
        if not constituents:
            return 0

        try:
            self.db["sector_constituents"].insert_all(
                [c.to_dict() for c in constituents],
                pk=SectorConstituent.__pk__,
                replace=True,
            )
            return len(constituents)
        except Exception as e:
            logger.error(f"批量添加板块成分股失败: {e}")
            return 0

    def get_constituents(
        self,
        sector_id: str,
        trade_date: datetime.date,
    ) -> list[SectorConstituent]:
        """获取板块成分股

        Args:
            sector_id: 板块ID
            trade_date: 数据日期

        Returns:
            成分股列表
        """
        rows = self.db["sector_constituents"].rows_where(
            "sector_id = ? AND trade_date = ?",
            (sector_id, trade_date),
            order_by="symbol",
        )
        return [SectorConstituent(**row) for row in rows]

    def get_constituents_df(
        self,
        sector_id: str,
        trade_date: datetime.date,
    ) -> pl.DataFrame:
        """获取板块成分股DataFrame

        Args:
            sector_id: 板块ID
            trade_date: 数据日期

        Returns:
            成分股DataFrame
        """
        rows = list(self.db["sector_constituents"].rows_where(
            "sector_id = ? AND trade_date = ?",
            (sector_id, trade_date),
            order_by="symbol",
        ))

        if not rows:
            return pl.DataFrame(schema={
                "sector_id": pl.Utf8,
                "trade_date": pl.Date,
                "symbol": pl.Utf8,
                "name": pl.Utf8,
                "weight": pl.Float64,
            })

        return pl.DataFrame(rows)

    def delete_constituents_by_date(self, trade_date: datetime.date) -> int:
        """删除指定日期的成分股数据

        Args:
            trade_date: 数据日期

        Returns:
            删除的记录数
        """
        try:
            cursor = self.db.execute(
                "DELETE FROM sector_constituents WHERE trade_date = ?",
                (trade_date,),
            )
            return cursor.rowcount
        except Exception as e:
            logger.error(f"删除成分股数据失败: {e}")
            return 0

    def get_stock_sectors(
        self,
        symbol: str,
        trade_date: datetime.date,
    ) -> list[Sector]:
        """获取个股所属板块

        Args:
            symbol: 股票代码
            trade_date: 数据日期

        Returns:
            板块列表
        """
        rows = list(self.db["sector_constituents"].rows_where(
            "symbol = ? AND trade_date = ?",
            (symbol, trade_date),
        ))

        sectors = []
        for row in rows:
            sector_id = row["sector_id"]
            sector = self.get_sector(sector_id, trade_date)
            if sector:
                sectors.append(sector)

        return sectors

    def save_sector_bars(self, bars: list[SectorBar]) -> int:
        """保存板块行情数据

        Args:
            bars: 行情数据列表

        Returns:
            保存的记录数
        """
        if not bars:
            return 0

        self.db["sector_bars"].insert_all(
            [bar.to_dict() for bar in bars],
            pk=SectorBar.__pk__,
            replace=True,
        )
        return len(bars)

    def get_sector_bars(
        self,
        sector_id: str,
        start: datetime.date,
        end: datetime.date,
    ) -> pl.DataFrame:
        """获取板块行情数据

        Args:
            sector_id: 板块ID
            start: 开始日期
            end: 结束日期

        Returns:
            行情数据DataFrame
        """
        rows = list(self.db["sector_bars"].rows_where(
            "sector_id = ? AND dt >= ? AND dt <= ?",
            (sector_id, start, end),
            order_by="dt",
        ))

        if not rows:
            return pl.DataFrame(schema={
                "dt": pl.Date,
                "sector_id": pl.Utf8,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "amount": pl.Float64,
            })

        df = pl.DataFrame(rows)
        return df.with_columns(pl.col("dt").cast(pl.Date))

    def get_sector_bars_by_date(
        self,
        trade_date: datetime.date,
    ) -> pl.DataFrame:
        """获取指定日期的所有板块行情

        Args:
            trade_date: 交易日期

        Returns:
            行情数据DataFrame
        """
        rows = list(self.db["sector_bars"].rows_where(
            "dt = ?",
            (trade_date,),
        ))

        if not rows:
            return pl.DataFrame(schema={
                "dt": pl.Date,
                "sector_id": pl.Utf8,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "amount": pl.Float64,
            })

        df = pl.DataFrame(rows)
        return df.with_columns(pl.col("dt").cast(pl.Date))
