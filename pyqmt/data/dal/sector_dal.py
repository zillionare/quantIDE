"""板块数据访问层"""

import datetime
from pathlib import Path

import polars as pl
from loguru import logger

from pyqmt.data.models.sector import Sector, SectorBar, SectorStock
from pyqmt.data.sqlite import SQLiteDB


class SectorDAL:
    """板块数据访问层"""

    def __init__(self, db: SQLiteDB):
        self.db = db

    def create_sector(self, sector: Sector) -> Sector:
        """创建板块

        Args:
            sector: 板块对象

        Returns:
            创建后的板块对象
        """
        sector.updated_at = datetime.datetime.now()
        self.db["sectors"].insert(sector.to_dict(), pk=Sector.__pk__)
        return sector

    def get_sector(self, sector_id: str) -> Sector | None:
        """获取板块

        Args:
            sector_id: 板块ID

        Returns:
            板块对象，不存在则返回None
        """
        row = self.db["sectors"].get(sector_id)
        if row:
            return Sector(**row)
        return None

    def list_sectors(
        self, sector_type: str | None = None, source: str | None = None
    ) -> list[Sector]:
        """列出板块

        Args:
            sector_type: 板块类型过滤：custom/industry/concept
            source: 来源过滤：user/tushare

        Returns:
            板块列表
        """
        table = self.db["sectors"]

        if sector_type and source:
            rows = table.rows_where(
                "sector_type = ? AND source = ?", (sector_type, source)
            )
        elif sector_type:
            rows = table.rows_where("sector_type = ?", (sector_type,))
        elif source:
            rows = table.rows_where("source = ?", (source,))
        else:
            rows = table.rows

        return [Sector(**row) for row in rows]

    def update_sector(self, sector: Sector) -> Sector:
        """更新板块

        Args:
            sector: 板块对象

        Returns:
            更新后的板块对象
        """
        sector.updated_at = datetime.datetime.now()
        self.db["sectors"].update(sector.id, sector.to_dict())
        return sector

    def delete_sector(self, sector_id: str) -> bool:
        """删除板块

        Args:
            sector_id: 板块ID

        Returns:
            是否删除成功
        """
        try:
            self.db["sectors"].delete(sector_id)
            return True
        except Exception as e:
            logger.error(f"删除板块失败: {e}")
            return False

    def add_stock_to_sector(
        self, sector_id: str, symbol: str, name: str = "", weight: float = 0.0
    ) -> bool:
        """添加股票到板块

        Args:
            sector_id: 板块ID
            symbol: 股票代码
            name: 股票名称
            weight: 权重

        Returns:
            是否添加成功
        """
        try:
            stock = SectorStock(
                sector_id=sector_id, symbol=symbol, name=name, weight=weight
            )
            self.db["sector_stocks"].insert(stock.to_dict(), pk=SectorStock.__pk__)
            return True
        except Exception as e:
            logger.error(f"添加股票到板块失败: {e}")
            return False

    def remove_stock_from_sector(self, sector_id: str, symbol: str) -> bool:
        """从板块移除股票

        Args:
            sector_id: 板块ID
            symbol: 股票代码

        Returns:
            是否移除成功
        """
        try:
            self.db["sector_stocks"].delete((sector_id, symbol))
            return True
        except Exception as e:
            logger.error(f"从板块移除股票失败: {e}")
            return False

    def get_sector_stocks(self, sector_id: str) -> list[SectorStock]:
        """获取板块成分股

        Args:
            sector_id: 板块ID

        Returns:
            成分股列表
        """
        rows = self.db["sector_stocks"].rows_where(
            "sector_id = ?", (sector_id,), order_by="symbol"
        )
        return [SectorStock(**row) for row in rows]

    def import_stocks_from_file(
        self, sector_id: str, file_path: str
    ) -> tuple[int, int, list[str]]:
        """从文件导入股票列表

        文件格式：
        - 每行一个股票代码
        - 支持格式：000001.SZ 或 000001
        - 可选：代码后加空格和名称，如 "000001.SZ 平安银行"

        Args:
            sector_id: 板块ID
            file_path: 文件路径

        Returns:
            (成功数, 失败数, 失败代码列表)
        """
        path = Path(file_path)
        if not path.exists():
            logger.error(f"文件不存在: {file_path}")
            return 0, 0, []

        success_count = 0
        failed_count = 0
        failed_symbols = []

        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    # 解析行：代码 [名称]
                    parts = line.split(maxsplit=1)
                    symbol = parts[0]
                    name = parts[1] if len(parts) > 1 else ""

                    # 标准化代码格式
                    if "." not in symbol:
                        # 根据代码规则添加后缀
                        if symbol.startswith("6"):
                            symbol = f"{symbol}.SH"
                        else:
                            symbol = f"{symbol}.SZ"

                    if self.add_stock_to_sector(sector_id, symbol, name):
                        success_count += 1
                    else:
                        failed_count += 1
                        failed_symbols.append(symbol)

        except Exception as e:
            logger.error(f"导入文件失败: {e}")

        return success_count, failed_count, failed_symbols

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
            [bar.to_dict() for bar in bars], pk=SectorBar.__pk__, replace=True
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
        rows = self.db["sector_bars"].rows_where(
            "sector_id = ? AND dt >= ? AND dt <= ?",
            (sector_id, start, end),
            order_by="dt",
        )

        if not rows:
            return pl.DataFrame()

        df = pl.DataFrame(rows)
        return df.with_columns(pl.col("dt").cast(pl.Date))
