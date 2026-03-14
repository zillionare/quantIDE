"""板块数据同步服务

基于 xtdata (QMT) 实现板块列表、成分股和行情的同步。
支持历史数据下载和每日增量更新。
"""

import datetime
from collections.abc import Callable
from pathlib import Path
from typing import Any

import polars as pl
from loguru import logger

from pyqmt.config import cfg
from pyqmt.data.dal.sector_dal import SectorDAL
from pyqmt.data.fetchers.xtdata_sectors import (
    fetch_sector_bars,
    fetch_sector_constituents,
    fetch_sector_list,
    get_index_list,
    get_tradeable_sectors,
)
from pyqmt.data.models.calendar import Calendar
from pyqmt.data.models.sector import Sector, SectorBar, SectorConstituent
from pyqmt.data.stores.sector_bars import SectorBarsStore


class SectorSyncService:
    """板块数据同步服务"""

    def __init__(
        self,
        dal: SectorDAL,
        calendar: Calendar,
        bars_store_path: str | Path | None = None,
    ):
        """初始化板块同步服务

        Args:
            dal: 板块数据访问层
            calendar: 日历对象
            bars_store_path: 板块行情存储路径，默认使用配置中的路径
        """
        self.dal = dal
        self.calendar = calendar

        if bars_store_path is None:
            bars_store_path = Path(cfg.home) / "data" / "sector_bars"

        self.bars_store = SectorBarsStore(bars_store_path, calendar)
        self._epoch = getattr(cfg, "epoch", datetime.date(2005, 1, 1))

    def sync_sector_list(
        self,
        trade_date: datetime.date | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
    ) -> int:
        """同步板块列表

        Args:
            trade_date: 数据日期，默认为今天
            progress_callback: 进度回调函数 (current, total, message)

        Returns:
            同步的板块数量
        """
        if trade_date is None:
            trade_date = datetime.date.today()

        logger.info(f"开始同步板块列表，日期: {trade_date}")

        if progress_callback:
            progress_callback(0, 100, "正在获取板块列表...")

        df = fetch_sector_list(trade_date)

        if len(df) == 0:
            logger.warning("未获取到板块数据")
            return 0

        if progress_callback:
            progress_callback(50, 100, f"获取到 {len(df)} 个板块，正在保存...")

        # 转换为 Sector 对象并批量保存
        sectors = [
            Sector(
                id=row["id"],
                name=row["name"],
                sector_type=row["sector_type"],
                source=row["source"],
                trade_date=row["trade_date"],
            )
            for row in df.to_dicts()
        ]

        count = self.dal.create_sectors_batch(sectors)

        if progress_callback:
            progress_callback(100, 100, f"板块列表同步完成，共 {count} 个")

        logger.info(f"板块列表同步完成，共 {count} 个板块")
        return count

    def sync_sector_constituents(
        self,
        trade_date: datetime.date | None = None,
        sector_types: list[str] | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
    ) -> int:
        """同步板块成分股

        Args:
            trade_date: 数据日期，默认为今天
            sector_types: 要同步的板块类型列表，None 表示所有类型
            progress_callback: 进度回调函数 (current, total, message)

        Returns:
            同步的总成分股数量
        """
        if trade_date is None:
            trade_date = datetime.date.today()

        logger.info(f"开始同步板块成分股，日期: {trade_date}")

        # 获取板块列表
        sectors_df = self.dal.get_sectors_by_date(trade_date)

        if len(sectors_df) == 0:
            logger.warning("未找到板块数据，请先同步板块列表")
            return 0

        # 过滤板块类型
        if sector_types is not None:
            sectors_df = sectors_df.filter(pl.col("sector_type").is_in(sector_types))

        sector_ids = sectors_df["id"].to_list()
        total = len(sector_ids)

        logger.info(f"需要同步 {total} 个板块的成分股")

        total_constituents = 0

        for i, sector_id in enumerate(sector_ids):
            if progress_callback:
                progress_callback(i, total, f"正在同步 {sector_id} 的成分股...")

            df = fetch_sector_constituents(sector_id, trade_date)

            if len(df) == 0:
                continue

            # 转换为 SectorConstituent 对象
            constituents = [
                SectorConstituent(
                    sector_id=row["sector_id"],
                    trade_date=row["trade_date"],
                    symbol=row["symbol"],
                    name=row["name"],
                    weight=row["weight"],
                )
                for row in df.to_dicts()
            ]

            count = self.dal.add_constituents_batch(constituents)
            total_constituents += count

            logger.debug(f"板块 {sector_id} 同步了 {count} 个成分股")

        if progress_callback:
            progress_callback(total, total, f"成分股同步完成，共 {total_constituents} 个")

        logger.info(f"板块成分股同步完成，共 {total_constituents} 个")
        return total_constituents

    def sync_sector_bars(
        self,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
        sector_ids: list[str] | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
    ) -> int:
        """同步板块行情数据

        Args:
            start_date: 开始日期，默认为两年前
            end_date: 结束日期，默认为今天
            sector_ids: 要同步的板块ID列表，None 表示所有可交易板块
            progress_callback: 进度回调函数 (current, total, message)

        Returns:
            同步的总记录数
        """
        if end_date is None:
            end_date = datetime.date.today()

        if start_date is None:
            start_date = end_date - datetime.timedelta(days=730)  # 两年前

        logger.info(f"开始同步板块行情: {start_date} ~ {end_date}")

        # 获取要同步的板块列表
        if sector_ids is None:
            sector_ids = get_tradeable_sectors()

        total = len(sector_ids)
        logger.info(f"需要同步 {total} 个板块的历史行情")

        total_records = 0

        for i, sector_id in enumerate(sector_ids):
            if progress_callback:
                progress_callback(i, total, f"正在同步 {sector_id} 的行情...")

            try:
                df = fetch_sector_bars(sector_id, start_date, end_date)

                if len(df) == 0:
                    continue

                # 转换为 SectorBar 对象
                bars = [
                    SectorBar(
                        sector_id=row["sector_id"],
                        dt=row["dt"],
                        open=row["open"],
                        high=row["high"],
                        low=row["low"],
                        close=row["close"],
                        volume=row["volume"],
                        amount=row["amount"],
                    )
                    for row in df.to_dicts()
                ]

                count = self.dal.save_sector_bars(bars)
                total_records += count

                logger.debug(f"板块 {sector_id} 同步了 {count} 条行情数据")

            except Exception as e:
                logger.warning(f"同步板块 {sector_id} 行情失败: {e}")
                continue

        if progress_callback:
            progress_callback(total, total, f"行情同步完成，共 {total_records} 条")

        logger.info(f"板块行情同步完成，共 {total_records} 条记录")
        return total_records

    def sync_daily_constituents(
        self,
        trade_date: datetime.date | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
    ) -> int:
        """同步每日板块成分股（增量更新）

        每天调用一次，同步当天的板块成分股。

        Args:
            trade_date: 数据日期，默认为今天
            progress_callback: 进度回调函数

        Returns:
            同步的成分股数量
        """
        if trade_date is None:
            trade_date = datetime.date.today()

        # 检查是否已同步
        existing = self.dal.get_sectors_by_date(trade_date)
        if len(existing) == 0:
            logger.info(f"日期 {trade_date} 的板块数据不存在，先同步板块列表")
            self.sync_sector_list(trade_date, progress_callback)

        return self.sync_sector_constituents(trade_date, progress_callback=progress_callback)

    def sync_daily_bars(
        self,
        trade_date: datetime.date | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
    ) -> int:
        """同步每日板块行情（增量更新）

        每天调用一次，同步当天的板块行情。

        Args:
            trade_date: 数据日期，默认为今天
            progress_callback: 进度回调函数

        Returns:
            同步的记录数
        """
        if trade_date is None:
            trade_date = datetime.date.today()

        return self.sync_sector_bars(
            start_date=trade_date,
            end_date=trade_date,
            progress_callback=progress_callback,
        )

    def get_sync_status(self, trade_date: datetime.date | None = None) -> dict[str, Any]:
        """获取同步状态

        Args:
            trade_date: 数据日期，默认为今天

        Returns:
            同步状态字典
        """
        if trade_date is None:
            trade_date = datetime.date.today()

        sectors_df = self.dal.get_sectors_by_date(trade_date)
        sector_count = len(sectors_df)

        # 统计成分股数量
        constituents_count = 0
        if sector_count > 0:
            try:
                cursor = self.dal.db.execute(
                    "SELECT COUNT(*) FROM sector_constituents WHERE trade_date = ?",
                    (trade_date,),
                )
                constituents_count = cursor.fetchone()[0]
            except Exception:
                pass

        # 统计行情数据
        bars_df = self.dal.get_sector_bars_by_date(trade_date)
        bars_count = len(bars_df)

        return {
            "trade_date": trade_date,
            "sector_count": sector_count,
            "constituents_count": constituents_count,
            "bars_count": bars_count,
            "is_synced": sector_count > 0,
        }

    def full_sync(
        self,
        start_date: datetime.date | None = None,
        end_date: datetime.date | None = None,
        progress_callback: Callable[[int, int, str], None] | None = None,
    ) -> dict[str, int]:
        """全量同步

        同步板块列表、成分股和历史行情。

        Args:
            start_date: 开始日期，默认为两年前
            end_date: 结束日期，默认为今天
            progress_callback: 进度回调函数

        Returns:
            同步结果统计
        """
        if end_date is None:
            end_date = datetime.date.today()

        if start_date is None:
            start_date = end_date - datetime.timedelta(days=730)

        results = {}

        # 1. 同步板块列表
        if progress_callback:
            progress_callback(0, 100, "正在同步板块列表...")
        results["sectors"] = self.sync_sector_list(end_date, progress_callback)

        # 2. 同步板块成分股
        if progress_callback:
            progress_callback(33, 100, "正在同步板块成分股...")
        results["constituents"] = self.sync_sector_constituents(
            end_date, progress_callback=progress_callback
        )

        # 3. 同步板块行情
        if progress_callback:
            progress_callback(66, 100, "正在同步板块行情...")
        results["bars"] = self.sync_sector_bars(
            start_date, end_date, progress_callback=progress_callback
        )

        if progress_callback:
            progress_callback(100, 100, "同步完成")

        logger.info(f"全量同步完成: {results}")
        return results
