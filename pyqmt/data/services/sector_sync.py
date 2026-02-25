"""板块数据同步服务

支持历史数据下载和每日增量更新。
"""

import datetime

import pandas as pd
from loguru import logger

from pyqmt.config import cfg
from pyqmt.data.dal.sector_dal import SectorDAL
from pyqmt.data.fetchers.tushare_ext import (
    fetch_concept_list,
    fetch_sector_bars,
    fetch_sector_list,
    fetch_sector_stocks,
    get_last_trade_date,
)
from pyqmt.data.models.sector import Sector, SectorBar, SectorStock


class SectorSyncService:
    """板块数据同步服务"""

    def __init__(self, dal: SectorDAL):
        self.dal = dal
        self._epoch = getattr(cfg, "epoch", datetime.date(2005, 1, 1))

    def sync_industry_sectors(self) -> int:
        """同步行业板块列表（从tushare）

        Returns:
            同步的板块数量
        """
        logger.info("开始同步行业板块列表...")

        df = fetch_sector_list()
        if df is None or df.empty:
            logger.warning("未获取到行业板块数据")
            return 0

        count = 0
        for _, row in df.iterrows():
            sector = Sector(
                id=row["id"],
                name=row["name"],
                sector_type="industry",
                source="tushare",
            )

            # 检查是否已存在
            existing = self.dal.get_sector(sector.id)
            if existing:
                # 更新
                existing.name = sector.name
                self.dal.update_sector(existing)
            else:
                # 创建
                self.dal.create_sector(sector)
                count += 1

        logger.info(f"行业板块列表同步完成，新增 {count} 个，更新 {len(df) - count} 个")
        return len(df)

    def sync_concept_sectors(self) -> int:
        """同步概念板块列表（从tushare）

        Returns:
            同步的板块数量
        """
        logger.info("开始同步概念板块列表...")

        df = fetch_concept_list()
        if df is None or df.empty:
            logger.warning("未获取到概念板块数据")
            return 0

        count = 0
        for _, row in df.iterrows():
            sector = Sector(
                id=row["id"],
                name=row["name"],
                sector_type="concept",
                source="tushare",
            )

            existing = self.dal.get_sector(sector.id)
            if existing:
                existing.name = sector.name
                self.dal.update_sector(existing)
            else:
                self.dal.create_sector(sector)
                count += 1

        logger.info(f"概念板块列表同步完成，新增 {count} 个，更新 {len(df) - count} 个")
        return len(df)

    def sync_all_sectors(self) -> dict[str, int]:
        """同步所有板块列表

        Returns:
            {"industry": count, "concept": count}
        """
        return {
            "industry": self.sync_industry_sectors(),
            "concept": self.sync_concept_sectors(),
        }

    def sync_sector_stocks(self, sector_id: str) -> int:
        """同步板块成分股

        Args:
            sector_id: 板块ID

        Returns:
            同步的成分股数量
        """
        logger.info(f"开始同步板块 {sector_id} 的成分股...")

        sector = self.dal.get_sector(sector_id)
        if not sector:
            logger.error(f"板块 {sector_id} 不存在")
            return 0

        df = fetch_sector_stocks(sector_id)
        if df is None or df.empty:
            logger.warning(f"未获取到板块 {sector_id} 的成分股")
            return 0

        # 清除旧的成分股
        old_stocks = self.dal.get_sector_stocks(sector_id)
        for stock in old_stocks:
            self.dal.remove_stock_from_sector(sector_id, stock.symbol)

        # 添加新的成分股
        count = 0
        for _, row in df.iterrows():
            if self.dal.add_stock_to_sector(
                sector_id=sector_id,
                symbol=row["symbol"],
                name=row.get("name", ""),
            ):
                count += 1

        logger.info(f"板块 {sector_id} 成分股同步完成，共 {count} 只")
        return count

    def sync_all_sector_stocks(self) -> dict[str, int]:
        """同步所有板块的成分股

        Returns:
            {sector_id: count}
        """
        sectors = self.dal.list_sectors()
        results = {}

        for sector in sectors:
            if sector.source == "tushare":
                count = self.sync_sector_stocks(sector.id)
                results[sector.id] = count

        return results

    def sync_sector_bars(
        self,
        sector_id: str,
        start: datetime.date | None = None,
        end: datetime.date | None = None,
    ) -> int:
        """同步板块行情数据

        Args:
            sector_id: 板块ID
            start: 开始日期，默认为 cfg.epoch
            end: 结束日期，默认为最近交易日

        Returns:
            同步的行情记录数
        """
        if start is None:
            start = self._epoch
        if end is None:
            end = get_last_trade_date()

        logger.info(f"开始同步板块 {sector_id} 行情: {start} ~ {end}")

        df = fetch_sector_bars(sector_id, start, end)
        if df is None or df.empty:
            logger.warning(f"未获取到板块 {sector_id} 的行情数据")
            return 0

        # 转换为 SectorBar 对象
        bars = []
        for _, row in df.iterrows():
            bar = SectorBar(
                sector_id=sector_id,
                dt=row["dt"],
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(row["volume"]),
                amount=float(row["amount"]),
            )
            bars.append(bar)

        count = self.dal.save_sector_bars(bars)
        logger.info(f"板块 {sector_id} 行情同步完成，共 {count} 条")
        return count

    def sync_all_sector_bars(
        self,
        start: datetime.date | None = None,
        end: datetime.date | None = None,
    ) -> dict[str, int]:
        """同步所有板块的行情数据

        Args:
            start: 开始日期，默认为 cfg.epoch
            end: 结束日期，默认为最近交易日

        Returns:
            {sector_id: count}
        """
        if start is None:
            start = self._epoch
        if end is None:
            end = get_last_trade_date()

        sectors = self.dal.list_sectors()
        results = {}

        for sector in sectors:
            if sector.source == "tushare":
                try:
                    count = self.sync_sector_bars(sector.id, start, end)
                    results[sector.id] = count
                except Exception as e:
                    logger.error(f"同步板块 {sector.id} 行情失败: {e}")
                    results[sector.id] = 0

        return results

    def sync_daily(self) -> dict:
        """每日增量同步

        同步内容：
        1. 板块列表（全量更新）
        2. 板块成分股（全量更新）
        3. 板块行情（增量更新）

        Returns:
            同步结果统计
        """
        logger.info("开始每日板块数据同步...")

        # 1. 同步板块列表
        sector_counts = self.sync_all_sectors()

        # 2. 同步成分股
        stock_counts = self.sync_all_sector_stocks()

        # 3. 同步行情（只同步最近7天的数据，避免重复下载）
        end = get_last_trade_date()
        start = end - datetime.timedelta(days=7)
        bar_counts = self.sync_all_sector_bars(start, end)

        return {
            "sectors": sector_counts,
            "stocks": stock_counts,
            "bars": bar_counts,
        }

    def sync_full_history(self) -> dict:
        """全量历史数据同步

        首次启动时调用，下载从 cfg.epoch 到当前的所有历史数据。

        Returns:
            同步结果统计
        """
        logger.info(f"开始全量历史数据同步，起始日期: {self._epoch}")

        # 1. 同步板块列表
        sector_counts = self.sync_all_sectors()

        # 2. 同步成分股
        stock_counts = self.sync_all_sector_stocks()

        # 3. 同步全部历史行情
        bar_counts = self.sync_all_sector_bars()

        return {
            "sectors": sector_counts,
            "stocks": stock_counts,
            "bars": bar_counts,
        }
