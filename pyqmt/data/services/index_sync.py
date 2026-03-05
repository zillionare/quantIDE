"""指数数据同步服务

支持历史数据下载和每日增量更新。
"""

import datetime

import pandas as pd
from loguru import logger

from pyqmt.config import cfg
from pyqmt.data.dal.index_dal import IndexDAL
from pyqmt.data.fetchers.tushare_ext import (
    fetch_index_bars,
    fetch_index_list,
    get_last_trade_date,
)
from pyqmt.data.models.index import Index, IndexBar


class IndexSyncService:
    """指数数据同步服务"""

    def __init__(self, dal: IndexDAL):
        self.dal = dal
        self._epoch = getattr(cfg, "epoch", datetime.date(2005, 1, 1))

    def sync_index_list(self) -> int:
        """同步指数列表（从tushare）

        Returns:
            同步的指数数量
        """
        logger.info("开始同步指数列表...")

        df = fetch_index_list()
        if df is None or df.empty:
            logger.warning("未获取到指数数据")
            return 0

        indices = []
        for _, row in df.iterrows():
            index = Index(
                symbol=row["symbol"],
                name=row["name"],
                index_type=row["index_type"],
                category=row.get("category", ""),
                publisher=row.get("publisher", ""),
            )
            indices.append(index)

        count = self.dal.upsert_indices(indices)
        logger.info(f"指数列表同步完成，共 {count} 个")
        return count

    def sync_index_bars(
        self,
        symbol: str,
        start: datetime.date | None = None,
        end: datetime.date | None = None,
    ) -> int:
        """同步指数行情数据

        Args:
            symbol: 指数代码
            start: 开始日期，默认为 cfg.epoch
            end: 结束日期，默认为最近交易日

        Returns:
            同步的行情记录数
        """
        if start is None:
            start = self._epoch
        if end is None:
            end = get_last_trade_date()

        logger.info(f"开始同步指数 {symbol} 行情: {start} ~ {end}")

        df = fetch_index_bars(symbol, start, end)
        if df is None or df.empty:
            logger.warning(f"未获取到指数 {symbol} 的行情数据")
            return 0

        # 转换为 IndexBar 对象
        bars = []
        for _, row in df.iterrows():
            bar = IndexBar(
                symbol=symbol,
                dt=row["dt"],
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=int(row["volume"]),
                amount=float(row["amount"]),
            )
            bars.append(bar)

        count = self.dal.save_index_bars(bars)
        logger.info(f"指数 {symbol} 行情同步完成，共 {count} 条")
        return count

    def sync_all_index_bars(
        self,
        start: datetime.date | None = None,
        end: datetime.date | None = None,
    ) -> dict[str, int]:
        """同步所有指数的行情数据

        Args:
            start: 开始日期，默认为 cfg.epoch
            end: 结束日期，默认为最近交易日

        Returns:
            {symbol: count}
        """
        if start is None:
            start = self._epoch
        if end is None:
            end = get_last_trade_date()

        indices = self.dal.list_indices()
        results = {}

        for index in indices:
            try:
                count = self.sync_index_bars(index.symbol, start, end)
                results[index.symbol] = count
            except Exception as e:
                logger.error(f"同步指数 {index.symbol} 行情失败: {e}")
                results[index.symbol] = 0

        return results

    def sync_daily(self) -> dict:
        """每日增量同步

        同步内容：
        1. 指数列表（全量更新）
        2. 指数行情（增量更新）

        Returns:
            同步结果统计
        """
        logger.info("开始每日指数数据同步...")

        # 1. 同步指数列表
        index_count = self.sync_index_list()

        # 2. 同步行情（只同步最近7天的数据，避免重复下载）
        end = get_last_trade_date()
        start = end - datetime.timedelta(days=7)
        bar_counts = self.sync_all_index_bars(start, end)

        return {
            "indices": index_count,
            "bars": bar_counts,
        }

    def sync_full_history(self) -> dict:
        """全量历史数据同步

        首次启动时调用，下载从 cfg.epoch 到当前的所有历史数据。

        Returns:
            同步结果统计
        """
        logger.info(f"开始全量历史数据同步，起始日期: {self._epoch}")

        # 1. 同步指数列表
        index_count = self.sync_index_list()

        # 2. 同步全部历史行情
        bar_counts = self.sync_all_index_bars()

        return {
            "indices": index_count,
            "bars": bar_counts,
        }

    def sync_main_indices(self) -> dict[str, int]:
        """同步主要市场指数

        同步常用的市场指数：上证指数、深证成指、创业板指等

        Returns:
            {symbol: count}
        """
        main_indices = [
            "000001.SH",  # 上证指数
            "399001.SZ",  # 深证成指
            "399006.SZ",  # 创业板指
            "000016.SH",  # 上证50
            "000300.SH",  # 沪深300
            "000905.SH",  # 中证500
            "000852.SH",  # 中证1000
        ]

        results = {}
        for symbol in main_indices:
            try:
                count = self.sync_index_bars(symbol)
                results[symbol] = count
            except Exception as e:
                logger.error(f"同步指数 {symbol} 失败: {e}")
                results[symbol] = 0

        return results
