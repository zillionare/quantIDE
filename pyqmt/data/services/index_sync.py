"""指数数据同步服务

基于 xtdata (QMT) 实现指数列表和行情的同步。
支持历史数据下载和每日增量更新。
"""

import datetime
from collections.abc import Callable

import pandas as pd
import polars as pl
from loguru import logger

from pyqmt.config import cfg
from pyqmt.core.legacy_qmt import ensure_legacy_local_qmt_enabled
from pyqmt.data.dal.index_dal import IndexDAL
from pyqmt.data.fetchers.xtdata_sectors import fetch_sector_bars, get_index_list
from pyqmt.data.models.calendar import calendar
from pyqmt.data.models.index import Index, IndexBar


class IndexSyncService:
    """指数数据同步服务"""

    def __init__(self, dal: IndexDAL):
        ensure_legacy_local_qmt_enabled(
            "指数 xtdata 同步服务",
            "qmt-gateway 或非 xtquant 数据源",
        )
        self.dal = dal
        self._epoch = getattr(cfg, "epoch", datetime.date(2005, 1, 1))

    def sync_index_list(self) -> int:
        """同步指数列表（从QMT）

        Returns:
            同步的指数数量
        """
        logger.info("开始同步指数列表...")

        index_codes = get_index_list()
        if not index_codes:
            logger.warning("未获取到指数数据")
            return 0

        indices = []
        for code in index_codes:
            # 从代码推断名称和类型
            name = code  # 简化处理，实际可以通过 get_instrument_detail 获取
            index_type = "stock"
            if code.startswith("0"):
                index_type = "sh"
            elif code.startswith("3") or code.startswith("2"):
                index_type = "sz"
            elif code.startswith("8") or code.startswith("9"):
                index_type = "csi"

            index = Index(
                symbol=code,
                name=name,
                index_type=index_type,
                category="",
                publisher="",
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
        progress_callback: Callable | None = None,
    ) -> int:
        """同步指数行情数据

        Args:
            symbol: 指数代码
            start: 开始日期，默认为 cfg.epoch
            end: 结束日期，默认为最近交易日
            progress_callback: 进度回调函数，接收 (symbol, current_date, completed_count, total_count) 参数

        Returns:
            同步的行情记录数
        """
        if start is None:
            start = self._epoch
        if end is None:
            end = calendar.last_trade_date()

        logger.info(f"开始同步指数 {symbol} 行情: {start} ~ {end}")

        # 获取交易日历
        trade_dates = calendar.get_trade_dates(start, end)
        if not trade_dates:
            logger.warning(f"日期范围 {start} ~ {end} 内没有交易日")
            return 0

        total = len(trade_dates)
        completed = 0
        all_bars = []

        # 使用 xtdata 获取数据
        try:
            df = fetch_sector_bars(symbol, start, end)
            if len(df) > 0:
                for row in df.to_dicts():
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
                    all_bars.append(bar)

                    completed += 1
                    current_date = row["dt"]

                    # 报告进度
                    msg = f"正在同步指数 {symbol} {current_date.strftime('%Y%m%d')}，已更新 {completed}/{total} 日"
                    logger.info(msg)

                    if progress_callback:
                        progress_callback(symbol, current_date, completed, total)

        except Exception as e:
            logger.error(f"同步指数 {symbol} 数据失败: {e}")

        # 批量保存
        if all_bars:
            count = self.dal.save_index_bars(all_bars)
            logger.info(f"指数 {symbol} 行情同步完成，共 {count} 条")
            return count
        return 0

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
            end = calendar.last_trade_date()

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
        end = calendar.last_trade_date()
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
