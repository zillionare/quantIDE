"""股票数据同步服务

支持历史数据下载和每日增量更新。
"""

import datetime
from collections.abc import Callable

import pandas as pd
import polars as pl
from loguru import logger

from pyqmt.config import cfg
from pyqmt.data.fetchers.tushare import fetch_stock_list
from pyqmt.data.fetchers.tushare_ext import get_last_trade_date
from pyqmt.data.models.calendar import Calendar
from pyqmt.data.models.stocks import StockList
from pyqmt.data.stores.bars import DailyBarsStore


class StockSyncService:
    """股票数据同步服务"""

    def __init__(self, stock_list: StockList, daily_store: DailyBarsStore, calendar: Calendar):
        """初始化股票同步服务

        Args:
            stock_list: 股票列表对象
            daily_store: 日线数据存储
            calendar: 交易日历
        """
        self.stock_list = stock_list
        self.daily_store = daily_store
        self.calendar = calendar
        self._epoch = getattr(cfg, "epoch", datetime.date(2005, 1, 1))

    def sync_stock_list(self) -> int:
        """同步股票列表

        Returns:
            同步的股票数量
        """
        logger.info("开始同步股票列表...")

        df = fetch_stock_list()
        if df is None or df.empty:
            logger.warning("未获取到股票列表数据")
            return 0

        # 保存到 parquet 文件
        self.stock_list.update()
        count = len(df)

        logger.info(f"股票列表同步完成，共 {count} 只")
        return count

    def sync_daily_bars(
        self,
        start: datetime.date | None = None,
        end: datetime.date | None = None,
        progress_callback: Callable | None = None,
    ) -> int:
        """同步日线行情数据

        Args:
            start: 开始日期，默认为 cfg.epoch
            end: 结束日期，默认为最近交易日
            progress_callback: 进度回调函数，接收 (current_date, completed_count, total_count) 参数

        Returns:
            同步的日期数量
        """
        if start is None:
            start = self._epoch
        if end is None:
            end = get_last_trade_date()

        logger.info(f"开始同步日线行情: {start} ~ {end}")

        # 获取交易日历中的所有交易日
        trade_dates = self.calendar.get_trade_dates(start, end)
        if not trade_dates:
            logger.warning(f"日期范围 {start} ~ {end} 内没有交易日")
            return 0

        # 使用逐日同步方法，提供详细进度
        try:
            count = self.daily_store.fetch_with_daily_progress(
                start, end, progress_callback=progress_callback
            )
            logger.info(f"日线行情同步完成，共 {count} 个交易日")
            return count
        except Exception as e:
            logger.error(f"日线行情同步失败: {e}")
            return 0

    def sync_daily(self) -> dict:
        """每日增量同步

        同步内容：
        1. 股票列表（全量更新）
        2. 日线行情（增量更新）

        Returns:
            同步结果统计
        """
        logger.info("开始每日股票数据同步...")

        # 1. 同步股票列表
        stock_count = self.sync_stock_list()

        # 2. 同步日线行情（只同步最近7天的数据，避免重复下载）
        end = get_last_trade_date()
        start = end - datetime.timedelta(days=7)
        bar_count = self.sync_daily_bars(start, end)

        return {
            "stocks": stock_count,
            "bars": bar_count,
        }

    def sync_full_history(
        self,
        start: datetime.date | None = None,
    ) -> dict:
        """全量历史数据同步

        首次启动时调用，下载从 start 到当前的所有历史数据。

        Args:
            start: 起始日期，默认为 cfg.epoch

        Returns:
            同步结果统计
        """
        if start is None:
            start = self._epoch

        logger.info(f"开始全量历史数据同步，起始日期: {start}")

        # 1. 同步股票列表
        stock_count = self.sync_stock_list()

        # 2. 同步全部历史行情
        bar_count = self.sync_daily_bars(start)

        return {
            "stocks": stock_count,
            "bars": bar_count,
        }
