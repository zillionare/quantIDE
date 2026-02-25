"""行情数据访问层"""

import datetime

import polars as pl
from loguru import logger

from pyqmt.data.dal.index_dal import IndexDAL
from pyqmt.data.dal.sector_dal import SectorDAL
from pyqmt.data.sqlite import SQLiteDB
from pyqmt.data.stores.bars import DailyBarsStore
from pyqmt.data.utils.resampler import Resampler


class BarDAL:
    """行情数据访问层

    整合个股、板块、指数行情数据的查询，支持多周期重采样。
    """

    def __init__(self, db: SQLiteDB, bars_store: DailyBarsStore | None = None):
        self.db = db
        self.bars_store = bars_store
        self.sector_dal = SectorDAL(db)
        self.index_dal = IndexDAL(db)

    def get_stock_bars(
        self,
        symbol: str,
        start: datetime.date,
        end: datetime.date,
        freq: str = "day",
    ) -> pl.DataFrame:
        """获取个股行情数据，支持多周期

        Args:
            symbol: 股票代码
            start: 开始日期
            end: 结束日期
            freq: 周期：day/week/month

        Returns:
            行情数据DataFrame
        """
        if self.bars_store is None:
            logger.error("DailyBarsStore 未初始化")
            return pl.DataFrame(schema={
                "dt": pl.Date,
                "symbol": pl.Utf8,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "amount": pl.Float64,
            })

        try:
            # 从 DailyBarsStore 获取日线数据
            # 使用 get 方法，assets 参数传入 [symbol]
            result = self.bars_store.get(assets=[symbol], start=start, end=end)

            # 确保是 DataFrame
            if isinstance(result, pl.LazyFrame):
                df = result.collect()
            else:
                df = result

            if df.is_empty():
                return pl.DataFrame(schema={
                    "dt": pl.Date,
                    "symbol": pl.Utf8,
                    "open": pl.Float64,
                    "high": pl.Float64,
                    "low": pl.Float64,
                    "close": pl.Float64,
                    "volume": pl.Int64,
                    "amount": pl.Float64,
                })

            # 重命名列以统一格式
            df = df.rename({
                "date": "dt",
                "asset": "symbol",
            })

            # 确保 dt 列是日期类型
            df = df.with_columns(pl.col("dt").cast(pl.Date))

            # 重采样
            if freq != "day":
                df = Resampler.resample(df, freq)

            return df

        except Exception as e:
            logger.error(f"获取个股行情失败: {e}")
            return pl.DataFrame(schema={
                "dt": pl.Date,
                "symbol": pl.Utf8,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "amount": pl.Float64,
            })

    def get_sector_bars(
        self,
        sector_id: str,
        start: datetime.date,
        end: datetime.date,
        freq: str = "day",
    ) -> pl.DataFrame:
        """获取板块行情数据，支持多周期

        Args:
            sector_id: 板块ID
            start: 开始日期
            end: 结束日期
            freq: 周期：day/week/month

        Returns:
            行情数据DataFrame
        """
        df = self.sector_dal.get_sector_bars(sector_id, start, end)

        if df.is_empty():
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

        # 重采样
        if freq != "day":
            df = Resampler.resample(df, freq)

        return df

    def get_index_bars(
        self,
        symbol: str,
        start: datetime.date,
        end: datetime.date,
        freq: str = "day",
    ) -> pl.DataFrame:
        """获取指数行情数据，支持多周期

        Args:
            symbol: 指数代码
            start: 开始日期
            end: 结束日期
            freq: 周期：day/week/month

        Returns:
            行情数据DataFrame
        """
        df = self.index_dal.get_index_bars(symbol, start, end)

        if df.is_empty():
            return pl.DataFrame(schema={
                "dt": pl.Date,
                "symbol": pl.Utf8,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "amount": pl.Float64,
            })

        # 重采样
        if freq != "day":
            df = Resampler.resample(df, freq)

        return df

    def get_bars_with_ma(
        self,
        symbol: str,
        start: datetime.date,
        end: datetime.date,
        freq: str = "day",
        ma_periods: list[int] | None = None,
    ) -> pl.DataFrame:
        """获取行情数据并计算均线

        Args:
            symbol: 代码（股票/板块/指数）
            start: 开始日期
            end: 结束日期
            freq: 周期
            ma_periods: 均线周期列表，如 [5, 10, 20, 60]

        Returns:
            包含均线的行情数据DataFrame
        """
        # 扩展日期范围以计算均线
        if ma_periods:
            max_ma = max(ma_periods)
            # 根据周期调整扩展天数
            if freq == "day":
                extend_days = max_ma * 2
            elif freq == "week":
                extend_days = max_ma * 7
            else:  # month
                extend_days = max_ma * 30

            query_start = start - datetime.timedelta(days=extend_days)
        else:
            query_start = start

        # 判断 symbol 类型并获取数据
        if symbol.startswith("sector_"):
            df = self.get_sector_bars(symbol, query_start, end, freq)
        elif ".SH" in symbol or ".SZ" in symbol:
            # 可能是指数或股票，先尝试指数
            df = self.get_index_bars(symbol, query_start, end, freq)
            if df.is_empty():
                df = self.get_stock_bars(symbol, query_start, end, freq)
        else:
            df = self.get_stock_bars(symbol, query_start, end, freq)

        if df.is_empty():
            return pl.DataFrame(schema={
                "dt": pl.Date,
                "symbol": pl.Utf8,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "amount": pl.Float64,
            })

        # 计算均线
        if ma_periods:
            df = Resampler.calculate_ma(df, ma_periods)

        # 过滤回原始日期范围
        df = df.filter(pl.col("dt") >= start)

        return df
