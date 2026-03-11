"""行情数据统一接口

提供策略统一的数据获取接口，无需区分回测/实盘。
自动合并历史数据和实时数据。
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Protocol

import polars as pl
from loguru import logger

from pyqmt.core.enums import FrameType

if TYPE_CHECKING:
    from pyqmt.data.models.daily_bars import DailyBars
    from pyqmt.data.models.live_quote import LiveQuote


Frame = datetime.date | datetime.datetime


class BarsFeed(Protocol):
    """行情数据统一接口

    策略通过此接口获取行情数据，无需关心是回测还是实盘。
    实现类负责自动处理历史数据+实时数据的合并。

    字段统一约定：
    - asset: 资产代码
    - frame: 时间帧（日期或日期时间）
    - open, high, low, close: 价格
    - volume, amount: 成交量和成交额
    - adjust: 复权因子
    - up_limit, down_limit: 涨跌停价
    """

    def get_bars(
        self,
        asset: str,
        start: Frame,
        end: Frame | None = None,
        frame_type: FrameType = FrameType.DAY,
        adjust: str | None = "qfq",
    ) -> pl.DataFrame:
        """获取行情数据

        自动处理回测/实盘场景：
        - 回测：从数据库获取历史数据
        - 实盘：合并数据库历史数据 + LiveQuote 实时数据

        暂不实现分钟级接口。需要实时/分钟级数据，
        可以直接从 LiveQuote 获取当天数据。

        Args:
            asset: 资产代码，如 "000001.SZ"
            start: 开始时间
            end: 结束时间，None 表示获取到最新
            frame_type: 时间周期类型，默认日线
            adjust: 复权类型，"qfq"(前复权)/"hfq"(后复权)/None(不复权)

        Returns:
            Polars DataFrame，列：
            asset, frame, open, high, low, close, volume, amount, adjust, up_limit, down_limit
        """
        ...

    def get_price_limits(self, asset: str) -> tuple[float, float]:
        """获取指定资产的实时涨跌停价格

        Args:
            asset: 资产代码

        Returns:
            (down_limit, up_limit) 元组
        """
        ...

    def get_current_price(self, asset: str) -> float | None:
        """获取指定资产的最新价格（仅实盘）

        Args:
            asset: 资产代码

        Returns:
            最新价格，如果无数据返回 None
        """
        ...

    def get_price_for_match(
        self, asset: str, tm: datetime.datetime
    ) -> pl.DataFrame | None:
        """获取用于撮合的行情数据（回测/模拟盘使用）

        对于日线，返回当日行情数据。

        返回的 DataFrame 包含 open, high, low, close, volume, up_limit, down_limit。
        如果缺少字段，返回 None。

        Args:
            asset: 资产代码
            tm: 开始时间（通常为报单时间）

        Returns:
            pl.DataFrame 或 None（无数据时）
        """
        ...

    def get_close_adjust_factor(
        self, assets: list[str], start: Frame, end: Frame
    ) -> pl.DataFrame:
        """获取指定日期范围内的收盘价和复权因子

        用于计算市值和除权。

        Args:
            assets: 资产列表
            start: 开始时间
            end: 结束时间

        Returns:
            pl.DataFrame: 包含字段 [frame, asset, close, adjust]
        """
        ...


class BarsFeedImpl:
    """BarsFeed 的实现类

    整合 DailyBars（历史数据）和 LiveQuote（实时数据），
    为策略提供统一的数据接口。
    """

    def __init__(
        self,
        daily_bars: "DailyBars" | None = None,
        live_quote: "LiveQuote" | None = None,
    ):
        """初始化 BarsFeedImpl

        Args:
            daily_bars: DailyBars 实例，用于获取历史数据
            live_quote: LiveQuote 实例，用于获取实时数据
        """
        self._daily_bars = daily_bars
        self._live_quote = live_quote
        self._is_backtest = live_quote is None or not live_quote.is_running

    def get_bars(
        self,
        asset: str,
        start: Frame,
        end: Frame | None = None,
        frame_type: FrameType = FrameType.DAY,
        adjust: str | None = "qfq",
    ) -> pl.DataFrame:
        """获取行情数据（历史+实时合并）

        实现逻辑：
        1. 从 DailyBars 获取历史数据
        2. 如果是实盘且包含当天，从 LiveQuote 获取实时数据并合并
        3. 统一字段命名和格式
        """
        try:
            if frame_type != FrameType.DAY:
                logger.warning(f"Frame type {frame_type} not supported yet, using DAY")

            history_df = self._get_history_bars(asset, start, end, adjust)

            if not self._is_backtest and self._live_quote is not None:
                today = datetime.date.today()
                need_live = end is None or (
                    isinstance(end, datetime.date) and end >= today
                ) or (
                    isinstance(end, datetime.datetime) and end.date() >= today
                )

                if need_live:
                    live_df = self._get_live_bar(asset)
                    if live_df is not None and len(live_df) > 0:
                        history_df = self._merge_history_live(history_df, live_df)

            return self._normalize_columns(history_df)

        except Exception as e:
            logger.error(f"Error getting bars for {asset}: {e}")
            return self._empty_df()

    def _get_history_bars(
        self,
        asset: str,
        start: Frame,
        end: Frame | None,
        adjust: str | None,
    ) -> pl.DataFrame:
        """从历史数据获取 bars"""
        if self._daily_bars is None:
            return self._empty_df()

        try:
            start_dt = self._to_datetime(start)
            end_dt = self._to_datetime(end) if end else None

            df = self._daily_bars.get_bars_in_range(
                assets=[asset],
                start=start_dt,
                end=end_dt,
                adjust=adjust,
                eager_mode=True,
            )

            if not df.is_empty():
                df = df.rename({
                    "date": "frame",
                    "asset": "asset",
                })

            return df

        except Exception as e:
            logger.error(f"Error getting history bars: {e}")
            return self._empty_df()

    def _get_live_bar(self, asset: str) -> pl.DataFrame | None:
        """从 LiveQuote 获取当天实时数据"""
        if self._live_quote is None:
            return None

        try:
            bar = self._live_quote.get_daily_bar(asset)
            if bar is None:
                return None

            return pl.DataFrame([bar])

        except Exception as e:
            logger.error(f"Error getting live bar: {e}")
            return None

    def _merge_history_live(
        self, history_df: pl.DataFrame, live_df: pl.DataFrame
    ) -> pl.DataFrame:
        """合并历史数据和实时数据"""
        if history_df.is_empty():
            return live_df

        if live_df.is_empty():
            return history_df

        common_cols = [col for col in history_df.columns if col in live_df.columns]
        live_df = live_df.select(common_cols)

        return history_df.vstack(live_df).sort("frame")

    def _normalize_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """统一字段格式"""
        if df.is_empty():
            return self._empty_df()

        required_cols = [
            "asset", "frame", "open", "high", "low", "close",
            "volume", "amount", "adjust", "up_limit", "down_limit"
        ]

        for col in required_cols:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))

        return df.select(required_cols)

    def _empty_df(self) -> pl.DataFrame:
        """返回空 DataFrame（统一格式）"""
        return pl.DataFrame(
            schema={
                "asset": pl.Utf8,
                "frame": pl.Datetime,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Int64,
                "amount": pl.Float64,
                "adjust": pl.Float64,
                "up_limit": pl.Float64,
                "down_limit": pl.Float64,
            }
        )

    def _to_datetime(self, frame: Frame) -> datetime.datetime:
        """将 Frame 转换为 datetime"""
        if isinstance(frame, datetime.datetime):
            return frame
        return datetime.datetime.combine(frame, datetime.time.min)

    def get_price_limits(self, asset: str) -> tuple[float, float]:
        """获取实时涨跌停价格"""
        if self._live_quote is not None:
            return self._live_quote.get_price_limits(asset)

        if self._daily_bars is not None:
            try:
                return self._daily_bars.get_trade_price_limits(
                    asset, datetime.date.today()
                )
            except Exception as e:
                logger.error(f"Error getting price limits: {e}")

        return 0.0, 0.0

    def get_current_price(self, asset: str) -> float | None:
        """获取最新价格（仅实盘）"""
        if self._live_quote is None:
            return None

        quote = self._live_quote.get_quote(asset)
        if quote is None:
            return None

        return quote.get("price")

    def get_price_for_match(
        self, asset: str, tm: datetime.datetime
    ) -> pl.DataFrame | None:
        """获取用于撮合的行情数据"""
        df = self.get_bars(
            asset=asset,
            start=tm.date(),
            end=tm.date(),
            frame_type=FrameType.DAY,
            adjust=None,
        )

        if df.is_empty():
            return None

        required = ["open", "high", "low", "close", "volume", "up_limit", "down_limit"]
        if not all(col in df.columns for col in required):
            return None

        return df

    def get_close_adjust_factor(
        self, assets: list[str], start: Frame, end: Frame
    ) -> pl.DataFrame:
        """获取收盘价和复权因子"""
        if self._daily_bars is None:
            return pl.DataFrame(
                schema={
                    "frame": pl.Datetime,
                    "asset": pl.Utf8,
                    "close": pl.Float64,
                    "adjust": pl.Float64,
                }
            )

        try:
            start_dt = self._to_datetime(start)
            end_dt = self._to_datetime(end)

            return self._daily_bars.get_close_adjust_factor(
                assets=assets, start=start_dt, end=end_dt
            )

        except Exception as e:
            logger.error(f"Error getting close adjust factor: {e}")
            return pl.DataFrame(
                schema={
                    "frame": pl.Datetime,
                    "asset": pl.Utf8,
                    "close": pl.Float64,
                    "adjust": pl.Float64,
                }
            )
