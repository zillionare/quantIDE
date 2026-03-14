"""K线数据重采样工具

提供日线转周线、月线的功能。
"""

import datetime

import polars as pl


class Resampler:
    """K线数据重采样器"""

    @staticmethod
    def daily_to_weekly(df: pl.DataFrame) -> pl.DataFrame:
        """日线转周线

        按照自然周进行聚合，周一到周五为一周。
        周线数据的开高低收成交量成交额计算规则：
        - open: 第一周第一个交易日的开盘价
        - high: 周内最高价的最高价
        - low: 周内最低价的最低价
        - close: 最后一周最后一个交易日的收盘价
        - volume: 周内成交量总和
        - amount: 周内成交额总和

        Args:
            df: 日线数据DataFrame，必须包含 dt, open, high, low, close, volume, amount 列

        Returns:
            周线数据DataFrame
        """
        if df.is_empty():
            return pl.DataFrame()

        # 确保 dt 列是日期类型
        df = df.with_columns(pl.col("dt").cast(pl.Date))

        # 构建聚合表达式
        agg_exprs = [
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
            pl.col("amount").sum().alias("amount"),
        ]

        # 个股有 adjust 字段，指数没有
        if "adjust" in df.columns:
            agg_exprs.append(pl.col("adjust").last().alias("adjust"))

        # 按周聚合
        # 使用 group_by_dynamic 按周分组
        return df.sort("dt").group_by_dynamic(
            index_column="dt",
            every="1w",
            period="1w",
            label="left",
            start_by="monday",
        ).agg(agg_exprs)

    @staticmethod
    def daily_to_monthly(df: pl.DataFrame) -> pl.DataFrame:
        """日线转月线

        按照自然月进行聚合。
        月线数据的开高低收成交量成交额计算规则与周线相同。

        Args:
            df: 日线数据DataFrame，必须包含 dt, open, high, low, close, volume, amount 列

        Returns:
            月线数据DataFrame
        """
        if df.is_empty():
            return pl.DataFrame()

        # 确保 dt 列是日期类型
        df = df.with_columns(pl.col("dt").cast(pl.Date))

        # 构建聚合表达式
        agg_exprs = [
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
            pl.col("amount").sum().alias("amount"),
        ]

        # 个股有 adjust 字段，指数没有
        if "adjust" in df.columns:
            agg_exprs.append(pl.col("adjust").last().alias("adjust"))

        # 按月聚合
        return df.sort("dt").group_by_dynamic(
            index_column="dt",
            every="1mo",
            period="1mo",
            label="left",
        ).agg(agg_exprs)

    @staticmethod
    def resample(df: pl.DataFrame, freq: str) -> pl.DataFrame:
        """通用重采样方法

        Args:
            df: 日线数据DataFrame
            freq: 目标周期，可选值：'day', 'week', 'month'

        Returns:
            重采样后的DataFrame

        Raises:
            ValueError: 不支持的周期类型
        """
        if freq == "day":
            return df
        elif freq == "week":
            return Resampler.daily_to_weekly(df)
        elif freq == "month":
            return Resampler.daily_to_monthly(df)
        else:
            raise ValueError(f"不支持的周期类型: {freq}，可选值: day, week, month")

    @staticmethod
    def calculate_ma(df: pl.DataFrame, periods: list[int]) -> pl.DataFrame:
        """计算移动平均线

        Args:
            df: 包含 close 列的DataFrame
            periods: 均线周期列表，如 [5, 10, 20, 60]

        Returns:
            添加了均线列的DataFrame
        """
        if df.is_empty():
            return df

        for period in periods:
            df = df.with_columns(
                pl.col("close").rolling_mean(window_size=period).alias(f"ma{period}")
            )

        return df
